# CR HotKey Chaining Design

## 1. Problem Statement

### Current limitation

Cross-shard transactions (SSC) lock state keys during simulation and hold those locks until the Commit/Rollback (CR) transaction finalizes. The typical lifecycle spans ~4 blocks:

```
Block N:   SubmitSimulationTx (lock acquired)
Block N+1: Simulation committed
Block N+2: ... CR vote collection ...
Block N+3: CR committed (lock released)
```

During these ~4 blocks, no other transaction can modify the same state key. For **hot keys** — state keys that every transaction touches — this serializes all transactions through a single bottleneck:

```
Hot Key K:  tx1  →  tx2  →  tx3  →  tx4
            [locked for 4 blocks]
                     [locked for 4 blocks]
                              [locked for 4 blocks]
                                       [locked for 4 blocks]
```

With RATE=100 and each key locked for 8 seconds, the system is fundamentally throughput-limited by lock contention on hot keys.

### Root cause

The gap between **CR unlock** (keys become available) and **next Simulation commit** (keys re-locked) wastes 2-3 blocks because:

1. CR tx commits in a block → unlocks hot key K
2. Next block: leader's `OnBlockCommitted` fires → `CanLock` check on retry pool → finds K is free
3. `ReSimulationSignal` sent to origin shard
4. Origin shard receives signal → calls `startReSimulation` → P2P to committee members
5. Members simulate → leader aggregates → submits new `SubmitSimulationTx`
6. **Next block**: new SimulationTx included in block → key K re-locked

This is 2-3 blocks of wasted lock-free time for hot keys.

## 2. Proposed Solution: CR→Sim Chaining

### Core idea

When a CR transaction (from `crSigner` at nonce `crN`) commits and releases a **hot key**, immediately pre-simulate the next waiting transaction that needs that key. Submit the resulting SimulationTx **also via `crSigner` at nonce `crN+1`**, guaranteeing:

- **Ordering**: CR(crN) always executes before Sim(crN+1) in the blockchain
- **Lock continuity**: key is locked (by CR) → unlocked (by CR commit) → re-locked (by Sim in next block) with no wasted slot

```
Block N:
  ├─ CR(crSigner, nonce=crN)    → unlocks hot key K → K's new value = v2
  └─ (mempool)
       │
       ▼ (goroutine triggered immediately after CR submit)
  retryScheduler:
    1. Detect: "key K was released by this CR"
    2. Look up retry pool: "tx_A is waiting for key K"
    3. Build CRHotWritePatch: { K → v2 } from CR's WriteState
    4. Send ReSimulationSignal(tx_A, hotPatch) to origin shard

Block N+1:
  ├─ SimulationTx(crSigner, nonce=crN+1)  ← pre-simulated with patch value
  │   → VerifySimulation: reads patched value of K, locks K again
  └─ ... other transactions ...
```

### Hot key detection

**Dynamic runtime detection** using existing `SimulationStats.HotKeyConflicts`:

| Mechanism | Detail |
|-----------|--------|
| Data source | `SimulationStats.HotKeyConflicts` (sync.Map: LockKey → conflict count) |
| Trigger | `stateLocker.Lockable()` calls `RecordLockConflict(key)` on every conflict |
| Classification | On each `OnBlockCommitted`, scan top-N keys by conflict count |
| Hot threshold | `conflict_count >= HotKeyThreshold` (configurable, default 50) |
| Reset | Conflict counts reset per epoch/experiment to avoid stale hot keys |

The `retryScheduler` maintains a `hotKeySet map[api.LockKey]struct{}` that is updated on each `OnBlockCommitted`:

```go
func (rs *retryScheduler) updateHotKeys() {
    topKeys := rs.sscService.stats.topHotKeys(hotKeyMaxCount)
    rs.hotKeySet = make(map[api.LockKey]struct{})
    for _, keyStr := range topKeys {
        rs.hotKeySet[api.LockKey(keyStr)] = struct{}{}
    }
}
```

Only hot keys trigger the CR→Sim chaining path. Non-hot keys follow the existing retry path (no optimization overhead).

## 3. Data Structure Changes

### 3.1 `CXTSimulationState`

Add a field for the CR write patch:

```go
type CXTSimulationState struct {
    // ... existing fields ...

    // CRHotWritePatch stores state values from the CR transaction's WriteSet
    // that unlocked hot keys. When set, GetState() checks this patch FIRST
    // before querying the actual stateDB. This allows a retrying simulation
    // to read the CR's post-unlock values without waiting for the next block.
    CRHotWritePatch *RWSet `json:"cr_hot_write_patch,omitempty"`
}
```

### 3.2 `retryScheduler`

Add hot key tracking and chaining methods:

```go
type retryScheduler struct {
    // ... existing fields ...

    hotKeySet    map[api.LockKey]struct{}  // current hot keys
    hotKeyMu     sync.RWMutex              // protects hotKeySet
}
```

New methods:

```go
func (rs *retryScheduler) updateHotKeys()
func (rs *retryScheduler) isHotKey(key api.LockKey) bool
func (rs *retryScheduler) chainHotKeyCR(txHash common.Hash, writeSet *RWSet)
```

### 3.3 `ReSimulationSignal`

Augment to carry the hot patch:

```go
type ReSimulationSignal struct {
    TxHash        common.Hash
    FromShard     uint32        // shard that produced this signal (CR commit shard for hot key path)
    Epoch         Epoch
    SimulationNum int
    Condition     ConflictCondition
    Ready         bool
    // CRHotWritePatch carries the WriteSet from the unlocking CR tx.
    // Only set when the unlocked key is a hot key.
    CRHotWritePatch *RWSet `json:"cr_hot_write_patch,omitempty"`
}
```

## 4. Flow: End-to-End

```
1. Leader submits CR(crSigner, nonce=crN)
   │
2. CR tx enters mempool
   │
3. Leader goroutine:
   a. Extract CR's WriteSet (key→newValue pairs)
   b. Filter to only hot keys: for key in WriteSet:
        if !rs.isHotKey(key): skip
   c. If any hot keys were modified:
        Build RWSet{WriteState: {key: newValue}} as patch
   d. For each retry tx waiting for these hot keys:
        - Check tempLockView.CanLock(tx.ReadSet, tx.WriteSet)
        - If ready: send ReSimulationSignal{CRHotWritePatch: patch}
                    to origin shard leader
   │
4. Origin shard leader receives signal:
   a. If CRHotWritePatch != nil:
        Store patch in CXTSimulationState.CRHotWritePatch
   b. Call rs.setSignal(signal.FromShard, signal)  → signal aggregation
   c. Check readyCnt == len(retryTx.RelatedShards)?
        ├─ Yes → go rs.tryToReSimulation(retryTx)
        │           → RPC RetryCommit to ALL related shards
        │           → All locked → startReSimulation
        └─ No  → Wait for remaining signals (normal OnBlockCommitted path)
   d. Simulation succeeds → leader aggregates → SubmitSimulationTx
        submitted using crSigner with nonce = crN+1
   │
5. Block N+1:
   ├─ CR(crSigner, nonce=crN)      ← actual CR (may already be in N)
   └─ SimTx(crSigner, nonce=crN+1) ← chained simulation
      → VerifySimulation succeeds with patched state values
      → Key is now locked by new tx
```

## 5. Code Changes

### File list

| File | Change | Priority |
|------|--------|:--------:|
| `ssc/api/types.go` | Add `CRHotWritePatch *RWSet` to `CXTSimulationState` and `ReSimulationSignal` | P0 |
| `ssc/api/types.go` | Add `TimeToLock` to `CXTSimulationState` (optional, for metric) | P2 |
| `ssc/retry_scheduler.go` | Add `hotKeySet`, `updateHotKeys()`, `isHotKey()`, `chainHotKeyCR()` | P0 |
| `ssc/retry_scheduler.go` | Modify `OnBlockCommitted` to call `updateHotKeys()` | P0 |
| `ssc/retry_scheduler.go` | Modify `RetryCancel` to clear `CRHotWritePatch` | P0 |
| `ssc/retry_scheduler.go` | Add `HandleHotKeySignal()` for origin shard to process CR-patched signals | P0 |
| `ssc/state_impl.go` | Modify `GetState` to check `state.CRHotWritePatch` first | P0 |
| `ssc/impl.go` | After `thresholdSignSimulationCommit` (CR path): call `rs.chainHotKeyCR()` | P0 |
| `ssc/statistics.go` | Use existing `HotKeyConflicts` (no change needed) | - |

### Detailed changes

#### 5.1 `GetState` patch check (ssc/state_impl.go)

```go
func (s *sscService) GetState(db api.StateDB, txHash common.Hash, 
    address common.Address, key common.Hash) (common.Hash, error) {
    
    // Step 0: Check CR hot write patch (new)
    s.stateLock.RLock()
    state, err := s.getState(txHash)
    s.stateLock.RUnlock()
    if err == nil && state.CRHotWritePatch != nil {
        if addrState, ok := state.CRHotWritePatch.WriteState.State[address]; ok {
            if val, exists := addrState[key]; exists {
                // Patch hit: return CR's written value without querying stateDB
                // Also cache it in the callState's RWSet for subsequent reads
                if callState := s.GetCallState(txHash); callState != nil {
                    callState.StateLock.Lock()
                    callState.RWSet.ReadState.State[address][key] = val
                    callState.RWSet.CurrentState.State[address][key] = val
                    callState.StateLock.Unlock()
                }
                return val, nil
            }
        }
    }
    
    // Step 1: Existing cache logic
    callState := s.GetCallState(txHash)
    // ... rest of existing GetState ...
}
```

#### 5.2 `chainHotKeyCR` (ssc/retry_scheduler.go, new method)

```go
// chainHotKeyCR is called after a CR transaction is submitted.
// It checks if the CR's WriteSet contains hot keys, and if so,
// finds matching retry transactions and signals them for immediate
// pre-simulation with the CR's updated state values.
func (rs *retryScheduler) chainHotKeyCR(crTxHash common.Hash, 
    writeSet *RWSet, epochs []api.Epoch) {
    
    // 1. Filter to hot keys only
    hotPatch := &api.RWSet{
        WriteState: newStateSet(),
    }
    hasHotKey := false
    for addr, state := range writeSet.WriteState.State {
        for key, val := range state {
            lockKey := api.FormKey(addr, key)
            if rs.isHotKey(lockKey) {
                if hotPatch.WriteState.State[addr] == nil {
                    hotPatch.WriteState.State[addr] = make(map[common.Hash]common.Hash)
                }
                hotPatch.WriteState.State[addr][key] = val
                hasHotKey = true
            }
        }
    }
    if !hasHotKey {
        return  // no hot keys affected, skip optimization
    }
    
    // 2. Find matching retry transactions
    rs.mu.RLock()
    defer rs.mu.RUnlock()
    
    for txHash, retryTx := range rs.retryPool {
        if bytes.Equal(retryTx.TxHash.Bytes(), crTxHash.Bytes()) {
            continue  // skip the CR tx itself
        }
        
        // Check if this retry tx needs any hot key from the patch
        needsHotKey := false
        for _, key := range retryTx.ReadSet {
            if _, exists := hotPatch.WriteState.State[
                common.HexToAddress(string(key))]; exists {
                needsHotKey = true
                break
            }
        }
        if !needsHotKey {
            for _, key := range retryTx.WriteSet {
                if rs.isHotKey(key) {
                    needsHotKey = true
                    break
                }
            }
        }
        if !needsHotKey {
            continue
        }
        
        // 3. Check if this tx can actually lock (via tempLockView)
        // Only proceed if tempLockView says the keys are free
        if !rs.tempLockView.CanLock(txHash, retryTx.ReadSet, retryTx.WriteSet) {
            continue  // still blocked, keep waiting
        }
        
        // 4. Send signal with hot patch to origin shard
        signal := &api.ReSimulationSignal{
            TxHash:          retryTx.TxHash,
            FromShard:       rs.selfShard,     // CR commit shard ID
            Epoch:           retryTx.Epochs[retryTx.OriginShardID],
            SimulationNum:   retryTx.SimulationNum,
            Condition:       retryTx.Condition,
            Ready:           true,
            CRHotWritePatch: hotPatch,
        }
        
        // Send to origin shard leader
        originLeader := rs.sscService.GetLeader(
            retryTx.Epochs[retryTx.OriginShardID], retryTx.OriginShardID)
        err := rs.comm.Call(rs.ctx, nil, originLeader, 
            api.Method_HandleHotKeyRetrySignal, signal)
        if err != nil {
            utils.SSCLogger().Error().Err(err).Str("txHash", txHash.Hex()).
                Msg("chainHotKeyCR: failed to send signal")
        } else {
            utils.SSCLogger().Info().Str("txHash", txHash.Hex()).
                Interface("hotKeys", hotPatch.WriteState.State).
                Msg("chainHotKeyCR: sent hot key retry signal")
        }
    }
}
```

#### 5.3 `HandleHotKeyRetrySignal` (origin shard, corrected to use signal aggregation)

```go
func (s *sscService) HandleHotKeyRetrySignal(signal *api.ReSimulationSignal) {
    s.stateLock.Lock()
    state, err := s.getState(signal.TxHash)
    if err != nil {
        s.stateLock.Unlock()
        utils.SSCLogger().Error().Err(err).Str("txHash", signal.TxHash.Hex()).
            Msg("HandleHotKeyRetrySignal: state not found")
        return
    }

    // Store the CR hot write patch for this tx's simulation to read
    if signal.CRHotWritePatch != nil {
        state.CRHotWritePatch = signal.CRHotWritePatch
    }
    s.stateLock.Unlock()

    // Feed into signal aggregation — treat this as one shard's signal
    rs := s.retryScheduler
    rs.mu.Lock()
    retryTx := rs.retryPool[signal.TxHash]
    if retryTx == nil {
        // Transaction already cleaned up (completed/stale), skip
        rs.mu.Unlock()
        return
    }

    rs.setSignal(signal.FromShard, signal)

    // Check if ALL related shards now signal ready
    readyCnt := 0
    cachedSignals := rs.signals[signal.TxHash][signal.SimulationNum]
    for _, s := range cachedSignals {
        if s.Ready {
            readyCnt++
        }
    }
    if readyCnt == len(retryTx.RelatedShards) {
        // All shards ready → cross-shard RetryCommit via tryToReSimulation
        go rs.tryToReSimulation(retryTx)
    }
    rs.mu.Unlock()

    utils.SSCLogger().Info().Str("txHash", signal.TxHash.Hex()).
        Int("patchAddrCount", len(signal.CRHotWritePatch.WriteState.State)).
        Int("readyCnt", readyCnt).
        Msg("HandleHotKeyRetrySignal: stored patch and integrated with signal aggregation")
}
```

#### 5.4 `RetryCancel` cleanup

```go
func (rs *retryScheduler) RetryCancel(txHash common.Hash) {
    rs.tempLockView.GarbageCollect(txHash)
    
    // Clean up CRHotWritePatch if present
    s.stateLock.Lock()
    if state, err := s.getState(txHash); err == nil {
        state.CRHotWritePatch = nil
        s.stateLock.Unlock()
    } else {
        s.stateLock.Unlock()
    }
    
    rs.mu.Lock()
    rs.staleTxs[txHash] = struct{}{}
    rs.mu.Unlock()
}
```

#### 5.5 Trigger point in `StartSimulateCXTransaction`

After the `thresholdSignSimulationCommit(simulationCommit)` call for CR transactions, add:

```go
// After thresholdSignSimulationCommit for CR path:
// (detect CR tx vs simulation tx by Commit flag and type)
if simulationCommit.Commit && s.IsLeader(req.Epochs[s.SelfShard]) {
    s.retryScheduler.chainHotKeyCR(txHash, crWriteSet, req.Epochs)
}
```

Note: The `crWriteSet` needs to be captured from the CR transaction's state. This requires the CR path to make its RWSet available. One approach is to store it in a temporary field on the simulation commit.

## 6. Hot Key Configuration

Add to `TimeoutConfig` in `ssc/api/types.go`:

```go
type HotKeyConfig struct {
    Enabled         bool   `json:"enabled" yaml:"enabled"`   // master switch
    Threshold       int    `json:"threshold" yaml:"threshold"` // min conflicts to be "hot"
    MaxCount        int    `json:"max_count" yaml:"max_count"` // max hot keys tracked
    ChainEnabled    bool   `json:"chain_enabled" yaml:"chain_enabled"` // CR→Sim chaining on/off
    ChainMaxPerBlock int   `json:"chain_max_per_block" yaml:"chain_max_per_block"` // max chained sims per block
}
```

Or keep it simple with a single config value in `ShardSimulateCommitteeConfig`:

```go
type ShardSimulateCommitteeConfig struct {
    Committees []*ShardSimulateCommittee
    Timeout    *TimeoutConfig
    HotKey     *HotKeyConfig    // NEW
    Reputation *ReputationConfig
}
```

## 7. Logging

Key log events for debugging:

| Event | Log level | Location | Data |
|-------|-----------|----------|------|
| Hot key detected | INFO | `chainHotKeyCR` | LockKey, conflict count |
| CR→Sim signal sent | INFO | `chainHotKeyCR` | txHash, hotPatch keys |
| Origin shard received signal | INFO | `HandleHotKeyRetrySignal` | txHash, patchSize |
| GetState patch hit | DEBUG | `GetState` | txHash, addr, key |
| GetState patch miss | DEBUG | `GetState` | txHash (patch exists but key not in it) |
| RetryCancel cleanup | DEBUG | `RetryCancel` | txHash |
| CR→Sim: no hot keys | DEBUG | `chainHotKeyCR` | txHash (skip) |
| CR→Sim: tx still blocked | DEBUG | `chainHotKeyCR` | txHash (CanLock false) |

## 8. Open Questions & Future Optimizations

### Q1: Chain multiple retries per CR
If the same CR unlocks multiple hot keys, and multiple retry txs need different keys, should we chain all of them? Risk: nonce consumption on crSigner.

### Q2: crSigner nonce exhaustion
If every hot key unlocks triggers a chained SimulationTx, the crSigner's nonce advances quickly. With 10 hot key unlocks/block × 2s block time = 300 nonces/minute. Is this sustainable? (Yes — uint64 nonce has margin.)

### Q3: Full vs delta RWSet
Currently the proposal sends the full CR WriteSet as the patch. For efficiency, consider sending only the delta (keys that are both in CR's WriteSet AND in the retry tx's ReadSet). But this requires per-tx analysis → more CPU cost. Start with full patch, optimize later.

### Q4: Non-hot key degredation
If too many keys are classified as "hot", the optimization becomes general-purpose and may increase contention on crSigner nonce. Keep `MaxCount` low (e.g., 10 keys per shard).

### Q5: Epoch boundary
Hot key sets should reset at epoch boundaries to adapt to changing usage patterns.

## 9. Success Criteria

| Metric | Before | Target |
|--------|--------|--------|
| Hot key commit rate | ~0% (all timeouts/rollbacks) | >60% |
| Overall commit rate (RATE=100) | ~45% | >55% |
| Rollback count | 0 with TempLockView | 0 (maintain) |
| TempLock pre-check fail rate | ~32% | <20% |
| crSigner nonce usage increase | N/A | <10% overhead |

## 10. Implementation Order

1. Add `CRHotWritePatch` to `CXTSimulationState` and `ReSimulationSignal` (types.go)
2. Add hot key tracking to `retryScheduler` (hotKeySet, updateHotKeys, isHotKey)
3. Modify `GetState` to check patch first (state_impl.go)
4. Implement `chainHotKeyCR` (retry_scheduler.go)
5. Add `HandleHotKeyRetrySignal` handler (impl.go)
6. Wire up trigger point after CR submission
7. Update `RetryCancel` for patch cleanup
8. Add comprehensive logging
9. Experiment: RATE=100, delay=20, compare before/after

## 11. Post-Implementation Fix: Cross-Shard Coordination in `HandleHotKeyRetrySignal`

### 11.1 Problem discovered post-implementation

The initial `HandleHotKeyRetrySignal` implementation (section 5.3 above was the buggy version) skipped cross-shard coordination:

```go
// BUGGY: locked only the origin shard, started simulation immediately
s.retryScheduler.RetryCommit(signal.TxHash)  // only locks origin
go s.startReSimulation(signal.TxHash, signal.SimulationNum)  // too early
```

This assumed the origin shard's readiness was sufficient. For cross-shard transactions, **all related shards** must agree before retrying.

### 11.2 Design decisions (via grill-me session 2026-06-06)

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | Add `FromShard uint32` to `ReSimulationSignal` | `setSignal()` needs the source shard ID; `chainHotKeyCR` fills it with `rs.selfShard` |
| D2 | `HandleHotKeyRetrySignal` feeds into `setSignal` + `readyCnt` check, not `RetryCommit` directly | Reuses existing `HandleReSimulationSignal` aggregation logic — no duplicate coordination |
| D3 | Get `rs.mu.Lock()` before `setSignal` + ready check | Prevents concurrent map write race with `HandleReSimulationSignal` |
| D4 | Guard: check `rs.retryPool[txHash] != nil` after acquiring `rs.mu` | Transaction may have been cleaned up between `stateLock` release and `rs.mu` acquisition |
| D5 | Don't clean CRHotWritePatch on nil retryTx | Patch in state is harmless — state will be cleaned when the tx state is garbage collected |
| D6 | Race between hot key signal and normal signal is acceptable | `tryToReSimulation` is the true arbiter (RPC RetryCommit to ALL shards). `readyCnt` check is just an optimization trigger |

### 11.3 Corrected flow

```
chainHotKeyCR (CR commit shard)
  └─ ReSimulationSignal{FromShard: rs.selfShard, Ready: true,
        CRHotWritePatch: hotPatch}
      └─ RPC → HandleHotKeyRetrySignal (origin shard)
          │
          ├─ stateLock → store CRHotWritePatch → stateLock.Unlock()
          │
          └─ rs.mu.Lock()
              ├─ retryPool[txHash] == nil? → return (cleaned up)
              ├─ setSignal(signal.FromShard, signal)
              ├─ readyCnt == len(RelatedShards)?
              │   ├─ Yes → go tryToReSimulation(retryTx)
              │   │           → RPC RetryCommit to ALL related shards
              │   │           → All locked → startReSimulation
              │   └─ No  → return (wait for normal signal path)
              └─ rs.mu.Unlock()
```

### 11.4 Files changed

| File | Change |
|------|--------|
| `ssc/api/types.go` | `ReSimulationSignal` — add `FromShard uint32` |
| `ssc/retry_scheduler.go` | `chainHotKeyCR` — fill `FromShard: rs.selfShard` in signal construction |
| `ssc/impl.go` | Rewrite `HandleHotKeyRetrySignal` — use `setSignal` + `readyCnt` → `tryToReSimulation` |

### 11.5 Verification

- After fix: hot key signal triggers cross-shard `tryToReSimulation` exactly like normal signals
- Observation points: "HandleHotKeyRetrySignal: stored patch and integrated with signal aggregation" log + "retry commit success/failed" from `tryToReSimulation`
- Risk: none introduced — hot key path now follows the same well-tested signal aggregation path as normal retries
