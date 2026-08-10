# DSN-XX: TxSubmitter 排序 Buffer + Priority Heap

## 问题

当前 `txSubmitter` 收到 SimTx/CRTx/Upload/NewEpoch 等任务后立刻 `submitWithRetry`，nonce 实时分配。这导致：

1. **交易过早提交到 TxPool** → gas limit 打满后大量交易积压，leader 每块只能处理极少量交易
2. **无排序** → 交易按到达顺序进 TxPool，nonce 顺序不可控
3. **无 Flight 控制** → 提交速度和链上消费速度脱节

**目标**：TxPool 中仅保持 1-2 个块的交易存量，其余交易在 submitter 中排队排序。

## 方案

在 `TxPriorityQueue`（5 个优先级 chan）和 `processTask` 之间插入 **5 个独立 MinHeap + Flight 限速**。

**仅改文件：`ssc/tx_submitter.go`**

## 设计细节

### 1. 架构

```
5 个 chan (现有 TxPriorityQueue)
    ↓ drainChannelsToHeap (non-blocking)
5 个独立 MinHeap
    ├── heap[0] — NewEpochTx (优先级 5)
    ├── heap[1] — UploadOpinionsTx (优先级 4)
    ├── heap[2] — CommitOrRollbackTx (优先级 3)
    ├── heap[3] — SimulationTx (优先级 2)
    └── heap[4] — EmptyTx (优先级 1)
        ↓ flight 检查 (inFlight < flightLimit?)
processTask (同步)
```

### 2. 排序 Key（所有类型统一）

```
Less(a, b):
  1. nonce 小优先
  2. shardId 小优先
```

SimTx 的 nonce 从 `CXTSimulation.Nonce` 取（入堆前已有）。CRTx/Upload/NewEpoch/Empty 的 nonce 在 `processTask` 中 `getNonce(txType)` 分配，入堆时 nonce 字段为 0。

### 3. Flight 控制

**目标**：TxPool 中仅保持 1-2 个块的交易存量。

```go
type txSubmitter struct {
    // ... 现有字段 ...

    heaps             [5]*taskHeap    // 5 个独立 MinHeap（按类型索引）
    flightLimit       int32           // 最大在途交易数（= lastBlockTxCnt * 2）
    inFlight          atomic.Int32    // 已提交到 TxPool 但未被块确认的交易数
    lastBlockTxCnt    int32           // 上个块实际打包的总交易数
    submittedTxHashes map[common.Hash]uint64 // txHash → nonce（用于 OnBlockCommitted 减计数）
}
```

- `flightLimit` 初始 = 1，第一块出完后更新为 `lastBlockTxCnt * 2`（取 2 个块的缓冲）
- 所有类型交易都受 flight 控制，不分 SimTx/CRTx
- `inFlight` = 成功 `AddPendingTransaction` 的交易数 - `OnBlockCommitted` 匹配数

### 4. processQueue 新逻辑

```go
func (t *txSubmitter) processQueue() {
    for {
        // Step 1: non-blocking drain 所有 chan 到对应 heap
        t.drainChannelsToHeap()

        // Step 2: 按优先级从高到低，检查每个 heap
        submitted := false
        for i := 0; i < 5; i++ {
            if t.heaps[i].Len() > 0 && t.inFlight.Load() < atomic.LoadInt32(&t.flightLimit) {
                task := heap.Pop(t.heaps[i]).(*txTask)
                t.inFlight.Add(1)
                t.submittedTxHashes[task.txHash] = task.nonce
                t.processTask(task)  // 同步
                submitted = true
                break  // 一次只处理一笔，下一轮继续
            }
        }

        // Step 3: 收新任务
        if submitted {
            continue
        }
        allEmpty := true
        for i := 0; i < 5; i++ {
            if t.heaps[i].Len() > 0 {
                allEmpty = false
                break
            }
        }
        if allEmpty {
            t.blockingDrainOne()  // 阻塞等待任意 chan
        } else {
            time.Sleep(50 * time.Millisecond)  // flight 满，等块确认释放
        }
    }
}
```

**为什么一次只处理一笔？** 保证优先级公平——高优先级类型不会因为一轮连续处理低优先级而被饿死。每一轮按优先级检查，找到了就提交并进入下一轮。

### 5. drainChannelsToHeap

```go
func (t *txSubmitter) drainChannelsToHeap() {
    for i := 0; i < 5; i++ {
        for {
            select {
            case task := <-t.queue.queues[i]:
                // 入堆时保留 nonce 快照（SimTx 的 Nonce 已在 simulation 中）
                // 非 SimTx 类型的 nonce 在 processTask 中分配
                if t.heaps[i].Len() < 10000 {
                    heap.Push(t.heaps[i], task)
                } else {
                    utils.SSCLogger().Warn().
                        Int("heapIdx", i).
                        Str("txType", string(task.txType)).
                        Msg("[TxSubmitter] heap full, dropping task")
                    task.done <- errors.New("heap full")
                }
            default:
                break
            }
        }
    }
}
```

### 6. OnBlockCommitted

```go
func (t *txSubmitter) OnBlockCommitted(block *types.Block) {
    matched := 0
    for _, tx := range block.Transactions() {
        txHash := tx.Hash()
        if _, ok := t.submittedTxHashes[txHash]; ok {
            delete(t.submittedTxHashes, txHash)
            matched++
        }
    }
    t.inFlight.Add(-int32(matched))

    // 更新 flightLimit = lastBlockTxCnt * 2，最小 1
    t.lastBlockTxCnt = int32(matched)
    atomic.StoreInt32(&t.flightLimit, max(1, int32(float64(matched)*2)))

    utils.SSCLogger().Info().
        Uint64("blockNum", block.NumberU64()).
        Int("matched", matched).
        Int32("inFlight", t.inFlight.Load()).
        Int32("flightLimit", t.flightLimit).
        Int("submittedTxHashes", len(t.submittedTxHashes)).
        Msg("[TxSubmitter] OnBlockCommitted")
}
```

### 7. 调用者不感知

`SubmitSimulationTx` / `SubmitCommitOrRollbackTx` / `SubmitNewEpoch` / `SubmitUploadOpinions` / `SubmitEmptyTx` 的签名和 `<-task.done` 等待模式**不变**。入堆后调用方同步等待，processTask 完成后发 done。

### 8. Heap 上限

每个 heap 独立上限 **10000**。超限时返回 error 给调用方。

### 9. submittedTxHashes 清理

当前不加主动 GC，仅打印日志观察泄漏情况。理论上不会泄漏——被 TxPool 丢弃的交易会产生 nonce gap，后续交易无法推进，早暴露了。

## 验证

1. `go build ./ssc/` 编译通过
2. 单节点启动，模拟 RATE=200 测试
3. 观察日志：`[TxSubmitter] OnBlockCommitted` 中 inFlight 稳定在 flightLimit 附近
4. 观察 TxPool 存量是否控制在 1-2 块的交易数
5. 对比 gas 打满前的交易吞吐
