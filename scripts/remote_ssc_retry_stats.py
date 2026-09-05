#!/usr/bin/env python3
"""
Remote-side SSC retry / DAG / timeout aggregator.

Reads all ssc-validator-*.log in a log dir and prints a compact summary of:
  * CHAIN_RETRY_STATS (accumulated across all 5s dumps)  -> DAG usage
  * STATS DUMP counters (tempLock, retry, sp1/pool fired)
  * timeout / rollback reason counts
  * on-chain retry limit exceeded -> simulationNum vs lockedSimulationNum
  * DAG rescue markers

Usage (on remote 199, inside a log dir):
  python3 remote_ssc_retry_stats.py <logdir>
"""
import sys, os, json, glob
from collections import Counter, defaultdict

def parse_line(line):
    i = line.find('{')
    if i < 0:
        return None
    try:
        return json.loads(line[i:])
    except Exception:
        return None

def main(logdir):
    files = sorted(glob.glob(os.path.join(logdir, 'ssc-validator-*.log')))
    if not files:
        print(f"no ssc-validator-*.log in {logdir}")
        sys.exit(1)
    print(f"# files: {len(files)}")

    chain = Counter()        # CHAIN_RETRY_STATS fields (ints)
    chain_dist = defaultdict(Counter)  # distribution dicts
    stats = Counter()        # STATS DUMP fields
    msg = Counter()          # message-substring counters
    dag_pb = {'max': 0, 'blocks_gt1': 0}  # per-block same-key writes (leader-only lines)
    chain_fate = Counter()   # [chainTxFate] no-commit close reason (leader-only, DAG-rescued txs)

    # retry-limit pairs: (simulationNum, lockedSimulationNum)
    retry_lim = Counter()
    retry_lim_detail = []    # keep a sample

    for f in files:
        with open(f, 'r', errors='replace') as fh:
            for line in fh:
                if '[dagPerBlock]' in line:
                    d = parse_line(line)
                    if d is not None:
                        v = d.get('maxSameKeyWrites')
                        if isinstance(v, int):
                            dag_pb['max'] = max(dag_pb['max'], v)
                            if v > 1:
                                dag_pb['blocks_gt1'] += 1
                if '[chainTxFate]' in line:
                    d = parse_line(line)
                    if d is not None:
                        chain_fate[d.get('reason', '?')] += 1
                if 'CHAIN_RETRY_STATS' in line:
                    d = parse_line(line)
                    if d is None:
                        continue
                    for k, v in d.items():
                        if k in ('level', 'port', 'ip', 'time', 'caller', 'message'):
                            continue
                        if isinstance(v, dict):
                            for k2, v2 in v.items():
                                try:
                                    chain_dist[k][k2] += int(v2)
                                except (TypeError, ValueError):
                                    pass
                        else:
                            try:
                                chain[k] += int(v)
                            except (TypeError, ValueError):
                                pass
                elif 'STATS DUMP' in line:
                    d = parse_line(line)
                    if d is None:
                        continue
                    for k, v in d.items():
                        if k in ('level', 'port', 'ip', 'time', 'caller', 'message'):
                            continue
                        if isinstance(v, (int, float)):
                            try:
                                stats[k] += int(v)
                            except (TypeError, ValueError):
                                pass
                elif 'on-chain retry limit exceeded' in line:
                    d = parse_line(line)
                    msg['on-chain retry limit exceeded'] += 1
                    if d is not None:
                        sn = d.get('simulationNum')
                        ls = d.get('lockedSimulationNum')
                        if sn is not None and ls is not None:
                            retry_lim[(int(sn), int(ls))] += 1
                            if len(retry_lim_detail) < 8:
                                retry_lim_detail.append((int(sn), int(ls)))
                else:
                    for marker in (
                        'cxt has timeout for sp1',
                        'simulation is pool timeout',
                        'lockWaitPool: expired',
                        'found DAG patches in PatchPool',
                        'consumed patches found, skipping TLV locks',
                        'chain tx detected',
                        'retry commit success',
                        'retry commit failed',
                        'retry commit success but local tx was wounded',
                        'tryToReSimulation: already in flight, skipping',
                        # DSN-54 广播 / 反查
                        'imported simDAGPatch subgraph',
                        'sent simDAGPatch subgraph to members',
                        'genuinely-uncovered lock conflict',
                    ):
                        if marker in line:
                            msg[marker] += 1
                            break

    # ── print ──
    print("\n===== CHAIN_RETRY_STATS (DAG / retry pipeline) =====")
    key_order = [
        'chainTxDetected', 'chainTxCRCommitted', 'retrySignalReceived',
        'retryCommitCall', 'retryCommitFail',
        'retryCommitRpcErr', 'retryCommitLocked', 'retryCommitWounded',
        'triggerReSim', 'retryCommitFailed', 'retryCommitCalled',
        'retryCommitWoundedPre', 'retryCommitPatchHit', 'retryCommitPatchMiss',
        'retryCommitPrevWounded', 'retryCommitTryLockOk',
        'retryCommitTryLockFail', 'retryCommitTryLockWounded',
        'tryReSimStarted',
        # DSN-54: covered-but-invisible(命中) vs genuinely-uncovered(真冲突)
        'simDAGPatchHit', 'simDAGPatchMiss',
        # DSN-55: chain-ready 局部 admission（前插放行 / 候选）+ rev2 得锁后保锁
        'chainReadyAdmission', 'chainReadyCandidate', 'dagHoldProtected',
    ]
    for k in key_order:
        if chain[k]:
            print(f"  {k:28s} {chain[k]:>12d}")
    # 语义标注：
    #  - chainDepthDist / chainDepthCommitDist：key = 真实 DAG 深度（node.Depth，DSN-50 判定链深用）
    #  - chainLengthDist / chainCommitDist：key = simulationNum（重试轮次），不是 DAG 深度，勿当深度读
    for k, c in sorted(chain_dist.items()):
        if k in ('chainDepthDist', 'chainDepthCommitDist'):
            print(f"  {k} = {dict(sorted(c.items()))}  <- key 是真实 DAG 深度(node.Depth)")
        else:
            print(f"  {k} = {dict(sorted(c.items()))}  <- key 是 simulationNum(重试轮次)，非 DAG 深度")

    print("\n===== STATS DUMP (tempLock / retry / timers) =====")
    for k in ('tempLockTryTotal', 'tempLockTryFail', 'retryAdd', 'retryReady',
              'retryNotReady', 'retrySuccess', 'retryFail',
              'sp1Started', 'sp1Fired', 'sp1Removed',
              'poolStarted', 'poolFired', 'poolRemoved'):
        if stats[k]:
            print(f"  {k:20s} {stats[k]:>12d}")

    print("\n===== timeout / rollback / DAG markers =====")
    for k in ('cxt has timeout for sp1', 'simulation is pool timeout',
              'lockWaitPool: expired', 'on-chain retry limit exceeded',
              'found DAG patches in PatchPool',
              'consumed patches found, skipping TLV locks',
              'chain tx detected', 'retry commit success', 'retry commit failed',
              'retry commit success but local tx was wounded',
              'tryToReSimulation: already in flight, skipping',
              # DSN-54 广播 / 反查
              'imported simDAGPatch subgraph',
              'sent simDAGPatch subgraph to members',
              'genuinely-uncovered lock conflict'):
        if msg[k]:
            print(f"  {k:50s} {msg[k]:>10d}")

    print("\n===== on-chain retry limit: (simNum, lockedSimNum) counts =====")
    for (sn, ls), c in sorted(retry_lim.items()):
        kind = 'MAX_TOTAL' if sn > 5 else ('MAX_ONCHAIN' if sn > ls + 2 else '?')
        print(f"  simNum={sn:>3} locked={ls:>3} : {c:>8d}  <- {kind}")

    print("\n===== DAG 判定 =====")
    tl_ok = chain.get('retryCommitTryLockOk', 0)
    tl_fail = chain.get('retryCommitTryLockFail', 0)
    patch_hit = chain.get('retryCommitPatchHit', 0)
    patch_miss = chain.get('retryCommitPatchMiss', 0)
    trig = chain.get('triggerReSim', 0)
    print(f"  TryLockOk={tl_ok} TryLockFail={tl_fail} "
          f"lockConflictRate={tl_fail/(tl_ok+tl_fail)*100 if tl_ok+tl_fail else 0:.1f}%")
    print(f"  PatchHit={patch_hit} PatchMiss={patch_miss} "
          f"DAG-rescue-share={patch_hit/(patch_hit+tl_fail)*100 if patch_hit+tl_fail else 0:.1f}% of conflicts")
    print(f"  triggerReSim={trig}")
    # 每块同 key 写入（leader Info 日志 [dagPerBlock]）：跨所有节点取最大 = 全局“一个 key 一块最多改几次”
    print(f"  dagPerBlockMaxSameKey={dag_pb['max']}  (blocks with same-key-writes>1: {dag_pb['blocks_gt1']})")

    print("\n===== DAG 去向 (chain-rescued tx fate, leader 口径) =====")
    committed = chain.get('chainTxCRCommitted', 0)
    no_commit = sum(chain_fate.values())
    print(f"  chainTxCRCommitted          {committed:>10d}  <- isChainTx 最终 CR commit")
    print(f"  chainTxNoCommitClose        {no_commit:>10d}  <- 被救起但以非 commit 终局 close")
    print(f"  ~仍在途/未终局(近似)         {max(trig - committed - no_commit, 0):>10d}  <- triggerReSim - 上面两项")
    for k, c in chain_fate.most_common(20):
        print(f"      reason={k:45s} {c:>8d}")

    # ── result.txt (if resolvable from RATE in dir name) ──
    import re, glob as _glob
    mrate = re.search(r'_rate=(\d+)_', logdir)
    if mrate:
        rate = mrate.group(1)
        results = sorted(_glob.glob(
            f"/home/zjnu/go/src/github.com/harmony-one/harmony-sscc/"
            f"data_process/output/throughput/RATE={rate}/HMY-SSCC/*_result.txt"))
        if results:
            print("\n===== result.txt (latest RATE=%s) =====" % rate)
            try:
                d = json.load(open(results[-1]))
                tx = d.get("transactions", {})
                print("  transactions:", {k: tx.get(k) for k in
                      ("total", "committed", "unfinished", "timeout", "rollback")})
                fr = d.get("failure_reasons")
                if fr:
                    top = sorted(fr.items(), key=lambda x: -x[1])[:10]
                    print("  failure_reasons:", top)
                ps = d.get("per_shard_status")
                if ps:
                    print("  per_shard:", ps)
            except Exception as e:
                print("  (result parse err)", e)
        else:
            print("\n  (no result.txt for rate=%s)" % rate)

    # ── leader close + rollback proof reason distribution ──
    print("\n===== leader close (commit:false) reason dist =====")
    _close = Counter()
    for f in files:
        with open(f, 'r', errors='replace') as fh:
            for line in fh:
                if 'leader close transaction' in line and 'commit: false' in line:
                    m = re.search(r'reason: ([^,}]+)', line)
                    _close[m.group(1) if m else '?'] += 1
    for k, c in _close.most_common(20):
        print(f"  {k:40s} {c:>8d}")

    print("\n===== rollback with proof reason dist =====")
    _rb = Counter()
    for f in files:
        with open(f, 'r', errors='replace') as fh:
            for line in fh:
                if 'rollback with proof' in line:
                    m = re.search(r'reason: ([^,}]+)', line)
                    _rb[m.group(1) if m else '?'] += 1
    for k, c in _rb.most_common(20):
        print(f"  {k:40s} {c:>8d}")


    # ── InvalidSimulation 细分：unmarshal vs execution ──
    print("\n===== InvalidSimulation 细分 =====")
    _inv = Counter()
    for f in files:
        with open(f, 'r', errors='replace') as fh:
            for line in fh:
                if 'failed to unmarshal simulation' in line:
                    _inv['failed to unmarshal simulation'] += 1
                elif 'failed to verify execution for call state' in line:
                    _inv['failed to verify execution'] += 1
    for k, c in _inv.most_common(20):
        print(f"  {k:50s} {c:>10d}")

    # ── sample verify execution error messages ──
    print("\n===== sample execution-verify error (msg) =====")
    _errmsg = Counter()
    n = 0
    for f in files:
        with open(f, 'r', errors='replace') as fh:
            for line in fh:
                if 'failed to verify execution for call state' in line:
                    d = parse_line(line)
                    if d is not None and d.get('error'):
                        _errmsg[d['error'][:90]] += 1
                        n += 1
                        if n >= 20000:
                            break
        if n >= 20000:
            break
    for k, c in _errmsg.most_common(15):
        print(f"  {c:>8d}  {k}")

    # ── sample unmarshal error ──
    print("\n===== sample unmarshal error (msg) =====")
    _u = Counter()
    for f in files:
        with open(f, 'r', errors='replace') as fh:
            for line in fh:
                if 'failed to unmarshal simulation' in line:
                    d = parse_line(line)
                    if d is not None and d.get('error'):
                        _u[d['error'][:90]] += 1
    for k, c in _u.most_common(10):
        print(f"  {c:>8d}  {k}")

if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else '.')
