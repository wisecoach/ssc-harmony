#!/usr/bin/env python3
"""
closeTransaction 全链路耗时分析 —— 收集全部 19 个 timing breakdown（模拟管线 + 重试管线 + 关闭管线）

子日志级别（共 19 个 marker）：
  已有（9 个）：
    closeTransaction timing breakdown     → stateLock, simCleanup, verCleanup, totalClose
    Simulator.Cleanup timing              → delSimState, pendingLock, simuLock
    Verifier.Cleanup timing               → duration
    retryScheduler.StaleTx timing         → duration
    RemoveOnChainPatch timing             → duration
    CXTTimerManager.RemoveTx timing       → duration
    PatchPool.Remove timing               → duration
    RemoveFromPassivePool timing          → duration
    CommitOrRollbackWithProof timing breakdown → unmarshal, isFinished, commitTx, closeTx, total

  新增模拟管线（4 个）：
    processSimulationTask timing          → queueWait, p2pCall, total
    StartSimulateCXTransaction timing     → callMembers, aggregate, thresholdSign, commitSend, total
    CommitSimulation timing               → buildCallStates, buildSignatures, onChainPatch, submitTx, total
    HandleCommitVote timing               → voteTracking, aggregate, sendVote, total

  新增重试管线（6 个）：
    AddToRetry timing                     → extractRWSet, poolInsert, total
    chainNextSim timing                   → scanPool, reserveSelect, sendSignals, total
    HandleRetrySignal timing              → setPatch, poolLookup, dispatch, total
    tryToReSimulation timing              → inFlightCheck, retryCalls, resultCheck, triggerSim, total
    StartReSimulation timing              → getState, callMembers, aggregate, thresholdSign, commitSend, total
    RetryCommit timing                    → passiveCheck, patchPoolCheck, tryLock, stateDbCheck, total

用法（与 ssc_grep.sh 一致的目录定位）：
  ./analyze-all-timing.py                # 使用 .env 定位日志目录
  ./analyze-all-timing.py <file|glob>    # 或指定文件
"""
import json, sys, glob, os
from collections import defaultdict

def resolve_logdir():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_base = script_dir
    for _ in range(3):
        env_path = os.path.join(script_dir, '.env')
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        k, v = line.split('=', 1)
                        os.environ[k.strip()] = v.strip()
            parts = [
                f"shard={os.environ['SHARD_NUM']}",
                f"validator={os.environ['VALIDATOR']}",
                f"ssc={os.environ['SSC']}",
                f"delay={os.environ['DELAY']}",
                f"rate={os.environ['RATE']}",
                f"vpn={os.environ['VALIDATOR_PER_NODE']}",
            ]
            return os.path.join(log_base, '_'.join(parts))
        script_dir = os.path.dirname(script_dir)
    return None

def parse_dur(s):
    if not s: return 0
    if isinstance(s, (int, float)): return float(s)
    s = s.strip()
    if s.endswith('ms'):   return float(s[:-2])
    if s.endswith('µs'):   return float(s[:-2]) / 1000
    if s.endswith('ns'):   return float(s[:-2]) / 1_000_000
    if s.endswith('s'):    return float(s[:-1]) * 1000
    return 0

files = []
for arg in sys.argv[1:]:
    files.extend(sorted(glob.glob(arg)))
if not files:
    logdir = resolve_logdir()
    if logdir and os.path.isdir(logdir):
        files = sorted(glob.glob(os.path.join(logdir, 'ssc-validator-*.log')))
        print(f"\U0001f4c2 {logdir}")
    else:
        files = sorted(glob.glob('ssc-validator-*.log'))
if not files:
    print("\u274c 未找到 ssc-validator-*.log 文件。"); sys.exit(1)

LOG_MARKERS = {
    # === 已有埋点（9 个）===
    'closeTransaction': ('closeTransaction timing breakdown',
        ['stateLock','simCleanup','verCleanup','totalClose']),
    'Simulator.Cleanup': ('Simulator.Cleanup timing',
        ['delSimState','pendingLock','simuLock']),
    'Verifier.Cleanup': ('Verifier.Cleanup timing', ['duration']),
    'retryScheduler.StaleTx': ('retryScheduler.StaleTx timing', ['duration']),
    'RemoveOnChainPatch': ('RemoveOnChainPatch timing', ['duration']),
    'CXTTimerManager.RemoveTx': ('CXTTimerManager.RemoveTx timing', ['duration']),
    'PatchPool.Remove': ('PatchPool.Remove timing', ['duration']),
    'RemoveFromPassivePool': ('RemoveFromPassivePool timing', ['duration']),
    'CommitOrRollbackWithProof': ('CommitOrRollbackWithProof timing breakdown',
        ['unmarshal','isFinished','commitTx','closeTx','total']),
    # === 新增模拟管线（4 个）===
    'processSimulationTask': ('processSimulationTask timing breakdown',
        ['queueWait', 'p2pCall', 'total']),
    'StartSimulateCXTransaction': ('StartSimulateCXTransaction timing breakdown',
        ['callMembers', 'aggregate', 'thresholdSign', 'commitSend', 'total']),
    'CommitSimulation': ('CommitSimulation timing breakdown',
        ['buildCallStates', 'buildSignatures', 'onChainPatch', 'submitTx', 'total']),
    'HandleCommitVote': ('HandleCommitVote timing breakdown',
        ['voteTracking', 'aggregate', 'sendVote', 'total']),
    # === 新增重试管线（6 个）===
    'AddToRetry': ('AddToRetry timing breakdown',
        ['extractRWSet', 'poolInsert', 'total']),
    'chainNextSim': ('chainNextSim timing breakdown',
        ['scanPool', 'reserveSelect', 'sendSignals', 'total']),
    'HandleRetrySignal': ('HandleRetrySignal timing breakdown',
        ['setPatch', 'poolLookup', 'dispatch', 'total']),
    'tryToReSimulation': ('tryToReSimulation timing breakdown',
        ['inFlightCheck', 'retryCalls', 'resultCheck', 'triggerSim', 'total']),
    'StartReSimulation': ('StartReSimulation timing breakdown',
        ['getState', 'callMembers', 'aggregate', 'thresholdSign', 'commitSend', 'total']),
    'RetryCommit': ('RetryCommit timing breakdown',
        ['passiveCheck', 'patchPoolCheck', 'tryLock', 'stateDbCheck', 'total']),
    # === 新增锁操作与验证 timing（8 个）===
    'VerifySimulation': ('VerifySimulation timing breakdown',
        ['chainPatch','lockCheck','execVerify','lockState','total']),
    'lockStateWithRWSet': ('lockStateWithRWSet timing breakdown',
        ['keys','cost']),
    'lockStateWithExecution': ('lockStateWithExecution timing breakdown',
        ['cost']),
    'locker.Commit': ('locker.Commit timing breakdown',
        ['muLockWait','muLockHold','applyTo','snapshot','total']),
    'pendingUnlocks.applyTo': ('pendingUnlocks.applyTo timing breakdown',
        ['unlockedTxs','cost']),
    'handleLockCommit': ('locker snapshot created',
        ['deepCopy','staleCheck','total']),
    'AddOnChainPatch': ('AddOnChainPatch: stored from SimTx',
        ['cost']),
    'TempLockView.OnBlockCommitted': ('TempLockView.OnBlockCommitted timing breakdown',
        ['buildList','cleanupLoop','total']),
    'CommitTx': ('CommitTx timing breakdown',
        ['cost']),
    'RollbackTx': ('RollbackTx timing breakdown',
        ['cost']),
    'retryScheduler.OnBlockCommitted': ('retryScheduler onBlockCommitted timing breakdown',
        ['tempLock','staleClean','retryRange','passiveScan','total']),
}

all_stats = {}
for key, (marker, fields) in LOG_MARKERS.items():
    all_stats[key] = (marker, {f: [] for f in fields})

for f in files:
    with open(f) as fp:
        for line in fp:
            for key, (marker, fields) in LOG_MARKERS.items():
                if marker not in line:
                    continue
                try:
                    d = json.loads(line.strip())
                except json.JSONDecodeError:
                    continue
                for field in fields:
                    v = d.get(field, '')
                    if v:
                        all_stats[key][1][field].append(parse_dur(v))
                break

for key in LOG_MARKERS:
    marker, stats = all_stats[key]
    has_data = any(len(v) > 0 for v in stats.values())
    if not has_data:
        continue
    print(f'\n{"="*50}\n  {key}\n{"="*50}')
    for field in LOG_MARKERS[key][1]:
        vals = stats[field]
        if not vals:
            print(f'  {field:20s}: 0 samples'); continue
        vals.sort(); n = len(vals)
        print(f'  {field:20s}  n={n:5d}  P50={vals[n//2]:8.2f}ms  '
              f'P90={vals[int(n*0.9)]:8.2f}ms  '
              f'P99={vals[int(n*0.99)]:8.2f}ms  '
              f'avg={sum(vals)/n:8.2f}ms  max={vals[-1]:8.2f}ms')