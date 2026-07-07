#!/usr/bin/env python3
"""
单交易全链路耗时追踪 —— 按 txHash 筛选所有 timing breakdown 日志，输出时间线。

用法：
  # 单交易追踪（默认）
  ./trace-tx-timing.py 0x2fc6a83506a2f8
  ./trace-tx-timing.py 0x2fc6a83506a2f8 /path/to/logdir/

  # Top N 耗时最长的交易
  ./trace-tx-timing.py --top 20
  ./trace-tx-timing.py --top 10 /path/to/logdir/

输出：
  追踪模式: 时间线 + 阶段汇总
  Top N 模式: 交易排名 + 各阶段耗时明细
"""

import json, sys, glob, os
from datetime import datetime, timezone
from collections import OrderedDict, defaultdict

# ============================================================
# 19 个 timing breakdown 的定义，与 analyze-all-timing.py 一致
# ============================================================
TIMING_MARKERS = OrderedDict({
    # === 已有埋点（9 个）===
    'CommitOrRollbackWithProof': {
        'marker': 'CommitOrRollbackWithProof timing breakdown',
        'fields': ['unmarshal','isFinished','commitTx','closeTx','total'],
        'stage': 'CR',
    },
    'closeTransaction': {
        'marker': 'closeTransaction timing breakdown',
        'fields': ['stateLock','simCleanup','verCleanup','totalClose'],
        'stage': 'CloseTx',
    },
    'Simulator.Cleanup': {
        'marker': 'Simulator.Cleanup timing',
        'fields': ['delSimState','pendingLock','simuLock'],
        'stage': 'SimCleanup',
    },
    'Verifier.Cleanup': {
        'marker': 'Verifier.Cleanup timing',
        'fields': ['duration'],
        'stage': 'VerCleanup',
    },
    'retryScheduler.StaleTx': {
        'marker': 'retryScheduler.StaleTx timing',
        'fields': ['duration'],
        'stage': 'StaleTx',
    },
    'RemoveOnChainPatch': {
        'marker': 'RemoveOnChainPatch timing',
        'fields': ['duration'],
        'stage': 'RmOnChain',
    },
    'CXTTimerManager.RemoveTx': {
        'marker': 'CXTTimerManager.RemoveTx timing',
        'fields': ['duration'],
        'stage': 'RmTimer',
    },
    'PatchPool.Remove': {
        'marker': 'PatchPool.Remove timing',
        'fields': ['duration'],
        'stage': 'RmPatchPool',
    },
    'RemoveFromPassivePool': {
        'marker': 'RemoveFromPassivePool timing',
        'fields': ['duration'],
        'stage': 'RmPassive',
    },
    # === 模拟管线（4 个）===
    'processSimulationTask': {
        'marker': 'processSimulationTask timing breakdown',
        'fields': ['queueWait','p2pCall','total'],
        'stage': 'SimTask',
    },
    'StartSimulateCXTransaction': {
        'marker': 'StartSimulateCXTransaction timing breakdown',
        'fields': ['callMembers','aggregate','thresholdSign','commitSend','total'],
        'stage': 'StartSim',
    },
    'CommitSimulation': {
        'marker': 'CommitSimulation timing breakdown',
        'fields': ['buildCallStates','buildSignatures','onChainPatch','submitTx','total'],
        'stage': 'CommitSim',
    },
    'HandleCommitVote': {
        'marker': 'HandleCommitVote timing breakdown',
        'fields': ['voteTracking','aggregate','sendVote','total'],
        'stage': 'CommitVote',
    },
    # === 重试管线（6 个）===
    'AddToRetry': {
        'marker': 'AddToRetry timing breakdown',
        'fields': ['extractRWSet','poolInsert','total'],
        'stage': 'AddRetry',
    },
    'chainNextSim': {
        'marker': 'chainNextSim timing breakdown',
        'fields': ['scanPool','reserveSelect','sendSignals','total'],
        'stage': 'ChainNext',
    },
    'HandleRetrySignal': {
        'marker': 'HandleRetrySignal timing breakdown',
        'fields': ['setPatch','poolLookup','dispatch','total'],
        'stage': 'RetrySignal',
    },
    'tryToReSimulation': {
        'marker': 'tryToReSimulation timing breakdown',
        'fields': ['inFlightCheck','retryCalls','resultCheck','triggerSim','total'],
        'stage': 'TryReSim',
    },
    'StartReSimulation': {
        'marker': 'StartReSimulation timing breakdown',
        'fields': ['getState','callMembers','aggregate','thresholdSign','commitSend','total'],
        'stage': 'StartReSim',
    },
    'RetryCommit': {
        'marker': 'RetryCommit timing breakdown',
        'fields': ['passiveCheck','patchPoolCheck','tryLock','stateDbCheck','total'],
        'stage': 'RetryCommit',
    },
    # === 新增锁操作与验证 timing ===
    'VerifySimulation': {
        'marker': 'VerifySimulation timing breakdown',
        'fields': ['chainPatch','lockCheck','execVerify','lockState','total'],
        'stage': 'VerifySim',
    },
    'lockStateWithRWSet': {
        'marker': 'lockStateWithRWSet timing breakdown',
        'fields': ['keys','cost'],
        'stage': 'lockRWSet',
    },
    'lockStateWithExecution': {
        'marker': 'lockStateWithExecution timing breakdown',
        'fields': ['cost'],
        'stage': 'lockExec',
    },
    'locker.Commit': {
        'marker': 'locker.Commit timing breakdown',
        'fields': ['muLockWait','muLockHold','applyTo','snapshot','total'],
        'stage': 'LkrCommit',
    },
    'CommitTx': {
        'marker': 'CommitTx timing breakdown',
        'fields': ['cost'],
        'stage': 'CommitTx',
    },
    'RollbackTx': {
        'marker': 'RollbackTx timing breakdown',
        'fields': ['cost'],
        'stage': 'RollbackTx',
    },
    'pendingUnlocks.applyTo': {
        'marker': 'pendingUnlocks.applyTo timing breakdown',
        'fields': ['unlockedTxs','cost'],
        'stage': 'ApplyTo',
    },
})

# 其他关注的日志（非 timing breakdown，但有 txHash）
WATCH_MARKERS = OrderedDict({
    'LOCK_STALE': {
        'marker': 'LOCK_STALE',
        'fields': ['heldBlocks'],
        'stage': 'LOCK_STALE',
    },
})

# 所有标记合并
ALL_MARKERS = OrderedDict()
ALL_MARKERS.update(TIMING_MARKERS)
ALL_MARKERS.update(WATCH_MARKERS)


def parse_dur(s):
    """将 '1.234ms' / '45.2µs' / '100ns' / '2.5s' 转为毫秒数"""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    s = s.strip()
    try:
        if s.endswith('ms'): return float(s[:-2])
        if s.endswith('µs') or s.endswith('us'): return float(s[:-2]) / 1000
        if s.endswith('ns'): return float(s[:-2]) / 1_000_000
        if s.endswith('s'):  return float(s[:-1]) * 1000
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_time(ts_str):
    """解析时间戳字符串"""
    if not ts_str: return None
    try:
        if '.' in ts_str:
            dt = datetime.strptime(ts_str[:26], '%Y-%m-%dT%H:%M:%S.%f')
        else:
            dt = datetime.strptime(ts_str[:19], '%Y-%m-%dT%H:%M:%S')
        # 时区信息忽略（同一实验内时区一致）
        return dt
    except (ValueError, IndexError):
        return None


def resolve_log_files():
    """自动定位日志目录（与 analyze-all-timing.py 一致）"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_base = script_dir
    for _ in range(3):
        env_path = os.path.join(log_base, '.env')
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith('#'):
                        k, v = line.split('=', 1)
                        os.environ[k.strip()] = v.strip()
            parts = [
                f"shard={os.environ.get('SHARD_NUM', '4')}",
                f"validator={os.environ.get('VALIDATOR', '4')}",
                f"ssc={os.environ.get('SSC', '1')}",
                f"delay={os.environ.get('DELAY', '10')}",
                f"rate={os.environ.get('RATE', '100')}",
                f"vpn={os.environ.get('VALIDATOR_PER_NODE', '4')}",
            ]
            logdir = os.path.join(log_base, '_'.join(parts))
            if os.path.isdir(logdir):
                return sorted(glob.glob(os.path.join(logdir, 'ssc-validator-*.log')))
        log_base = os.path.dirname(log_base)
    # fallback: cwd + 首个匹配的 vpn=4 子目录（排除 cc=1 等变体）
    files = sorted(glob.glob('ssc-validator-*.log'))
    if not files:
        for d in sorted(os.listdir('.')):
            if not os.path.isdir(os.path.join('.', d)):
                continue
            if '_cc=' in d:
                continue
            if not d.endswith('_vpn=4'):
                continue
            sub = os.path.join('.', d)
            files = sorted(glob.glob(os.path.join(sub, 'ssc-validator-*.log')))
            if files:
                print(f'📂 {sub}')
                break
    return files


def fmt_ms(ms):
    """友好格式化毫秒数"""
    if ms is None: return 'N/A'
    if ms >= 10000: return f'{ms/1000:.2f}s'
    if ms >= 1000:  return f'{ms/1000:.1f}s'
    if ms >= 1:     return f'{ms:.1f}ms'
    if ms >= 0.001: return f'{ms*1000:.1f}µs'
    return f'{ms*1000:.0f}µs'


# ============================================================
# Top N 模式
# ============================================================

def cmd_top(n_top):
    """列出耗时最长的 N 个交易"""
    files = resolve_log_files()
    if not files:
        print('❌ 未找到 ssc-validator-*.log 文件。'); sys.exit(1)

    print(f'📁 扫描 {len(files)} 个日志文件...\n')

    # 逐行扫描，收集每个 txHash 的：
    #   timestamps: 最早/最晚时间
    #   stage_totals: 每个 stage 的 total/总时字段值
    #   stage_count: 每个 stage 的出现次数
    #   max_hold: LOCK_STALE 的最大 heldBlocks
    tx_data = defaultdict(lambda: {
        'first_ts': None, 'last_ts': None,
        'stage_totals': defaultdict(list),
        'stage_count': defaultdict(int),
        'max_hold': 0, 'hold_count': 0,
    })

    total_lines = 0
    for fpath in files:
        with open(fpath) as fp:
            for line in fp:
                total_lines += 1
                try:
                    d = json.loads(line.strip())
                except json.JSONDecodeError:
                    continue

                msg = d.get('message', '')
                tx_hash = d.get('txHash', '').lower()
                if not tx_hash or tx_hash == '0x0000000000000000000000000000000000000000000000000000000000000000':
                    continue

                # 匹配 marker
                matched = None
                for name, cfg in ALL_MARKERS.items():
                    if cfg['marker'] in msg:
                        matched = (name, cfg)
                        break
                if not matched:
                    continue

                name, cfg = matched
                data = tx_data[tx_hash]

                # 时间戳
                ts = parse_time(d.get('time', ''))
                if ts:
                    if data['first_ts'] is None or ts < data['first_ts']:
                        data['first_ts'] = ts
                    if data['last_ts'] is None or ts > data['last_ts']:
                        data['last_ts'] = ts

                # stage 计数
                data['stage_count'][name] += 1

                # total 字段
                total_field = None
                for fname in ('total', 'totalClose', 'duration', 'cost'):
                    raw = d.get(fname)
                    if raw:
                        parsed = parse_dur(raw)
                        if parsed is not None:
                            total_field = parsed
                            break
                if total_field is not None:
                    data['stage_totals'][name].append(total_field)

                # LOCK_STALE
                if name == 'LOCK_STALE':
                    held = parse_dur(d.get('heldBlocks'))
                    if held and held > data['max_hold']:
                        data['max_hold'] = int(held)
                    data['hold_count'] += 1

    if not tx_data:
        print(f'⚠️  未匹配到任何交易数据（扫描 {total_lines} 行）'); sys.exit(0)

    # 计算每个交易的统计
    ranked = []
    for tx_hash, data in tx_data.items():
        wall_ms = None
        if data['first_ts'] and data['last_ts']:
            wall_ms = (data['last_ts'] - data['first_ts']).total_seconds() * 1000

        # 各 stage 的总耗时（取 P99 或 avg）
        stage_p99 = {}
        stage_avg = {}
        stage_max = {}
        stage_n = dict(data['stage_count'])
        for stagename, vals in data['stage_totals'].items():
            sv = sorted(vals)
            n = len(sv)
            stage_p99[stagename] = sv[int(n * 0.99)] if n > 1 else sv[0]
            stage_avg[stagename] = sum(sv) / n if n else 0
            stage_max[stagename] = max(sv)

        ranked.append({
            'tx': tx_hash,
            'wall_ms': wall_ms,
            'wall_s': f'{wall_ms/1000:.3f}s' if wall_ms else 'N/A',
            'stage_p99': stage_p99,
            'stage_avg': stage_avg,
            'stage_max': stage_max,
            'stage_n': stage_n,
            'max_hold': data['max_hold'],
            'hold_count': data['hold_count'],
        })

    # 按 wall time 降序
    ranked.sort(key=lambda x: -(x['wall_ms'] or 0))

    top_n = ranked[:n_top]

    # 打印
    print(f'🏆 耗时最长的 {len(top_n)} 个交易（扫描 {total_lines} 行, {len(tx_data)} 个 tx）\n')

    # 列定义
    all_stages = list(TIMING_MARKERS.keys())
    # 只显示有数据的 stage
    active_stages = []
    for s in all_stages:
        if any(s in r['stage_n'] for r in top_n):
            active_stages.append(s)

    # 表头
    header = f'{"#":>3s}  {"txHash":>20s}  {"总耗时":>10s}  {"重试":>5s}  {"锁定":>8s}'
    for s in active_stages:
        label = TIMING_MARKERS[s]['stage']
        header += f'  {label:>12s}'
    print(header)
    print('─' * len(header))

    for i, r in enumerate(top_n):
        wall_str = r['wall_s']
        retry_n = r['stage_n'].get('StartReSimulation', 0)
        hold_str = f'{r["hold_count"]}计{r["max_hold"]}b'

        tx_short = r['tx'][:20]
        line = f'{i+1:3d}  {tx_short:>20s}  {wall_str:>10s}  {retry_n:>5d}  {hold_str:>8s}'
        for s in active_stages:
            vals = r['stage_p99'].get(s)
            if vals is not None:
                line += f'  {fmt_ms(vals):>12s}'
            else:
                line += f'  {"—":>12s}'
        print(line)

    # 底部附注
    print()
    print(f'  列说明: 总耗时=首次出现到末次出现的 wall time')
    print(f'          重试=StartReSimulation 调用次数')
    print(f'          锁定=LOCK_STALE 出现次数 / 最大 heldBlocks')
    print(f'          各 stage = 该 stage total 字段的 P99 值')
    print()
    print(f'  查看单个交易详细时间线: {sys.argv[0]} <txHash>')


# ============================================================
# 单交易追踪模式
# ============================================================

def cmd_trace(target_hash):
    """追踪单个交易的所有阶段"""
    files = []
    for arg in sys.argv[2:]:
        if arg.startswith('--'): continue  # 跳过 --top 等参数
        if os.path.isdir(arg):
            files.extend(sorted(glob.glob(os.path.join(arg, 'ssc-validator-*.log'))))
        else:
            files.extend(sorted(glob.glob(arg)))
    if not files:
        files = resolve_log_files()

    if not files:
        print(f'❌ 未找到 ssc-validator-*.log 文件。')
        print(f'   请指定文件路径: {sys.argv[0]} {target_hash} /path/to/ssc-validator-*.log')
        sys.exit(1)

    print(f'🔍 追踪交易 {target_hash}')
    print(f'📁 扫描 {len(files)} 个日志文件\n')

    entries = []
    total_lines = 0

    for fpath in files:
        fname = os.path.basename(fpath)
        with open(fpath) as fp:
            for line in fp:
                total_lines += 1
                if target_hash not in line.lower():
                    continue
                try:
                    d = json.loads(line.strip())
                except json.JSONDecodeError:
                    continue

                msg = d.get('message', '')
                matched = None
                for name, cfg in ALL_MARKERS.items():
                    if cfg['marker'] in msg:
                        matched = (name, cfg)
                        break

                if not matched:
                    continue

                name, cfg = matched
                shard = d.get('shardId') or d.get('port', '?')

                fields = OrderedDict()
                for f in cfg['fields']:
                    raw = d.get(f)
                    parsed = parse_dur(raw) if isinstance(raw, str) else raw
                    if parsed is not None:
                        fields[f] = f'{parsed:.2f}ms' if isinstance(parsed, (int, float)) else str(parsed)
                    elif raw is not None:
                        fields[str(f)] = str(raw)

                for extra in ('cost', 'duration', 'simulationNum', 'heldBlocks', 'reason', 'simNum'):
                    if extra not in cfg['fields'] and extra in d:
                        val = d[extra]
                        parsed = parse_dur(val) if isinstance(val, str) else val
                        if parsed is not None:
                            fields[extra] = f'{parsed:.2f}ms' if isinstance(parsed, (int, float)) else str(parsed)
                        else:
                            fields[extra] = str(val)[:40]

                ts_str = d.get('time', '')
                ts = parse_time(ts_str)

                entries.append({
                    'ts': ts, 'raw_ts': ts_str,
                    'stage': cfg['stage'],
                    'fields': fields,
                    'shard': shard,
                    'name': name,
                    'msg': msg[:60],
                    'file': fname,
                })

    if not entries:
        print(f'⚠️  未找到交易 {target_hash} 的 timing 日志。')
        print(f'   扫描了 {total_lines} 行日志，跨 {len(files)} 个文件。')
        sys.exit(0)

    entries.sort(key=lambda e: (e['ts'] or datetime.min, e['raw_ts'] or ''))

    origin = None
    for e in entries:
        if e['ts']:
            origin = e['ts']
            break

    print(f'📊 共 {len(entries)} 条日志，扫描 {total_lines} 行\n')

    # 分类统计
    stage_count = {}
    for e in entries:
        stage_count[e['name']] = stage_count.get(e['name'], 0) + 1

    print('── 分类统计 ──')
    for name, count in sorted(stage_count.items(), key=lambda x: -x[1]):
        cfg = ALL_MARKERS.get(name, {})
        label = cfg.get('stage', name) if isinstance(cfg, dict) else name
        print(f'  {label:18s}  ×{count}')
    print()

    # 时间线
    print('── 时间线 ──')
    prev_ts = None
    total_gap = 0.0
    for e in entries:
        elapsed = -1
        if origin and e['ts']:
            elapsed = (e['ts'] - origin).total_seconds()

        time_str = f'+{elapsed:.3f}s' if elapsed >= 0 else '??.???s'

        # gap 行：上次日志到这次之间未计入的时间
        gap = 0.0
        if prev_ts and e['ts']:
            gap = (e['ts'] - prev_ts).total_seconds()
        if gap > 0.5:  # >500ms 的间隔标出来
            total_gap += gap
            print(f'  [{time_str}]  {"…wait…":18s}  shard={" ":6s}  Δ{gap:.3f}s  (未计入 timing)')

        field_parts = []
        for k, v in e['fields'].items():
            field_parts.append(f'{k}={v}')
        field_str = '  '.join(field_parts) if field_parts else e['msg']

        print(f'  [{time_str}]  {e["stage"]:18s}  shard={e["shard"]:6s}  {field_str}')
        prev_ts = e['ts']

    # 汇总：gap 总占比
    if total_gap > 0:
        all_tss_timeline = [e['ts'] for e in entries if e['ts']]
        if all_tss_timeline:
            wall = (max(all_tss_timeline) - min(all_tss_timeline)).total_seconds()
            print(f'\n  ⚠️  未计入的块间等待总计: {total_gap:.3f}s / {wall:.3f}s = {total_gap/wall*100:.0f}%')

    # 阶段汇总
    print()
    print('── 阶段耗时汇总 ──')
    stage_timeline = {}
    for e in entries:
        if e['name'] not in stage_timeline:
            stage_timeline[e['name']] = {'first_ts': e['ts'], 'last_ts': e['ts'], 'count': 1}
        else:
            stage_timeline[e['name']]['last_ts'] = e['ts']
            stage_timeline[e['name']]['count'] += 1
            if e['ts'] and e['ts'] < stage_timeline[e['name']]['first_ts']:
                stage_timeline[e['name']]['first_ts'] = e['ts']

    all_tss = [e['ts'] for e in entries if e['ts']]
    if all_tss:
        total_time = (max(all_tss) - min(all_tss)).total_seconds()
        print(f'  总跨度: {total_time:.3f}s')

    for name, data in sorted(stage_timeline.items(), key=lambda x: (
        (x[1]['first_ts'] or datetime.max) if x[1]['first_ts'] else datetime.max
    )):
        cfg = ALL_MARKERS.get(name, {})
        label = cfg.get('stage', name) if isinstance(cfg, dict) else name
        first = data['first_ts']
        last = data['last_ts']
        if first and last and origin:
            start_offset = (first - origin).total_seconds()
            duration = (last - first).total_seconds() if last != first else None
            if duration is not None:
                print(f'  {label:18s}  +{start_offset:.3f}s  Δ{duration:.3f}s  ×{data["count"]}')
            else:
                print(f'  {label:18s}  +{start_offset:.3f}s  ×{data["count"]}')


# ============================================================
# 入口
# ============================================================

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    if sys.argv[1] == '--top':
        n = 20
        if len(sys.argv) > 2 and sys.argv[2].isdigit():
            n = int(sys.argv[2])
        cmd_top(n)
    else:
        target_hash = sys.argv[1].lower()
        if not target_hash.startswith('0x'):
            target_hash = '0x' + target_hash
        cmd_trace(target_hash)


if __name__ == '__main__':
    main()
