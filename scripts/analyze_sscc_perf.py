#!/usr/bin/env python3
"""
Harmony SSCC 性能埋点分析脚本。

自动读取 ../../.env 定位日志目录，跨所有 ssc-validator-*.log 文件分析 [perf] 事件。

用法：
  python3 analyze_sscc_perf.py                    # 自动选 .env 对应的日志目录
  python3 analyze_sscc_perf.py <日志文件或目录>    # 手动指定
  python3 analyze_sscc_perf.py --top 20
  python3 analyze_sscc_perf.py --cat verify
  python3 analyze_sscc_perf.py --raw              # 原始事件明细
"""

import json
import os
import re
import sys
from collections import defaultdict

DUR_RE = re.compile(r'^(\d+(?:\.\d+)?)(µs|μs|us|ms|s|m)$')


def parse_dur(s):
    """将 "1.234ms"、"50µs" 或 float(ms) 转成 ms"""
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    if not isinstance(s, str):
        return 0.0
    m = DUR_RE.match(s)
    if not m:
        s2 = s.replace('µs', 'us').replace('μs', 'us')
        if s2.endswith('ms'):
            return float(s2[:-2])
        elif s2.endswith('us'):
            return float(s2[:-2]) / 1000.0
        elif s2.endswith('s'):
            return float(s2[:-1]) * 1000.0
        elif s2.endswith('m'):
            return float(s2[:-1]) * 60000.0
        return 0.0
    val = float(m.group(1))
    unit = m.group(2).lower()
    if unit in ('us', 'µs', 'μs'):
        return val / 1000.0
    elif unit == 'ms':
        return val
    elif unit == 's':
        return val * 1000.0
    elif unit == 'm':
        return val * 60000.0
    return 0.0


def find_log_dir(script_dir=None):
    """
    自动定位日志目录：
    1. 如果传入的是文件或目录，直接返回
    2. 否则读 ../../.env（脚本在 logs/harmony-sscc/ 下）构造目录名
    """
    if script_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))

    # 尝试读环境文件
    env_file = os.path.join(script_dir, "../../.env")
    env = {}
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()

    if env.get("SHARD_NUM"):
        parts = [
            f"shard={env['SHARD_NUM']}",
            f"validator={env['VALIDATOR']}",
            f"ssc={env.get('SSC', '1')}",
            f"delay={env.get('DELAY', '10')}",
            f"rate={env.get('RATE', '100')}",
            f"vpn={env.get('VALIDATOR_PER_NODE', '4')}",
        ]
        dir_name = "_".join(parts)
        log_dir = os.path.join(script_dir, dir_name)
        if os.path.isdir(log_dir):
            return log_dir

    # fallback：找最新目录
    dirs = [d for d in os.listdir(script_dir)
            if d.startswith("shard=") and os.path.isdir(os.path.join(script_dir, d))]
    if dirs:
        return os.path.join(script_dir, sorted(dirs)[-1])

    return script_dir


def collect_log_files(log_path):
    """返回要分析的 .log 文件列表"""
    if os.path.isfile(log_path):
        return [log_path]
    if os.path.isdir(log_path):
        return sorted([
            os.path.join(log_path, f) for f in os.listdir(log_path)
            if f.startswith("ssc-validator") and f.endswith(".log")
        ])
    return []


def extract_perf_events(log_files):
    """从文件列表中提取 [perf] 事件"""
    events = []
    total_lines = 0
    perf_count = 0

    for fpath in log_files:
        with open(fpath, 'r', errors='ignore') as f:
            for line in f:
                total_lines += 1
                if '[perf]' not in line:
                    continue
                try:
                    start = line.index('{')
                    end = line.rindex('}') + 1
                    data = json.loads(line[start:end])
                except (ValueError, json.JSONDecodeError):
                    continue
                if not all(k in data for k in ('cat', 'func', 'phase', 'total', 'cnt')):
                    continue
                events.append({
                    'cat': data['cat'],
                    'func': data['func'],
                    'phase': data['phase'],
                    'total_ms': parse_dur(data.get('total', 0)),
                    'cnt': int(data.get('cnt', 0)),
                    'avg_ms': parse_dur(data.get('avg', 0)),
                    'max_ms': parse_dur(data.get('max', 0)),
                    'burstCnt': int(data.get('burstCnt', 0)),
                    'warmup': data.get('warmup', False),
                })
                perf_count += 1

    return events, total_lines, perf_count


def print_summary(events, top_n=15, cat_filter=None):
    if not events:
        print("❌ 未找到 [perf] 事件（日志不包含埋点，需要重新编译部署后跑实验）")
        return

    grouped = defaultdict(lambda: {
        'total_ms': 0.0, 'cnt': 0, 'max_ms': 0.0,
        'total_burst': 0, 'windows': 0
    })

    for ev in events:
        if cat_filter and ev['cat'] != cat_filter:
            continue
        key = (ev['cat'], ev['func'], ev['phase'])
        g = grouped[key]
        g['total_ms'] += ev['total_ms']
        g['cnt'] += ev['cnt']
        g['max_ms'] = max(g['max_ms'], ev['max_ms'])
        g['total_burst'] += ev['burstCnt']
        g['windows'] += 1

    if not grouped:
        print(f"❌ cat='{cat_filter}' 无匹配事件")
        return

    sorted_items = sorted(grouped.items(), key=lambda x: x[1]['total_ms'], reverse=True)

    cats = defaultdict(list)
    for key, g in sorted_items:
        cat, func, phase = key
        avg_ms = g['total_ms'] / g['windows'] if g['windows'] > 0 else 0
        cats[cat].append((func, phase, g, avg_ms))

    for cat_name in sorted(cats.keys()):
        items = cats[cat_name]
        total_total = sum(g['total_ms'] for _, _, g, _ in items)
        print(f"\n{'='*80}")
        print(f"  📊 {cat_name:30s}  总耗时: {total_total:.1f}ms  |  {len(items)} 个阶段")
        print(f"{'='*80}")
        print(f"  {'阶段':30s} {'total(ms)':>10} {'cnt':>6} {'avg(ms)':>8} {'max(ms)':>8} {'burst':>5} {'windows':>7}")
        print(f"  {'-'*76}")
        for func, phase, g, avg_ms in items:
            label = f"{func}.{phase}"
            print(f"  {label:30s} {g['total_ms']:>10.1f} {g['cnt']:>6} {avg_ms:>8.2f} {g['max_ms']:>8.2f} {g['total_burst']:>5} {g['windows']:>7}")
        print(f"  {'-'*76}")

    print(f"\n{'='*80}")
    print(f"  🏆 Top {top_n} 最耗时阶段 (按 total_ms 排序)")
    print(f"{'='*80}")
    print(f"  {'#':>3} {'阶段':35s} {'total(ms)':>10} {'cnt':>6} {'avg(ms)':>8}")
    print(f"  {'-'*62}")
    for i, (key, g) in enumerate(sorted_items[:top_n]):
        cat, func, phase = key
        avg_ms = g['total_ms'] / g['windows'] if g['windows'] > 0 else 0
        label = f"{cat}.{func}.{phase}"
        print(f"  {i+1:>3} {label:35s} {g['total_ms']:>10.1f} {g['cnt']:>6} {avg_ms:>8.2f}")

    print(f"\n  📈 统计概要:")
    print(f"    总事件数: {len(events)}")
    print(f"    总日志行数: 已自动过滤")
    total_all = sum(g['total_ms'] for _, g in sorted_items)
    print(f"    所有阶段总耗时: {total_all:.1f}ms")
    print(f"    总 burst 次数: {sum(g['total_burst'] for _, g in sorted_items)}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Harmony SSCC 性能埋点分析')
    parser.add_argument('path', nargs='?', default=None,
                        help='日志文件或目录（默认：自动从 ../../.env 定位）')
    parser.add_argument('--top', type=int, default=15, help='Top N (默认 15)')
    parser.add_argument('--cat', help='只显示指定模块')
    parser.add_argument('--raw', action='store_true', help='原始事件明细')
    args = parser.parse_args()

    # 定位日志源
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if args.path:
        log_files = collect_log_files(args.path)
    else:
        log_dir = find_log_dir(script_dir)
        log_files = collect_log_files(log_dir)
        print(f"📁 自动定位日志目录: {log_dir}", file=sys.stderr)

    if not log_files:
        print("❌ 未找到 ssc-validator-*.log 文件")
        sys.exit(1)

    print(f"📋 扫描 {len(log_files)} 个日志文件...", file=sys.stderr)
    events, total_lines, perf_count = extract_perf_events(log_files)
    print(f"📋 总计: {total_lines} 行日志, {perf_count} 个 [perf] 事件")

    if args.raw:
        for ev in events:
            print(f"  {ev['cat']:20s} {ev['func']:35s} {ev['phase']:25s} "
                  f"total={ev['total_ms']:>8.2f}ms  cnt={ev['cnt']:>4}  "
                  f"avg={ev['avg_ms']:>8.2f}ms  max={ev['max_ms']:>8.2f}ms")
        return

    print_summary(events, top_n=args.top, cat_filter=args.cat)


if __name__ == '__main__':
    main()
