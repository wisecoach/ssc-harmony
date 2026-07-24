#!/usr/bin/env python3
"""
按交易类型统计 SSC 日志耗时。

用法：
  python3 analyze-tx-type-timing.py          # 自动选最新日志目录
  python3 analyze-tx-type-timing.py <路径>    # 指定目录

统计目标：
  - VerifySimulation timing breakdown（含总耗时 + 子阶段）
  - CommitSimulation 调用次数
  - SSC tx commit timing（按 txType 区分 CR/SimTx/SL/NewEpoch/normal）
  - BatchVerifySimulations 调用次数与批次大小
"""

import json
import os
import sys
from collections import defaultdict


def find_latest_log_dir(base=None):
    if base is None:
        base = os.path.dirname(os.path.abspath(__file__))
    # 先尝试读 .env 找当前配置的目录
    env_file = os.path.join(base, "../../.env")
    if os.path.exists(env_file):
        env = {}
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
        parts = [
            f"shard={env.get('SHARD_NUM', '*')}",
            f"validator={env.get('VALIDATOR', '*')}",
            f"ssc={env.get('SSC', '*')}",
            f"delay={env.get('DELAY', '*')}",
            f"rate={env.get('RATE', '*')}",
            f"vpn={env.get('VALIDATOR_PER_NODE', '*')}",
        ]
        pattern = "_".join(parts)
        dirs = [d for d in os.listdir(base)
                if d.startswith("shard=") and os.path.isdir(os.path.join(base, d))]
        # 优先匹配 .env 配置的目录
        for d in sorted(dirs, reverse=True):
            # 用 .env 中的值匹配（支持通配）
            if all(f"{k}={v}" in d for k, v in [
                ("shard", env.get("SHARD_NUM")),
                ("validator", env.get("VALIDATOR")),
                ("ssc", env.get("SSC")),
                ("delay", env.get("DELAY")),
                ("rate", env.get("RATE")),
                ("vpn", env.get("VALIDATOR_PER_NODE")),
            ] if v):
                return os.path.join(base, d)
        # 回退：取最新的
        if dirs:
            dirs.sort()
            return os.path.join(base, dirs[-1])
    else:
        dirs = [d for d in os.listdir(base)
                if d.startswith("shard=") and os.path.isdir(os.path.join(base, d))]
        if dirs:
            dirs.sort()
            return os.path.join(base, dirs[-1])
    return None


def parse_duration(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        # VerifySimulation timing breakdown 使用 float 秒
        if s < 1000:  # 秒
            return s * 1000
        return float(s)
    if not isinstance(s, str):
        return None
    try:
        if s.endswith("ms"):
            return float(s[:-2])
        elif "us" in s or "\u00b5s" in s:
            return float(s.replace("\u00b5s", "").replace("us", "")) / 1000
        elif s.endswith("s"):
            return float(s[:-1]) * 1000
        elif s.endswith("m"):
            return float(s[:-1]) * 60000
        else:
            return float(s)
    except (ValueError, TypeError):
        return None


def collect_files(log_dir):
    files = []
    for f in os.listdir(log_dir):
        if (f.startswith("ssc-validator-") or f.startswith("log-") or f.startswith("zerolog-validator-")) \
                and f.endswith(".log"):
            files.append(os.path.join(log_dir, f))
    return files


def print_stat(label, vals):
    n = len(vals)
    if n == 0:
        return
    sv = sorted(vals)
    avg = sum(vals) / n
    p50 = sv[n // 2]
    p90 = sv[int(n * 0.9)] if int(n * 0.9) < n else sv[-1]
    p99 = sv[int(n * 0.99)] if int(n * 0.99) < n else sv[-1]
    print(f"  {label:30s}  n={n:>5}  avg={avg:>8.2f}  P50={p50:>8.2f}  P90={p90:>8.2f}  P99={p99:>8.2f}")


def main():
    if len(sys.argv) > 1:
        log_dir = sys.argv[1]
    else:
        base = os.path.dirname(os.path.abspath(__file__))
        log_dir = find_latest_log_dir(base)
        if not log_dir:
            print("ERROR: no shard=* directory found, specify path")
            sys.exit(1)

    files = collect_files(log_dir)
    if not files:
        print(f"ERROR: no log files in {log_dir}")
        sys.exit(1)

    print(f"dir: {os.path.basename(log_dir)}")
    print(f"files: {len(files)}")

    markers = defaultdict(int)
    # 按 txType 分组的 SSC_tx_commit 耗时
    tx_type_timings = defaultdict(list)
    # VerifySimulation 子阶段
    vs_timings = defaultdict(list)
    batch_phases = []
    verify_sim_cost = []  # verify simulation cost

    for fp in files:
        with open(fp, "r", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or not line.startswith("{"):
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                msg = obj.get("message", "")

                # ---------- VerifySimulation timing breakdown ----------
                if "VerifySimulation timing breakdown" in msg:
                    markers["VerifySimulation"] += 1
                    for key in ["total", "lockCheck", "execVerify", "lockState", "chainPatch"]:
                        v = obj.get(key)
                        if v:
                            ms = parse_duration(v)
                            if ms is not None:
                                vs_timings[key].append(ms)

                # ---------- verify simulation cost (precompile) ----------
                elif "verify simulation" in msg and "cost" in obj:
                    markers["verify_sim_precompile"] += 1
                    ms = parse_duration(obj.get("cost"))
                    if ms is not None:
                        verify_sim_cost.append(ms)

                # ---------- SSC tx commit timing (按 txType 分组) ----------
                elif "SSC tx commit timing" in msg:
                    markers["SSC_tx_commit"] += 1
                    tx_type = obj.get("txType", "unknown")
                    d = obj.get("duration", "0")
                    ms = parse_duration(d)
                    if ms is not None:
                        tx_type_timings[tx_type].append(ms)

                # ---------- CommitSimulation ----------
                elif "CommitSimulation: called" in msg:
                    markers["CommitSimulation"] += 1

                # ---------- batch 模式标记 ----------
                elif "skip single VerifySimulation" in msg:
                    markers["batch_skip_verify"] += 1

                # ---------- BatchVerifySimulations ----------
                elif "BatchVerifySimulations: start" in msg:
                    markers["Batch_start"] += 1
                    bs = obj.get("batchSize")
                    if bs is not None:
                        vs_timings["batch_size"].append(bs)

                elif "BatchVerifySimulations: processing batch" in msg:
                    markers["Batch_process"] += 1
                    bs = obj.get("batchSize")
                    if bs is not None:
                        vs_timings["batch_process_size"].append(bs)

                elif "BatchVerifySimulations: done" in msg:
                    markers["Batch_done"] += 1

                elif "BatchVerify: passed phase done" in msg:
                    markers["Batch_passed"] += 1
                    batch_phases.append({
                        "passed": obj.get("passed"),
                        "callStates": obj.get("callStates"),
                        "conflict": obj.get("conflict"),
                        "execFail": obj.get("execFail"),
                        "lockCheck": obj.get("lockCheck", ""),
                        "total": obj.get("total", ""),
                    })

    # ===== 输出 =====
    print()
    print("===== Marker 计数 =====")
    for k in sorted(markers.keys()):
        print(f"  {k:30s}  {markers[k]:>8}")

    print()
    print("===== VerifySimulation 耗时 (ms) =====")
    for k in ["total", "lockCheck", "execVerify", "lockState", "chainPatch"]:
        if vs_timings.get(k):
            print_stat("VS_" + k, vs_timings[k])

    if verify_sim_cost:
        print()
        print("===== precompile verify simulation cost (ms) =====")
        print_stat("verify_sim_cost", verify_sim_cost)

    if tx_type_timings:
        print()
        print("===== SSC tx commit 按类型统计 (ms) =====")
        for ttype in ["CR", "SimTx", "SL", "NewEpoch", "normal", "otherSSC", "unknown"]:
            if tx_type_timings.get(ttype):
                print_stat("SSC_" + ttype, tx_type_timings[ttype])

    if batch_phases:
        print()
        print("===== BatchVerify 详情 =====")
        for i, e in enumerate(batch_phases):
            lc = e.get("lockCheck", "")
            tt = e.get("total", "")
            print(f"  [{i:2d}] passed={e['passed']:>3}  callStates={e['callStates']:>3}  "
                  f"conflict={e['conflict']:>2}  execFail={e['execFail']:>2}  "
                  f"lockCheck={lc}  total={tt}")

    if vs_timings.get("batch_size"):
        print()
        print("===== Batch 大小 =====")
        print_stat("batch_size", vs_timings["batch_size"])

    if markers.get("batch_skip_verify", 0) > 0:
        print("\n  => Batch 并行模式已启用")
    else:
        print("\n  => Batch 并行模式未启用")


if __name__ == "__main__":
    main()
