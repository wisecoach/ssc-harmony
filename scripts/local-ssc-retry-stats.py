#!/usr/bin/env python3
"""
本地一键向远程 199 请求 SSC 重试/DAG/超时统计。

流程：
  1. scp 远程聚合脚本 scripts/remote_ssc_retry_stats.py 到 199 的 logs 目录
  2. SSH 到 199，在目标实验日志目录上运行聚合脚本
  3. 本地打印 CHAIN_RETRY_STATS / STATS DUMP / 超时 / 回滚 reason 汇总

用法：
  python3 scripts/local-ssc-retry-stats.py                 # auto 最新实验目录
  python3 scripts/local-ssc-retry-stats.py --rate 200      # 按 RATE 找目录
  python3 scripts/local-ssc-retry-stats.py --dir '<logdir>'# 直接指定远程日志目录名

依赖：本机有 ssh/scp 免密连 zjnu@10.7.95.199 -p 10022。
"""
import os, sys, subprocess, argparse

SSH_HOST = "zjnu@10.7.95.199"
SSH_KEY = os.path.expanduser("~/.ssh/id_ed25519")  # 199 授权的 key
SSH_PORT = "10022"
REMOTE_BASE = "/home/zjnu/go/src/github.com/harmony-one/logs/harmony-sscc"
LOCAL_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "remote_ssc_retry_stats.py")
REMOTE_SCRIPT = REMOTE_BASE + "/remote_ssc_retry_stats.py"


def ssh(cmd, timeout=600):
    # -F /dev/null: 避开系统 /etc/ssh/ssh_config.d/20-systemd-ssh-proxy.conf 权限报错
    # -i id_ed25519: 199 只授权这把 key（id_rsa_new 会 Permission denied）
    cmd = (f'ssh -F /dev/null -i {SSH_KEY} -p {SSH_PORT} -o ConnectTimeout=15 '
           f'-o BatchMode=yes -o StrictHostKeyChecking=no -o IdentitiesOnly=yes '
           f'{SSH_HOST} "{cmd}"')
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    return r.returncode, r.stdout, r.stderr


def scp_push(local_file, remote_path):
    cmd = (f'scp -F /dev/null -i {SSH_KEY} -P {SSH_PORT} -o ConnectTimeout=15 '
           f'-o StrictHostKeyChecking=no -o IdentitiesOnly=yes '
           f'{local_file} {SSH_HOST}:{remote_path}')
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
    return r.returncode, r.stdout, r.stderr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rate', help='按 RATE 选日志目录（如 200）')
    ap.add_argument('--dir', help='直接指定远程日志目录名')
    args = ap.parse_args()

    if args.dir:
        logdir = f"{REMOTE_BASE}/{args.dir}"
        if not logdir.startswith(REMOTE_BASE + "/"):
            logdir = f"{REMOTE_BASE}/{args.dir}"
    elif args.rate:
        # 找该 RATE 下最新目录
        rc, out, err = ssh(
            f"ls -dt {REMOTE_BASE}/shard=*_rate={args.rate}_vpn=*/ 2>/dev/null | head -1")
        logdir = out.strip()
        if not logdir:
            print(f"✗ 远程没有 rate={args.rate} 的日志目录")
            sys.exit(1)
    else:
        # auto：最新目录
        rc, out, err = ssh(f"ls -dt {REMOTE_BASE}/shard=*/ 2>/dev/null | head -1")
        logdir = out.strip()
        if not logdir:
            print("✗ 远程没有实验日志目录")
            sys.exit(1)

    print(f"▶ 目标日志目录: {logdir}")

    # ① 上传聚合脚本
    rc, out, err = scp_push(LOCAL_SCRIPT, REMOTE_SCRIPT)
    if rc != 0:
        print(f"✗ scp 上传失败: {err[-300:]}")
        sys.exit(1)
    print("  ✅ 已上传 remote_ssc_retry_stats.py")

    # ② 远程运行
    print("▶ 在 199 上聚合统计 ...")
    rc, out, err = ssh(f"python3 {REMOTE_SCRIPT} '{logdir}' 2>&1")
    print(out)
    if rc != 0:
        print(f"✗ 远程聚合失败 rc={rc}")
        print(err[-500:])
        sys.exit(1)


if __name__ == '__main__':
    main()
