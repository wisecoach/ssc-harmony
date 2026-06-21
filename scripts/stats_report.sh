#!/bin/bash
# stats_report.sh — 解析 SSC 实验的 STATS DUMP 日志
# 用法: cd <实验日志目录> && ./stats_report.sh

# 找到所有 ssc-validator 日志中最后一次 STATS DUMP
last_dump=$(grep -h 'STATS DUMP' ssc-validator-*.log 2>/dev/null | tail -1)

if [ -z "$last_dump" ]; then
    echo "未找到 STATS DUMP 日志，请确认："
    echo "  1. 代码已同步到远程（含 statistics.go）"
    echo "  2. 实验已运行完毕"
    exit 1
fi

echo "=== SSC 实验统计报告 ==="
echo ""
echo "--- 队列 ---"
echo "$last_dump" | python3 -c "
import sys,json
for l in sys.stdin:
 d=json.loads(l.strip())
 qwait_ms = d['queueAvgWait'] / 1e6 if 'queueAvgWait' in d else 0
 print(f'  Push:     {d.get(\"queuePush\",0):>8}')
 print(f'  Pop:      {d.get(\"queuePop\",0):>8}')
 print(f'  当前长度: {d.get(\"queueLen\",0):>8}')
 print(f'  空等次数: {d.get(\"queueEmptyPop\",0):>8}')
 print(f'  平均等待: {qwait_ms:>8.1f} ms')
 print(f'  等待计数: {d.get(\"queueWaitCount\",0):>8}')
 print()
 print('--- Worker ---')
 sim_avg = d.get('simAvgTime',0)/1e6
 p2p_avg = d.get('p2pAvgTime',0)/1e6
 print(f'  模拟次数: {d.get(\"simCount\",0):>8}')
 print(f'  模拟平均: {sim_avg:>8.1f} ms')
 print(f'  P2P 次数: {d.get(\"p2pCallCount\",0):>8}')
 print(f'  P2P 平均: {p2p_avg:>8.1f} ms')
 print(f'  成功:     {d.get(\"simSuccess\",0):>8}')
 print(f'  失败:     {d.get(\"simFail\",0):>8}')
 print()
 print('--- 锁系统 ---')
 print(f'  Lockable Base冲突:  {d.get(\"lockableFailBase\",0):>8}')
 print(f'  Lockable Pending冲: {d.get(\"lockableFailPending\",0):>8}')
 print(f'  Lockable RLock阻:   {d.get(\"lockableFailRlock\",0):>8}')
 print(f'  Snapshot 命中:      {d.get(\"snapshotHit\",0):>8}')
 print(f'  Snapshot 缺失:      {d.get(\"snapshotMiss\",0):>8}')
 print(f'  TempLock 尝试:      {d.get(\"tempLockTryTotal\",0):>8}')
 print(f'  TempLock 失败:      {d.get(\"tempLockTryFail\",0):>8}')
 fail_pct = 0
 if d.get('tempLockTryTotal',0) > 0:
  fail_pct = d['tempLockTryFail']*100//d['tempLockTryTotal']
 print(f'  TempLock 失败率:    {fail_pct:>7}%')
 print(f'  pendingUnlock 总量: {d.get(\"pendingUnlockTotal\",0):>8}')
 print(f'  pendingUnlock 批次: {d.get(\"pendingUnlockBatch\",0):>8}')
 print()
 print('--- 定时器 ---')
 print(f'  Sp1 启动:  {d.get(\"sp1Started\",0):>8}')
 print(f'  Sp1 触发:  {d.get(\"sp1Fired\",0):>8}')
 print(f'  Sp1 取消:  {d.get(\"sp1Removed\",0):>8}')
 print(f'  Pool 启动: {d.get(\"poolStarted\",0):>8}')
 print(f'  Pool 触发: {d.get(\"poolFired\",0):>8}')
 print(f'  Pool 取消: {d.get(\"poolRemoved\",0):>8}')
 # 关键比率
 if d.get('sp1Started',0) > 0:
  cancel_rate = d['sp1Removed']*100//d['sp1Started']
  print(f'  Sp1 取消率:           {cancel_rate:>7}%')
 print()
 print('--- 重试 ---')
 retry_ok = d.get('retryReady',0)
 retry_no = d.get('retryNotReady',0)
 retry_total = retry_ok + retry_no
 print(f'  retryPool 添加: {d.get(\"retryAdd\",0):>8}')
 print(f'  Ready 信号:     {retry_ok:>8}')
 print(f'  NotReady 信号:  {retry_no:>8}')
 if retry_total > 0:
  ready_pct = retry_ok*100//retry_total
  print(f'  Ready 率:       {ready_pct:>7}%')
 print(f'  重试成功: {d.get(\"retrySuccess\",0):>8}')
 print(f'  重试失败: {d.get(\"retryFail\",0):>8}')
 print()
 print('--- 耗时 ---')
 elapsed_s = d.get('elapsed',0) / 1e9
 print(f'  实验耗时: {elapsed_s:.0f} 秒 ({elapsed_s/60:.1f} 分钟)')
"
