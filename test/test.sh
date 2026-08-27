#!/bin/bash

# 清理上次实验残留的 pprof viewer 进程，确保端口可用
pkill -f "go tool pprof -http=" 2>/dev/null || true

cd ../ssc-cli/cli
node deploySSCCTest.js
cd -
cd ../ssc-cli/cli-py

# 后台启动远程客户端（SSH 异步启动客户端 + 轮询等待 + 收集结果）
./simulate_sscc_remote.sh &
REMOTE_PID=$!

# 滞后采样：等待 80 秒让模拟队列积压（rate=200 时 simulatingCount 峰值在 ~80-100s），
# 然后对 4 个工作节点并发抓取 pprof CPU profile（采样窗口 80s-140s，覆盖吞吐/队列瓶颈现场）
echo "===== 等待 80 秒后开始 pprof 采样（60 秒，窗口 80-140s）====="
sleep 80
echo "===== 开始 pprof 采样 ====="

PPROF_DIR="/tmp/pprof_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$PPROF_DIR"

# 每个节点的 4 个 harmony 进程 HTTP 端口（P2P port - 2500）
# 200: 6500/6502/6504/6506
# 201: 6540/6542/6544/6546
# 202: 6580/6582/6584/6586
# 203: 6620/6622/6624/6626
PORTS_200="6500 6502 6504 6506"
PORTS_201="6540 6542 6544 6546"
PORTS_202="6580 6582 6584 6586"
PORTS_203="6620 6622 6624 6626"

for NODE in 200 201 202 203; do
    PORTS_VAR="PORTS_$NODE"
    for PORT in ${!PORTS_VAR}; do
        (
            # 4 类采样完全并行：CPU 60s 与 mutex/block 20s、goroutine 快照同时启动
            (
              curl -s --max-time 65 "http://10.7.95.$NODE:$PORT/debug/pprof/profile?seconds=60" \
                -o "$PPROF_DIR/cpu_profile_${NODE}_${PORT}.pprof" 2>/dev/null
              [ -s "$PPROF_DIR/cpu_profile_${NODE}_${PORT}.pprof" ] && \
                echo "  ✅ 10.7.95.$NODE:$PORT: CPU完成" || \
                echo "  ❌ 10.7.95.$NODE:$PORT: CPU失败"
            ) &
            (
              curl -s --max-time 65 "http://10.7.95.$NODE:$PORT/debug/pprof/mutex?seconds=60" \
                -o "$PPROF_DIR/mutex_profile_${NODE}_${PORT}.pprof" 2>/dev/null
              [ -s "$PPROF_DIR/mutex_profile_${NODE}_${PORT}.pprof" ] && \
                echo "  ✅ 10.7.95.$NODE:$PORT: mutex完成" || \
                echo "  ⚪ 10.7.95.$NODE:$PORT: mutex空(禁/无竞争)"
            ) &
            (
              curl -s --max-time 65 "http://10.7.95.$NODE:$PORT/debug/pprof/block?seconds=60" \
                -o "$PPROF_DIR/block_profile_${NODE}_${PORT}.pprof" 2>/dev/null
              [ -s "$PPROF_DIR/block_profile_${NODE}_${PORT}.pprof" ] && \
                echo "  ✅ 10.7.95.$NODE:$PORT: block完成" || \
                echo "  ⚪ 10.7.95.$NODE:$PORT: block空"
            ) &
            (
              curl -s "http://10.7.95.$NODE:$PORT/debug/pprof/goroutine?debug=1" \
                -o "$PPROF_DIR/goroutine_${NODE}_${PORT}.txt" 2>/dev/null
              [ -s "$PPROF_DIR/goroutine_${NODE}_${PORT}.txt" ] && \
                echo "  ✅ 10.7.95.$NODE:$PORT: goroutine完成" || \
                echo "  ⚪ 10.7.95.$NODE:$PORT: goroutine空"
            ) &
            wait   # 等本进程的 4 类采样（并行，最长 ~60s）
        ) &
    done
done

# 等待所有采样完成
wait
echo "===== pprof 采样完成，结果在 $PPROF_DIR ====="

# 在后台启动 pprof HTTP 服务（CPU/mutex/block 分三段端口），供本地浏览器查看
echo "===== 启动 pprof HTTP 服务 ====="
start_viewer() {
    local glob=$1 port_base=$2 label=$3
    local idx=0
    for f in "$PPROF_DIR"/$glob; do
        if [ -s "$f" ]; then
            local PORT=$((port_base + idx))
            nohup go tool pprof -http="0.0.0.0:$PORT" "$f" > /dev/null 2>&1 &
            fname=$(basename "$f" .pprof)
            echo "  🔗 [$label] $fname → http://10.7.95.199:$PORT"
            idx=$((idx + 1))
        fi
    done
}
# CPU: 12300 段；mutex: 12400 段；block: 12500 段
start_viewer "cpu_profile_*.pprof"   12300 "CPU"
start_viewer "mutex_profile_*.pprof" 12400 "MUTEX"
start_viewer "block_profile_*.pprof" 12500 "BLOCK"
echo "===== pprof HTTP 服务已启动（CPU 12300+/MUTEX 12400+/BLOCK 12500+），实验结束后手动关闭 ====="

# 等待远程客户端脚本结束
wait $REMOTE_PID
cd -
