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

# 等待 30 秒让交易稳定发送，然后对 4 个工作节点并发抓取 pprof CPU profile
echo "===== 等待 30 秒后开始 pprof 采样（60 秒）====="
sleep 30
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
            echo "  10.7.95.$NODE:$PORT: 开始 CPU profile 采样..."
            curl -s --max-time 65 "http://10.7.95.$NODE:$PORT/debug/pprof/profile?seconds=60" \
                -o "$PPROF_DIR/cpu_profile_${NODE}_${PORT}.pprof" 2>/dev/null
            if [ $? -eq 0 ] && [ -s "$PPROF_DIR/cpu_profile_${NODE}_${PORT}.pprof" ]; then
                echo "  ✅ 10.7.95.$NODE:$PORT: 采样完成 ($(stat -c%s "$PPROF_DIR/cpu_profile_${NODE}_${PORT}.pprof") bytes)"
            else
                echo "  ❌ 10.7.95.$NODE:$PORT: 采样失败"
            fi
        ) &
    done
done

# 等待所有采样完成
wait
echo "===== pprof 采样完成，结果在 $PPROF_DIR ====="

# 在后台启动 pprof HTTP 服务，每个文件监听不同端口，供本地浏览器访问
echo "===== 启动 pprof HTTP 服务 ====="
PORT_BASE=12300
INDEX=0
for f in "$PPROF_DIR"/cpu_profile_*.pprof; do
    if [ -s "$f" ]; then
        PORT=$((PORT_BASE + INDEX))
        nohup go tool pprof -http="0.0.0.0:$PORT" "$f" > /dev/null 2>&1 &
        # 提取简短文件名显示
        fname=$(basename "$f" .pprof)
        echo "  🔗 $fname → http://10.7.95.199:$PORT"
        INDEX=$((INDEX + 1))
    fi
done
echo "===== pprof HTTP 服务已启动（端口 $PORT_BASE-$(($PORT_BASE + INDEX - 1))），实验结束后手动关闭 ====="

# 等待远程客户端脚本结束
wait $REMOTE_PID
cd -
