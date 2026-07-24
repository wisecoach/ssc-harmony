#!/bin/bash
# CPU 监控脚本（使用 mpstat 取瞬时采样，周期 2 秒，120 秒后自动退出）
# 依赖: sysstat（mpstat）
# 输出格式: time,user,system,iowait,idle
#
# 与旧 top -bn1 版本的区别：
# - top -bn1 第一次输出是开机以来的累积平均，不能反映实验期间的真实 CPU 占用
# - mpstat 每次输出都是采样间隔内的瞬时值

MPSTAT=$(which mpstat 2>/dev/null)
if [ -z "$MPSTAT" ]; then
    echo "ERROR: mpstat not found. Install sysstat package."
    exit 1
fi

OUTFILE="/tmp/cpu_usage_$(date +%Y%m%d_%H%M%S).log"
echo "time,user,system,iowait,idle" > "$OUTFILE"
END=$((SECONDS + 120))

while [ $SECONDS -lt $END ]; do
    # mpstat 1 1: 采样 1 秒，输出 1 次瞬时值
    $MPSTAT 1 1 | tail -1 | awk \
        -v t="$(date +%H:%M:%S)" \
        '{print t","$3","$5","$6","$12}' \
        >> "$OUTFILE"
done

echo "DONE" >> "$OUTFILE"
