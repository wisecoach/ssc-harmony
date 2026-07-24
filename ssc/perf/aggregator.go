package perf

import (
	"sync"
	"time"

	"github.com/harmony-one/harmony/core/types"
	"github.com/harmony-one/harmony/internal/utils"
)

// perfKey 唯一标识一个阶段
type perfKey struct {
	Cat   string // 模块分类：retryScheduler / verify / simLeader / ...
	Func  string // 外层函数名
	Phase string // 阶段名
}

// PerfWindow 一个区块窗口内的聚合数据
type PerfWindow struct {
	mu       sync.Mutex
	total    time.Duration // 窗口内总耗时
	count    int           // 调用次数
	max      time.Duration // 单次最大耗时
	burstCnt int           // 超过 BurstThreshold 的次数
}

// PerfAggregator 性能聚合器
// Record() 记录一次耗时，OnBlockCommitted() 输出并重置窗口
type PerfAggregator struct {
	mu             sync.Mutex
	active         map[perfKey]*PerfWindow
	BurstThreshold time.Duration // 超过此值的调用记为 burst（默认 100ms）
	WarmupDuration time.Duration // 启动预热期（默认 10s）
	startTime      time.Time
}

// NewPerfAggregator 创建性能聚合器
func NewPerfAggregator() *PerfAggregator {
	return &PerfAggregator{
		active:         make(map[perfKey]*PerfWindow),
		BurstThreshold: 100 * time.Millisecond,
		WarmupDuration: 10 * time.Second,
		startTime:      time.Now(),
	}
}

// Record 记录一次性能事件
// cat: 模块分类（如 "retryScheduler"）
// funcName: 外层函数名（如 "OnBlockCommitted"）
// phase: 阶段名（如 "staleCleanup"）
// dur: 耗时
func (pa *PerfAggregator) Record(cat, funcName, phase string, dur time.Duration) {
	key := perfKey{cat, funcName, phase}

	pa.mu.Lock()
	w, ok := pa.active[key]
	if !ok {
		w = &PerfWindow{}
		pa.active[key] = w
	}
	pa.mu.Unlock()

	w.mu.Lock()
	w.total += dur
	w.count++
	if dur > w.max {
		w.max = dur
	}
	if dur > pa.BurstThreshold {
		w.burstCnt++
	}
	w.mu.Unlock()
}

// IsWarmup 返回当前是否处于启动预热期
func (pa *PerfAggregator) IsWarmup() bool {
	return time.Since(pa.startTime) < pa.WarmupDuration
}

// OnBlockCommitted 输出并重置所有窗口
// 应在所有模块的 Record() 调用完成后再调
func (pa *PerfAggregator) OnBlockCommitted(block *types.Block) {
	if !Enabled {
		return
	}

	pa.mu.Lock()
	oldActive := pa.active
	pa.active = make(map[perfKey]*PerfWindow)
	pa.mu.Unlock()

	isWarmup := pa.IsWarmup()

	for key, w := range oldActive {
		w.mu.Lock()
		count := w.count
		total := w.total
		max := w.max
		burstCnt := w.burstCnt
		w.mu.Unlock()

		if count == 0 || total == 0 {
			continue
		}

		avg := time.Duration(int64(total) / int64(count))

		utils.SSCLogger().Info().
			Str("cat", key.Cat).
			Str("func", key.Func).
			Str("phase", key.Phase).
			Dur("total", total).
			Int("cnt", count).
			Dur("avg", avg).
			Dur("max", max).
			Int("burstCnt", burstCnt).
			Bool("warmup", isWarmup).
			Msg("[perf] perf window")
	}
}

// ─── 包级全局默认聚合器 ─────────────────────────────────────
// 各模块直接调 perf.Record() 即可，不需要传指针

var defaultAggregator = NewPerfAggregator()

// Enabled 控制 perf 是否工作。默认关闭，由启动时设置。
var Enabled = true

// RecordPkg 使用默认全局聚合器记录性能事件
func RecordPkg(cat, funcName, phase string, dur time.Duration) {
	if !Enabled {
		return
	}
	defaultAggregator.Record(cat, funcName, phase, dur)
}

// GetAggregator 返回全局聚合器，供 impl.go 注入到 BlockCommitted
func GetAggregator() *PerfAggregator {
	return defaultAggregator
}
