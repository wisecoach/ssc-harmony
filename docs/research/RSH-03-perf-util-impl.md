# perfLog 工具函数 — 统一性能埋点

## 用法

```go
import "github.com/wisecoach/harmony-sscc/ssc/utils"

// 在函数开头 defer
defer utils.PerfLog("retryScheduler", "OnBlockCommitted", "staleCleanup", time.Now(),
    utils.Int("staleN", staleNum),
)()
```

## 实现建议

```go
// utils/perf.go
package utils

import (
    "time"
    "github.com/rs/zerolog"
)

// PerfLog 返回一个 func()，在 defer 中调用以记录耗时
// 用法：defer PerfLog(cat, fn, phase, time.Now()).()
// 或：defer PerfLog(cat, fn, phase, time.Now(), Int("key", val)).()
func PerfLog(cat, fn, phase string, t0 time.Time, extra ...func(*zerolog.Event)) func() {
    return func() {
        evt := SSCLogger().Debug().
            Str("cat", cat).
            Str("func", fn).
            Str("phase", phase).
            Dur("dur", time.Since(t0))
        for _, f := range extra {
            f(evt)
        }
        evt.Msg("[perf] " + cat + " perf")
    }
}

func Int(key string, val int) func(*zerolog.Event) {
    return func(e *zerolog.Event) { e.Int(key, val) }
}

func Str(key, val string) func(*zerolog.Event) {
    return func(e *zerolog.Event) { e.Str(key, val) }
}
```

## 调用示例

```go
// 简单
func (rs *retryScheduler) OnBlockCommitted(block *types.Block) {
    defer utils.PerfLog("retryScheduler", "OnBlockCommitted", "staleCleanup", time.Now(),
        utils.Int("staleN", staleNum),
    )()
    // ...
}

// 多阶段
func (rs *retryScheduler) OnBlockCommitted(block *types.Block) {
    // stale 清理
    func() {
        defer utils.PerfLog("retryScheduler", "OnBlockCommitted", "staleCleanup", time.Now())()
        // stale 操作...
    }()
    
    // querySubscribers
    func() {
        defer utils.PerfLog("retryScheduler", "OnBlockCommitted", "querySubscribers", time.Now())()
        candidates := rs.querySubscribers(releasedKeys)
    }()
    
    // sort
    func() {
        defer utils.PerfLog("retryScheduler", "OnBlockCommitted", "sortCandidates", time.Now(),
            utils.Int("candidatesN", len(candidates)),
        )()
        sort.SliceStable(sorted, ...)
    }()
}
```
