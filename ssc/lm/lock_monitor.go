package lm

import (
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/harmony-one/harmony/internal/utils"
)

var MonitorEnabled = true
var TimeoutThreshold = 5 * time.Second

func EnableMonitor() {
	MonitorEnabled = true
}

func DisableMonitor() {
	MonitorEnabled = false
}

func SetTimeout(duration time.Duration) {
	TimeoutThreshold = duration
}

func NewMutex() Mutex {
	return Mutex{
		mu: sync.Mutex{},
	}
}

func NewRWMutex() RWMutex {
	return RWMutex{
		rw: sync.RWMutex{},
	}
}

// Mutex 带监控的互斥锁
type Mutex struct {
	mu          sync.Mutex
	holderGID   uint64
	holderStack []byte
	holderTime  time.Time
}

func (m *Mutex) Lock() {
	if !MonitorEnabled {
		m.mu.Lock()
		return
	}

	currentGID := getGoroutineID()
	currentStack := debug.Stack()

	done := make(chan struct{})
	go func() {
		m.mu.Lock()
		// 记录锁持有者信息
		m.holderGID = currentGID
		m.holderStack = currentStack
		m.holderTime = time.Now()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(TimeoutThreshold):
		// 获取当前持有锁的goroutine信息
		holderGID := m.holderGID
		holderStack := m.holderStack
		holderTime := m.holderTime

		utils.SSCLogger().Error().Msgf("\n\n⚠️ MUTEX LOCK TIMEOUT DETECTED ⚠️\n"+
			"Waited longer than %s\n"+
			"Requesting goroutine ID: %d\n"+
			"Requesting goroutine stack:\n%s\n"+
			"Lock holder goroutine ID: %d\n"+
			"Lock holder stack:\n%s\n"+
			"Lock held since: %s\n\n",
			TimeoutThreshold,
			currentGID, string(currentStack),
			holderGID, string(holderStack),
			time.Since(holderTime))
		<-done // 仍然获取锁，但已记录问题
	}
}

func (m *Mutex) Unlock() {
	m.holderGID = 0
	m.holderStack = nil
	m.mu.Unlock()
}

// RWMutex 带监控的读写锁
type RWMutex struct {
	rw           sync.RWMutex
	holderGID    uint64
	holderStack  []byte
	holderTime   time.Time
	readers      map[uint64][]byte // 记录所有读锁持有者
	readersMutex sync.Mutex
}

func (rw *RWMutex) Lock() {
	if !MonitorEnabled {
		rw.rw.Lock()
		return
	}

	currentGID := getGoroutineID()
	currentStack := debug.Stack()

	done := make(chan struct{})
	go func() {
		rw.rw.Lock()
		// 记录写锁持有者信息
		rw.holderGID = currentGID
		rw.holderStack = currentStack
		rw.holderTime = time.Now()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(TimeoutThreshold):
		// 获取当前持有锁的goroutine信息
		holderGID := rw.holderGID
		holderStack := rw.holderStack
		holderTime := rw.holderTime

		utils.SSCLogger().Error().Msgf("\n\n⚠️ RW MUTEX LOCK TIMEOUT DETECTED ⚠️\n"+
			"Waited longer than %s\n"+
			"Requesting goroutine ID: %d\n"+
			"Requesting goroutine stack:\n%s\n"+
			"Lock holder goroutine ID: %d\n"+
			"Lock holder stack:\n%s\n"+
			"Lock held since: %s\n\n",
			TimeoutThreshold,
			currentGID, string(currentStack),
			holderGID, string(holderStack),
			time.Since(holderTime))
		<-done
	}
}

func (rw *RWMutex) Unlock() {
	rw.holderGID = 0
	rw.holderStack = nil
	rw.rw.Unlock()
}

func (rw *RWMutex) RLock() {
	if !MonitorEnabled {
		rw.rw.RLock()
		return
	}

	currentGID := getGoroutineID()
	currentStack := debug.Stack()

	done := make(chan struct{})
	go func() {
		rw.rw.RLock()
		// 记录读锁持有者信息
		rw.readersMutex.Lock()
		if rw.readers == nil {
			rw.readers = make(map[uint64][]byte)
		}
		rw.readers[currentGID] = currentStack
		rw.readersMutex.Unlock()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(TimeoutThreshold):
		// 获取当前持有锁的goroutine信息
		rw.readersMutex.Lock()
		readers := make(map[uint64][]byte)
		for gid, stack := range rw.readers {
			readers[gid] = stack
		}
		rw.readersMutex.Unlock()

		utils.SSCLogger().Error().Msgf("\n\n⚠️ RW MUTEX RLOCK TIMEOUT DETECTED ⚠️\n"+
			"Waited longer than %s\n"+
			"Requesting goroutine ID: %d\n"+
			"Requesting goroutine stack:\n%s\n"+
			"Current readers count: %d\n",
			TimeoutThreshold,
			currentGID, string(currentStack),
			len(readers))

		// 打印所有读锁持有者的信息
		for gid, stack := range readers {
			utils.SSCLogger().Error().Msgf("Reader goroutine ID: %d\nReader stack:\n%s\n", gid, string(stack))
		}

		// 如果有写锁持有者，也打印其信息
		if rw.holderGID != 0 {
			utils.SSCLogger().Error().Msgf("Writer goroutine ID: %d\nWriter stack:\n%s\n",
				rw.holderGID, string(rw.holderStack))
		}

		utils.SSCLogger().Error().Msgf("\n")
		<-done
	}
}

func (rw *RWMutex) RUnlock() {
	currentGID := getGoroutineID()
	rw.readersMutex.Lock()
	if rw.readers != nil {
		delete(rw.readers, currentGID)
	}
	rw.readersMutex.Unlock()
	rw.rw.RUnlock()
}

// getGoroutineID 获取当前goroutine的ID
func getGoroutineID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	// 解析goroutine ID，格式为 "goroutine 123 [running]:"
	var gid uint64
	for i := 10; i < len(b); i++ {
		if b[i] == ' ' {
			break
		}
		if b[i] >= '0' && b[i] <= '9' {
			gid = gid*10 + uint64(b[i]-'0')
		}
	}
	return gid
}
