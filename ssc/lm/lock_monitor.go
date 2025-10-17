package lm

import (
	"github.com/harmony-one/harmony/internal/utils"
	"runtime/debug"
	"sync"
	"time"
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
	mu sync.Mutex
}

func (m *Mutex) Lock() {
	if !MonitorEnabled {
		m.mu.Lock()
		return
	}

	done := make(chan struct{})
	go func() {
		m.mu.Lock()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(TimeoutThreshold):
		stack := debug.Stack()
		utils.SSCLogger().Error().Msgf("\n\n⚠️ MUTEX LOCK TIMEOUT DETECTED ⚠️\n"+
			"Waited longer than %s\n"+
			"Current goroutine stack:\n%s\n\n",
			TimeoutThreshold, string(stack))
		<-done // 仍然获取锁，但已记录问题
	}
}

func (m *Mutex) Unlock() {
	m.mu.Unlock()
}

// RWMutex 带监控的读写锁
type RWMutex struct {
	rw          sync.RWMutex
	lockedStack []byte
}

func (rw *RWMutex) Lock() {
	if !MonitorEnabled {
		rw.rw.Lock()
		return
	}

	done := make(chan struct{})
	stack := debug.Stack()
	go func() {
		rw.rw.Lock()
		rw.lockedStack = stack
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(TimeoutThreshold):
		utils.SSCLogger().Error().Msgf("\n\n⚠️ RW MUTEX LOCK TIMEOUT DETECTED ⚠️\n"+
			"Waited longer than %s\n"+
			"Current goroutine stack:\n%s\n\nLocked at:\n%s\n",
			TimeoutThreshold, string(stack), string(rw.lockedStack))
		<-done
	}
}

func (rw *RWMutex) Unlock() {
	rw.lockedStack = nil
	rw.rw.Unlock()
}

func (rw *RWMutex) RLock() {
	if !MonitorEnabled {
		rw.rw.RLock()
		return
	}

	done := make(chan struct{})
	go func() {
		rw.rw.RLock()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(TimeoutThreshold):
		stack := debug.Stack()
		utils.SSCLogger().Error().Msgf("\n\n⚠️ RW MUTEX RLOCK TIMEOUT DETECTED ⚠️\n"+
			"Waited longer than %s\n"+
			"Current goroutine stack:\n%s\n\n",
			TimeoutThreshold, string(stack))
		<-done
	}
}

func (rw *RWMutex) RUnlock() {
	rw.rw.RUnlock()
}
