// Copyright 2021 ByteDance Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gopool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Pool interface {
	// Name returns the corresponding pool name.
	Name() string
	// SetCap sets the goroutine capacity of the pool.
	SetCap(cap int32)
	// Go executes f.
	Go(f func())
	// CtxGo executes f and accepts the context.
	CtxGo(ctx context.Context, f func())
	// SetPanicHandler sets the panic handler.
	SetPanicHandler(f func(context.Context, interface{}))
	// WorkerCount returns the number of running workers
	WorkerCount() int32
}

var taskPool sync.Pool

func init() {
	taskPool.New = newTask
}

type task struct {
	ctx context.Context
	f   func()

	next *task
}

func (t *task) zero() {
	t.ctx = nil
	t.f = nil
	t.next = nil
}

func (t *task) Recycle() {
	t.zero()
	taskPool.Put(t)
}

func newTask() interface{} {
	return &task{}
}

type taskList struct {
	sync.Mutex
	taskHead *task
	taskTail *task
}

type pool struct {
	// The name of the pool
	name string

	// capacity of the pool, the maximum number of goroutines that are actually working
	cap int32
	// Configuration information
	config *Config
	// linked list of tasks
	taskHead  *task
	taskTail  *task
	taskLock  sync.Mutex
	taskCount int32

	// Record the number of running workers
	workerCount int32

	// This method will be called when the worker panic
	panicHandler func(context.Context, interface{})

	// stats
	newCnt          int64
	totalCallCnt    int64
	workerStats     []int64 // 0, 1, 10, 100, 1000, 10000
	workerStatsLock sync.RWMutex
}

// NewPool creates a new pool with the given name, cap and config.
func NewPool(name string, cap int32, config *Config) Pool {
	p := &pool{
		name:        name,
		cap:         cap,
		config:      config,
		workerStats: make([]int64, 6),
	}
	go p.dumpStat()
	return p
}

func (p *pool) Name() string {
	return p.name
}

func (p *pool) SetCap(cap int32) {
	atomic.StoreInt32(&p.cap, cap)
}

func (p *pool) Go(f func()) {
	p.CtxGo(context.Background(), f)
}

func (p *pool) CtxGo(ctx context.Context, f func()) {
	t := taskPool.Get().(*task)
	t.ctx = ctx
	t.f = f
	p.taskLock.Lock()
	if p.taskHead == nil {
		p.taskHead = t
		p.taskTail = t
	} else {
		p.taskTail.next = t
		p.taskTail = t
	}
	p.taskLock.Unlock()
	atomic.AddInt32(&p.taskCount, 1)
	atomic.AddInt64(&p.totalCallCnt, 1)
	// The following two conditions are met:
	// 1. the number of tasks is greater than the threshold.
	// 2. The current number of workers is less than the upper limit p.cap.
	// or there are currently no workers.
	if (atomic.LoadInt32(&p.taskCount) >= p.config.ScaleThreshold && p.WorkerCount() < atomic.LoadInt32(&p.cap)) || p.WorkerCount() == 0 {
		p.incWorkerCount()
		w := workerPool.Get().(*worker)
		w.pool = p
		w.run()
	}
}

// SetPanicHandler the func here will be called after the panic has been recovered.
func (p *pool) SetPanicHandler(f func(context.Context, interface{})) {
	p.panicHandler = f
}

func (p *pool) WorkerCount() int32 {
	return atomic.LoadInt32(&p.workerCount)
}

func (p *pool) incWorkerCount() {
	atomic.AddInt32(&p.workerCount, 1)
}

func (p *pool) decWorkerCount() {
	atomic.AddInt32(&p.workerCount, -1)
}

func (p *pool) recordWorkerTaskNum(cnt int64) {
	p.workerStatsLock.Lock()
	defer p.workerStatsLock.Unlock()

	idx := 0
	for cnt > 0 {
		idx++
		cnt /= 10
	}
	if idx >= len(p.workerStats) {
		p.workerStats[len(p.workerStats)-1]++
	} else {
		p.workerStats[idx]++
	}
}

func (p *pool) dumpStat() {
	for range time.Tick(1 * time.Second) {
		if atomic.LoadInt64(&p.newCnt) != 0 {
			p.workerStatsLock.RLock()
			fmt.Printf("[gopool] total GoCtx cnt=%d, new goroutine cnt=%d\n", p.totalCallCnt, p.newCnt)
			fmt.Printf("[gopool] stats %v\n", p.workerStats)
			fmt.Printf("[gopool] stats [0]:%.2f%%, [1]:%.2f%%, [10]:%.2f%%, [100]:%.2f%%, [1000]:%.2f%%, [10000]:%.2f%%\n",
				float64(p.workerStats[0])/float64(p.newCnt)*100,
				float64(p.workerStats[1])/float64(p.newCnt)*100,
				float64(p.workerStats[2])/float64(p.newCnt)*100,
				float64(p.workerStats[3])/float64(p.newCnt)*100,
				float64(p.workerStats[4])/float64(p.newCnt)*100,
				float64(p.workerStats[5])/float64(p.newCnt)*100,
			)
			p.workerStatsLock.RUnlock()
		}
	}
}
