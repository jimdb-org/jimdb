/*
 * Copyright 2019 The Baudtime Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package syn

import (
	"sync"

	"github.com/baudtime/baudtime/util"
	"github.com/cespare/xxhash/v2"
)

const (
	DefaultShardCount = 1 << 8
	DefaultShardCap   = 1 << 8
)

type entry struct {
	k interface{}
	v interface{}
}

type shard struct {
	items map[interface{}]interface{}
	sync.RWMutex
	_ [40]byte
}

func (s *shard) ForEach(entrys []entry, fn func(key, val interface{}) bool) bool {
	e := &entry{}

	s.RLock()
	for e.k, e.v = range s.items {
		entrys = append(entrys, *e)
	}
	s.RUnlock()

	for _, e := range entrys {
		if !fn(e.k, e.v) {
			return false
		}
	}

	return true
}

type Map struct {
	shards     []*shard
	entryPool  sync.Pool
	shardCount int
}

func NewMap(shardCount int) *Map {
	// must be a power of 2
	if shardCount < 1 {
		shardCount = DefaultShardCount
	} else if shardCount&(shardCount-1) != 0 {
		panic("shardCount must be a power of 2")
	}

	m := &Map{
		shards:     make([]*shard, shardCount),
		shardCount: shardCount,
	}

	m.entryPool.New = func() interface{} {
		out := make([]entry, 0, DefaultShardCap)

		return &out // return a ptr to avoid extra allocation on Get/Put
	}

	for i := range m.shards {
		m.shards[i] = &shard{items: make(map[interface{}]interface{})}
	}

	return m
}

func (m *Map) getShard(key interface{}) *shard {
	return m.shards[m.hash(key)&uint64(m.shardCount-1)]
}

func (m *Map) Set(key, value interface{}) {
	shard := m.getShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

func (m *Map) SetIfAbsent(key, value interface{}) (interface{}, bool) {
	shard := m.getShard(key)
	shard.Lock()
	oldv, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()

	if ok {
		return oldv, false
	}
	return value, true
}

func (m *Map) Get(key interface{}) (interface{}, bool) {
	shard := m.getShard(key)
	shard.RLock()
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

func (m *Map) Remove(key interface{}) {
	shard := m.getShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

func (m *Map) ForEach(fn func(key, val interface{}) bool) bool {
	entrysP := m.entryPool.Get().(*[]entry)
	defer m.entryPool.Put(entrysP)

	for _, shard := range m.shards {
		entries := (*entrysP)[:0]
		if !shard.ForEach(entries, fn) {
			return false
		}
	}

	return false
}

func (m *Map) hash(key interface{}) uint64 {
	if s, ok := key.(string); ok {
		return xxhash.Sum64(util.YoloBytes(s))
	}

	if b, ok := key.([]byte); ok {
		return xxhash.Sum64(b)
	}

	panic("not support")
}
