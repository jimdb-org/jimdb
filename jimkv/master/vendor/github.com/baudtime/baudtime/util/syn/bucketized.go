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
	"fmt"
	"reflect"
	"sync"
)

type Bucket interface {
	Get() interface{}
	Put(interface{})
}

// BucketizedPool is a bucketed pool for variably sized byte slices.
type BucketizedPool struct {
	buckets    []Bucket
	sizes      []int
	clearOnPut bool
	make       func(int) interface{}
}

// NewBucketizedPool returns a new Pool with size buckets for minSize to maxSize, increasing by the given factor.
func NewBucketizedPool(minSize, maxSize int, factor float64, clearOnPut bool, makeFunc func(int) interface{}, initBucket func() Bucket) *BucketizedPool {
	if minSize < 1 {
		panic("invalid minimum pool size")
	}
	if maxSize < 1 {
		panic("invalid maximum pool size")
	}
	if factor < 1 {
		panic("invalid factor")
	}

	if initBucket == nil {
		initBucket = func() Bucket {
			return new(sync.Pool)
		}
	}

	var sizes []int

	for s := minSize; s <= maxSize; s = int(float64(s) * factor) {
		sizes = append(sizes, s)
	}

	p := &BucketizedPool{
		buckets:    make([]Bucket, len(sizes)),
		sizes:      sizes,
		clearOnPut: clearOnPut,
		make:       makeFunc,
	}

	for i := 0; i < len(sizes); i++ {
		p.buckets[i] = initBucket()
	}

	return p
}

func (p *BucketizedPool) Get(sz int) interface{} {
	for i, bktSize := range p.sizes {
		if sz > bktSize {
			continue
		}
		b := p.buckets[i].Get()
		if b == nil {
			b = p.make(bktSize)
		}
		return b
	}
	return p.make(sz)
}

// Put adds a slice to the right Bucket in the pool.
func (p *BucketizedPool) Put(s interface{}) {
	slice := reflect.ValueOf(s)

	if slice.Kind() != reflect.Slice {
		panic(fmt.Sprintf("%+v is not a slice", slice))
	}

	capacity := slice.Cap()
	for i, size := range p.sizes {
		if capacity > size {
			continue
		}
		if p.clearOnPut {
			p.buckets[i].Put(slice.Slice(0, 0).Interface())
		} else {
			p.buckets[i].Put(slice.Slice(0, capacity).Interface())
		}
		return
	}
}
