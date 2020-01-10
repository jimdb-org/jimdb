// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package syn

import (
	"math"
	"sync/atomic"
)

type ObjectPool struct {
	objects             chan interface{}
	size                int
	refillLowWatermark  int
	refillHighWatermark int
	refilling           int32
	create              func() interface{}
	destroy             func(interface{})
}

func NewObjectPool(size int, create func() interface{}) *ObjectPool {
	return NewObjectPoolWithWaterMark(size, 0, 0, create, nil)
}

func NewObjectPoolWithDestroy(size int, create func() interface{}, destroy func(interface{})) *ObjectPool {
	return NewObjectPoolWithWaterMark(size, 0, 0, create, destroy)
}

func NewObjectPoolWithWaterMark(size int, refillLowWaterMark float64, refillHighWaterMark float64, create func() interface{}, destroy func(interface{})) *ObjectPool {
	p := &ObjectPool{
		objects:             make(chan interface{}, size),
		size:                size,
		refillLowWatermark:  int(math.Ceil(refillLowWaterMark * float64(size))),
		refillHighWatermark: int(math.Ceil(refillHighWaterMark * float64(size))),
		create:              create,
		destroy:             destroy,
	}
	return p
}

func (p *ObjectPool) Get() interface{} {
	var obj interface{}
	select {
	case obj = <-p.objects:
	default:
		if p.create != nil {
			obj = p.create()
		}
	}

	if p.create != nil && p.refillLowWatermark > 0 && len(p.objects) <= p.refillLowWatermark {
		p.tryFill()
	}
	return obj
}

func (p *ObjectPool) Put(obj interface{}) {
	select {
	case p.objects <- obj:
	default:
		if p.destroy != nil {
			p.destroy(obj)
		}
	}
}

func (p *ObjectPool) Clear() {
	for {
		select {
		case obj := <-p.objects:
			if p.destroy != nil {
				p.destroy(obj)
			}
		default:
			return
		}
	}
}

func (p *ObjectPool) ForEach(do func(obj interface{})) {
	for {
		select {
		case obj := <-p.objects:
			do(obj)
		default:
			return
		}
	}
}

func (p *ObjectPool) tryFill() {
	if !atomic.CompareAndSwapInt32(&p.refilling, 0, 1) {
		return
	}

	go func() {
		defer atomic.StoreInt32(&p.refilling, 0)

		for len(p.objects) < p.refillHighWatermark {
			select {
			case p.objects <- p.create():
			default:
				return
			}
		}
	}()
}
