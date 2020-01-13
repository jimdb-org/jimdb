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
	"github.com/pkg/errors"
	"sync"
)

type Queue struct {
	closed bool
	ch     chan interface{}
	sync.RWMutex
}

func NewQueue(cap int) *Queue {
	return &Queue{ch: make(chan interface{}, cap)}
}

func (q *Queue) Enqueue(element interface{}) (err error) {
	q.RLock()
	if q.closed {
		err = errors.New("queue is closed")
	} else {
		q.ch <- element
	}
	q.RUnlock()

	return
}

func (q *Queue) Dequeue(block bool) interface{} {
	if block {
		return <-q.ch
	} else {
		select {
		case e := <-q.ch:
			return e
		default:
		}
		return nil
	}
}

func (q *Queue) Close() {
	go func() {
		for range q.ch {
		}
	}()
	q.Lock()
	defer q.Unlock()
	if q.closed {
		return
	}
	close(q.ch)
	q.closed = true
}
