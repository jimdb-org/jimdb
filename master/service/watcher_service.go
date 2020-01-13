// Copyright 2019 The JIMDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package service

import (
	"container/list"
	"context"
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"github.com/spf13/cast"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

const watcher_max_size = 50000

func NewWatcherService(service *BaseService) *WatcherService {
	return &WatcherService{service: service}
}

type WatcherService struct {
	service *BaseService
	job     *watcherJob
}

type watcherJob struct {
	*BaseService
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	service *BaseService
	lock    sync.RWMutex
	queue   *list.List
	version uint64
}

func (ws *WatcherService) Watcher(ctx context.Context, version uint64) []*basepb.WatcherEvent {
	ws.job.lock.RLock()
	defer ws.job.lock.RUnlock()

	if version == 0 {
		return toList(ws.job.queue.Front())
	}

	return toList(searchElement(ctx, ws.job.queue, version))

}

func (ws *WatcherService) Version() uint64 {
	return ws.job.version
}

func searchElement(ctx context.Context, queue *list.List, version uint64) *list.Element {
	if queue.Len() == 0 {
		return nil
	}

	timeout := 10

	if deadline, ok := ctx.Deadline(); ok {
		out := deadline.Sub(time.Now()).Seconds()
		if out > 1 {
			timeout = int(out)
		}
	}

	timeOut, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	last := queue.Back()
	for {
		if last.Value.(*basepb.WatcherEvent).Version > version {
			break
		}
		select {
		case <-timeOut.Done():
			return nil
		default:
			time.Sleep(200 * time.Millisecond)
		}

		last = queue.Back()
	}

	for {
		if last == nil {
			return queue.Front()
		}
		if last.Value.(*basepb.WatcherEvent).Version <= version {
			return last.Next()
		}
		last = last.Prev()
	}

}

func toList(element *list.Element) []*basepb.WatcherEvent {
	if element == nil {
		return nil
	}
	result := make([]*basepb.WatcherEvent, 0, 10)

	for {
		result = append(result, element.Value.(*basepb.WatcherEvent))
		element = element.Next()
		if element == nil {
			return result
		}
	}
}

func (ws *WatcherService) Cancel() {
	if ws != nil && ws.job != nil {
		ws.job.cancel()
	}
}

func (ws *WatcherService) StartWatcher() {

	if ws != nil && ws.job != nil {
		log.Warn("to stop job and restart a new")
		ws.job.cancel()
	} else {
		log.Info("to start watcher ")
	}

	wj := &watcherJob{service: ws.service, queue: list.New()}
	wj.ctx, wj.cancel = context.WithCancel(context.Background())

	wj.startWatcher(entity.PrefixNode, wj.AddNodeEvent)
	wj.startWatcher(entity.PrefixDataBase, wj.AddDatabaseEvent)
	wj.startWatcher(entity.PrefixTable, wj.AddTableEvent)
	wj.startWatcher(entity.PrefixRange, wj.AddRangeEvent)
	ws.job = wj

}

func (wj *watcherJob) startWatcher(prefix string, addEnvent addEnvent) {
	go func() {
		defer func() {
			if rErr := recover(); rErr != nil {
				log.Error("recover() err:[%v]", rErr)
				log.Error("stack:[%s]", debug.Stack())
			}
		}()
		for {
			select {
			case <-wj.ctx.Done():
				log.Debug("watchjob %s job to stop", prefix)
				return
			default:
				log.Debug("start watcher %s", prefix)
			}

			wj.wg.Add(1)
			go func() {
				defer func() {
					if rErr := recover(); rErr != nil {
						log.Error("recover() err:[%v]", rErr)
						log.Error("stack:[%s]", debug.Stack())
					}
				}()
				defer wj.wg.Done()

				select {
				case <-wj.ctx.Done():
					log.Debug("watchjob %s job to stop", prefix)
					return
				default:
				}

				watcher, err := wj.service.WatchPrefix(wj.ctx, prefix)

				if err != nil {
					log.Error("watch prefix:[%s] err", prefix)
					time.Sleep(1 * time.Second)
					return
				}

				for reps := range watcher {
					if reps.Canceled {
						log.Error("chan is closed by server watcher job")
						return
					}

					for _, event := range reps.Events {
						switch event.Type {
						case mvccpb.PUT, mvccpb.DELETE:
							addEnvent(event.Type, event.Kv.Key, event.Kv.Value)
						}
					}
				}
			}()
			wj.wg.Wait()
		}
	}()

}

type addEnvent func(et mvccpb.Event_EventType, key []byte, value []byte)

func (wj *watcherJob) AddNodeEvent(et mvccpb.Event_EventType, key []byte, value []byte) {
	split := strings.Split(string(key), "/")

	if len(split) != 3 {
		return
	}

	nodeID := cast.ToUint64(split[2])

	event := &basepb.WatcherEvent{
		WatcherType: basepb.WatcherType_Watcher_Type_Node,
		EventType:   etcdType2WatcherType(et),
		NodeId:      nodeID,
		Value:       value,
	}

	wj.lock.Lock()
	defer wj.lock.Unlock()
	wj.version++
	event.Version = wj.version
	wj.queue.PushBack(event)

	for wj.queue.Len() > watcher_max_size {
		wj.queue.Remove(wj.queue.Front())
	}
}

func (wj *watcherJob) AddDatabaseEvent(et mvccpb.Event_EventType, key []byte, value []byte) {
	split := strings.Split(string(key), "/")

	if len(split) != 3 {
		return
	}

	dbID := cast.ToInt32(split[2])

	event := &basepb.WatcherEvent{
		WatcherType: basepb.WatcherType_Watcher_Type_Database,
		EventType:   etcdType2WatcherType(et),
		DbId:        dbID,
		Value:       value,
	}

	wj.lock.Lock()
	defer wj.lock.Unlock()
	wj.version++
	event.Version = wj.version
	wj.queue.PushBack(event)

	for wj.queue.Len() > watcher_max_size {
		wj.queue.Remove(wj.queue.Front())
	}
}

func (wj *watcherJob) AddTableEvent(et mvccpb.Event_EventType, key []byte, value []byte) {
	split := strings.Split(string(key), "/")

	if len(split) != 4 {
		return
	}

	dbID := cast.ToInt32(split[2])
	tableID := cast.ToInt32(split[3])

	event := &basepb.WatcherEvent{
		WatcherType: basepb.WatcherType_Watcher_Type_Table,
		EventType:   etcdType2WatcherType(et),
		DbId:        dbID,
		TableId:     tableID,
		Value:       value,
	}

	wj.lock.Lock()
	defer wj.lock.Unlock()
	wj.version++
	event.Version = wj.version
	wj.queue.PushBack(event)

	for wj.queue.Len() > watcher_max_size {
		wj.queue.Remove(wj.queue.Front())
	}
}

func (wj *watcherJob) AddRangeEvent(et mvccpb.Event_EventType, key []byte, value []byte) {
	split := strings.Split(string(key), "/")

	if len(split) != 4 {
		return
	}

	tableID := cast.ToInt32(split[2])
	rangeID := cast.ToUint64(split[3])

	rng := &basepb.Range{}

	if et == mvccpb.PUT {
		if err := rng.Unmarshal(value); err != nil {
			log.Error("watcher unmarshal range has err:[%s]", err.Error())
			return
		} else {
			rng.Peers, rng.KeySchema = nil, nil
			if value, err = rng.Marshal(); err != nil {
				log.Error("watcher marshal range has err:[%s]", err.Error())
				return
			}
		}
	}

	event := &basepb.WatcherEvent{
		WatcherType: basepb.WatcherType_Watcher_Type_Range,
		EventType:   etcdType2WatcherType(et),
		TableId:     tableID,
		RangeId:     rangeID,
		DbId:        rng.DbId,
		Value:       value,
	}

	wj.lock.Lock()
	defer wj.lock.Unlock()
	wj.version++
	event.Version = wj.version
	wj.queue.PushBack(event)

	for wj.queue.Len() > watcher_max_size {
		wj.queue.Remove(wj.queue.Front())
	}
}

func etcdType2WatcherType(event mvccpb.Event_EventType) basepb.EventType {
	switch event {
	case mvccpb.PUT:
		return basepb.EventType_Event_Type_PUT
	case mvccpb.DELETE:
		return basepb.EventType_Event_Type_DELETE
	default:
		panic("unknown")
	}
}
