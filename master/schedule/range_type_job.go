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

package schedule

import (
	"fmt"
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/master/service"
	"github.com/jimdb-org/jimdb/master/utils/cblog"
	"github.com/jimdb-org/jimdb/master/utils/hack"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"github.com/spf13/cast"
	"sync"
	"sync/atomic"
	"time"
)

var _ ProcessJob = &RangeTypeJob{}

type RangeTypeJob struct {
	service *service.BaseService
}

// apply range type to warm or hot
func (rj *RangeTypeJob) process(ctx *processContext) {
	if ctx.stop == true {
		log.Info("got stop so skip RangeTypeJob")
		return
	}

	for _, rng := range ctx.rangeMap {
		if table := ctx.tableMap[rng.TableId]; table != nil && table.State == entity.RecordState_Created {
			effectiveNode, invalidNode, err := rj.check(ctx, int(table.Replicas), rng)
			if err != nil {
				_ = cblog.LogErrAndReturn(err)
			}
			if len(effectiveNode) >= int(table.Replicas) && len(invalidNode) > 0 {
				rj.removePeerByInvalid(ctx, rng, invalidNode)
			}
		}
	}
}

func (rj *RangeTypeJob) check(ctx *processContext, replicasCount int, rng *basepb.Range) (effectiveNode, invalidNode []*service.NodeHandler, err error) {

	for _, peer := range rng.Peers {
		nh := ctx.nodeHandlerMap[peer.NodeId]
		if nh == nil {
			log.Warn("not found node:[%d] in peer:[%d] so skip this job", peer.NodeId, peer.Id)
			return
		}

		if nh.Type == rng.StoreType {
			effectiveNode = append(effectiveNode, nh)
		} else {
			log.Info("find invalid range:[%d] range_type:[%v] node_type:[%v]", rng.StoreType, nh.Node.Type)
			invalidNode = append(invalidNode, nh)
		}
	}

	if len(effectiveNode) >= replicasCount {
		return
	}

	log.Info("range:[%d] not enough replica need:[%d] but effective:[%d]", rng.Id, replicasCount, len(effectiveNode))

	leaderNh := ctx.nodeHandlerMap[rng.Leader]

	rh := leaderNh.GetRH(rng.Id)
	if rh == nil {
		return nil, nil, fmt.Errorf("not found leader range:[%d] in node:[%d]", rng.Id, rng.Leader)
	}

	if err := rh.CanCreateRange(ctx.nodeHandlerMap); err != nil {
		return nil, nil, err
	}

	lock := sync.Mutex{}
	m := entity.Monitor()

	moveNum := hack.PInt32(int32(replicasCount - len(effectiveNode)))

	for i := 0; i < replicasCount-len(effectiveNode); i++ {

		if leaderNh.OutSnapshot > 5 {
			select {
			case <-ctx.Context.Done():
				return nil, nil, fmt.Errorf("node:[%d] outSnaphost wait less than 5 time out ", leaderNh.Id)
			default:

			}
			log.Info("node:[%d] has more snapshot so wait it end", leaderNh.Id)
			i--
			time.Sleep(1 * time.Second)
			continue
		}

		nh, err := ctx.nodeHandlerMap.MinArriveNodeByRange(rng.StoreType, rng)
		if err != nil {
			log.Error(err.Error())
			return nil, nil, err
		}

		if err = rj.service.CreateRangeToNode(ctx, nh.Node, rng); err == nil {
			ctx.stop = true
			lock.Lock()
			nh.RangeNum++
			nh.InSnapshot++
			leaderNh.OutSnapshot++
			lock.Unlock()
			if m != nil {
				m.GetGauge(m.GetCluster(), "schedule", "event", "create_range", "success").Add(1)
			}
			//wait snapshot ok
			go func(from, to *service.NodeHandler) {

				defer func() {
					atomic.AddInt32(moveNum, -1)
					if e := recover(); e != nil {
						log.Error("!!!! panice err :[%s]", cast.ToString(e))
					}
				}()

				for {

					select {
					case <-ctx.Context.Done():
						log.Error("wait range:[%d] has node:[%d] time out ", rng.Id, to.Id)
						return
					default:

					}

					log.Info("check add range:[%d] to node is ok ", rng.Id)

					if rng, err := rj.service.QueryRange(ctx, rng.TableId, rng.Id); err != nil {
						log.Error(err.Error())
					} else {
						for _, peer := range rng.Peers {
							if peer.NodeId != to.Id {
								continue
							}

							if peer.Type == basepb.PeerType_PeerType_Normal {
								lock.Lock()
								to.RangeNum++
								to.InSnapshot--
								from.OutSnapshot--
								lock.Unlock()
								return
							}
						}

						time.Sleep(time.Second)
					}
				}

			}(leaderNh, nh)

		} else {
			return nil, nil, err
		}
	}

	for atomic.LoadInt32(moveNum) > 0 {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("wait move num ok err want 0 but %d", *moveNum)
		default:

		}

		log.Info("wait move range by type ")
		time.Sleep(time.Second)
	}

	return
}

func (rj *RangeTypeJob) removePeerByInvalid(ctx *processContext, rng *basepb.Range, handlers []*service.NodeHandler) {

	log.Info("remove range:[%d] to node ", rng.Id)

	for i := 0; i < len(handlers); i++ {

		nodeID := handlers[i].Id
		var peer *basepb.Peer

		for _, p := range rng.Peers {
			if p.NodeId == nodeID {
				peer = p
			}
		}

		if peer == nil {
			log.Error("can not found range:[%d] in node:[%d]", rng.Id, nodeID)
			return
		}

		if err := rj.service.SyncDeleteRangeToNode(ctx, rng, peer.Id, peer.NodeId); err == nil {
			handlers[i].RangeNum = handlers[i].RangeNum - 1
		}
	}
}
