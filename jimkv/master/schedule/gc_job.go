// Copyright 2019 The JimDB Authors
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
	"github.com/jimdb-org/jimdb/jimkv/master/entity"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/jimkv/master/service"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/hack"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"sync/atomic"
	"time"
)

var _ ProcessJob = &GCJob{}

type GCJob struct {
	service *service.BaseService
}

func (gc *GCJob) process(ctx *processContext) {
	if ctx.stop {
		log.Info("got stop so skip GCjob")
		return
	}
	log.Info("start GCjob begin")

	gc.processNodeRange(ctx)

	gc.processRange(ctx)

	gc.processTable(ctx)
}

func (gc *GCJob) processRange(ctx *processContext) {
	for _, rng := range ctx.rangeMap {
		if ctx.tableMap[rng.TableId] == nil {
			log.Info("Could not find Table:[%d] contains range,rangeID:[%d] so remove it from etcd!", rng.TableId, rng.Id)

			gc.deleteRangeKeyWithMonitor(ctx, rng)
		}
	}

	for _, createRecord := range ctx.createRecordMap {
		if createRecord.CreateState != entity.RecordState_Created {
			if createRecord.CreateState == entity.RecordState_Creating &&
				time.Now().Unix()-createRecord.CreateTime <= 1*3600 {
				continue
			}

			if createRecord.CreateState == entity.RecordState_Creating &&
				time.Now().Unix()-createRecord.CreateTime > 1*3600 {
				log.Warn("range[%v] will be deleted later, because its create request is still in RecordState_Creating state over 1h", createRecord.RngIds)
			}

			for _, rngId := range createRecord.RngIds {
				rng, ok := ctx.rangeMap[rngId]
				if !ok {
					continue
				}

				gc.deleteRangeKeyWithMonitor(ctx, rng)
			}
		}
	}

}

func (gc *GCJob) processTable(ctx *processContext) {
	for _, table := range ctx.tableMap {
		if _, ok := ctx.dbMap[table.DbId]; !ok {
			log.Info("Could not find db:[%d] contains table:[%d], so remove it from etcd!", table.DbId, table.TableId)
			gc.deleteTableKeyWithMonitor(ctx, table.DbId, table.TableId)
		}
		if table.State != entity.RecordState_Created && time.Now().Unix()-table.CreateTime <= 1*3600 {
			tableHasRange := false
			for _, rng := range ctx.rangeMap {
				if rng.TableId == table.TableId {
					tableHasRange = true
					break
				}
			}
			if !tableHasRange {
				gc.deleteTableKeyWithMonitor(ctx, table.DbId, table.TableId)
			}
		}
	}
}

func (gc *GCJob) deleteTableKeyWithMonitor(ctx *processContext, dbId, tableId int32) {
	m := entity.Monitor()
	if err := gc.service.Delete(ctx, entity.TableKey(dbId, tableId)); err != nil {
		log.Error("delete table err :[%s]", err.Error())
		if m != nil {
			m.GetGauge(m.GetCluster(), "schedule", "event", "clear_table", "fail").Add(1)
		}
	} else {
		if m != nil {
			m.GetGauge(m.GetCluster(), "schedule", "event", "clear_table", "success").Add(1)
		}
	}
}

func (gc *GCJob) deleteRangeKeyWithMonitor(ctx *processContext, rng *basepb.Range) {
	m := entity.Monitor()
	if err := gc.service.Delete(ctx, entity.RangeKey(rng.TableId, rng.Id)); err != nil {
		log.Error("delete range err :[%s]", err.Error())
		if m != nil {
			m.GetGauge(m.GetCluster(), "schedule", "event", "delete_range", "fail").Add(1)
		}
	} else {
		if rng.RangeType == basepb.RangeType_RNG_Index {
			if err = gc.service.Delete(ctx, entity.IndexRangeKey(rng.TableId, rng.IndexId, rng.Id)); err != nil {
				log.Error("delete range err :[%s]", err.Error())
			}
		}
		ctx.stop = true
		if m != nil {
			m.GetGauge(m.GetCluster(), "schedule", "event", "delete_range", "success").Add(1)
		}
	}
}

func (gc *GCJob) processNodeRange(ctx *processContext) {

	delMap := make(map[uint64][]*service.RangeHandler)
	delRngMap := make(map[uint64]*basepb.Range)

	counter := hack.PInt32(0)
	m := entity.Monitor()
	for _, nh := range ctx.nodeHandlerMap {
		for _, rh := range nh.RangeHanders {

			table, found := ctx.tableMap[rh.TableId]

			if !found {
				if ctx.nodeMap[nh.Node.Id] == nil {
					continue
				}
				atomic.AddInt32(counter, 1)
				delMap[nh.Node.Id] = append(delMap[nh.Node.Id], rh)
			} else { //if this range has db and table , check range is out of
				if table.State != entity.RecordState_Created {
					continue
				}

				dbRng := ctx.rangeMap[rh.Id]
				if dbRng == nil {
					log.Warn("range:[%d] not register in db", rh.Id)
					continue
				}
				if rh.RangeEpoch.Version <= dbRng.RangeEpoch.Version && rh.Range.RangeEpoch.ConfVer < dbRng.RangeEpoch.ConfVer {
					dbPeers := make(map[uint64]*basepb.Peer)
					for _, p := range dbRng.Peers {
						dbPeers[p.Id] = p
					}

					if _, found := dbPeers[rh.Peer.Id]; !found {
						log.Info("find outside range not in range peers node:[%d] peerID:[%d] version:[%v] dbVersion:[%v]", rh.Peer.NodeId, rh.Peer.Id, rh.RangeEpoch, dbRng.RangeEpoch)
						if err := gc.service.SyncDeleteRangeToNode(ctx, rh.Range, rh.Peer.Id, rh.Peer.NodeId); err != nil {
							log.Error("delete range by node:[%d] peer:[%d] has err:[%s]", rh.Peer.NodeId, rh.Peer.Id, err.Error())
							if m != nil {
								m.GetGauge(m.GetCluster(), "schedule", "event", "delete_range", "fail").Add(1)
							}
						} else {
							ctx.stop = true
							log.Info("delete range by node:[%d] peer:[%d]", rh.Peer.NodeId, rh.Peer.Id)
							if m != nil {
								m.GetGauge(m.GetCluster(), "schedule", "event", "delete_range", "success").Add(1)
							}
						}
					}
				}
			}
		}
	}

	for _, createRecord := range ctx.createRecordMap {
		if createRecord.CreateState != entity.RecordState_Created {
			if createRecord.CreateState == entity.RecordState_Creating &&
				time.Now().Unix()-createRecord.CreateTime <= 1*3600 {
				continue
			}

			if createRecord.CreateState == entity.RecordState_Creating &&
				time.Now().Unix()-createRecord.CreateTime > 1*3600 {
				log.Warn("range[%v] will be deleted later, because its create request is still in RecordState_Creating state over 1h", createRecord.RngIds)
			}
			for _, rngId := range createRecord.RngIds {
				if rng, ok := ctx.rangeMap[rngId]; ok {
					delRngMap[rngId] = rng
					atomic.AddInt32(counter, 1)
				}
			}
		}
	}

	for nodeID, rangeHandlers := range delMap {
		go func(node *basepb.Node, rhs []*service.RangeHandler) {
			defer func() {
				if i := recover(); i != nil {
					log.Error("panic err %v", i)
				}
			}()
			for _, rh := range rhs {
				if rh.Peer == nil {
					log.Error("range:[%d] has nil peer so del it", rh.Id)
					continue
				}

				gc.deleteDsRange(ctx, node, rh.Id, rh.Peer.Id)
				atomic.AddInt32(counter, -1)
			}
		}(ctx.nodeMap[nodeID], rangeHandlers)
	}

	for _, rng := range delRngMap {
		if rng.Peers == nil {
			log.Error("range:[%d] has no peers to del", rng.Id)
			continue
		}
		for _, peer := range rng.Peers {
			node, ok := ctx.nodeMap[peer.NodeId]
			if !ok {
				continue
			}
			gc.deleteDsRange(ctx, node, rng.Id, peer.Id)
		}
		atomic.AddInt32(counter, -1)
	}

	for *counter > 0 {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(1 * time.Second)
		}
	}

}

func (gc *GCJob) deleteDsRange(ctx *processContext, node *basepb.Node, rangeId, peerId uint64) {
	m := entity.Monitor()

	if err := gc.service.DsClient().DeleteRange(ctx, nodeAddr(node), rangeId, peerId); err != nil {
		log.Error("delete range err:[%s]", err.Error())
		if m != nil {
			m.GetGauge(m.GetCluster(), "schedule", "event", "delete_range", "fail").Add(1)
		}
	} else {
		ctx.stop = true
		log.Info("job delete range ok :rangeID:[%d], peerID:[%d]", rangeId, peerId)
		if m != nil {
			m.GetGauge(m.GetCluster(), "schedule", "event", "delete_range", "success").Add(1)
		}
	}
}
