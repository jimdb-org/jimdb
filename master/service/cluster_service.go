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
	"context"
	"encoding/json"
	"fmt"
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/errs"
	"github.com/jimdb-org/jimdb/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/master/entity/pkg/mspb"
	"github.com/jimdb-org/jimdb/master/utils/cblog"
	"github.com/jimdb-org/jimdb/master/utils/hack"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"github.com/spf13/cast"
	"go.etcd.io/etcd/clientv3/concurrency"
	"math/rand"
	"time"
)

func (cs *BaseService) createRange(targetNodes []*basepb.Node, errChain chan error, ctx context.Context, r *basepb.Range) {
	for _, node := range targetNodes {
		go func(node *basepb.Node, r *basepb.Range) {
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("create range err: %v ", r)
					errChain <- err
					log.Error(err.Error())
				}
			}()
			log.Info("ready to create range:[%d] with peer:[%v] on node:[%d]", r.Id, r.Peers, node.Id)
			if err := cs.dsClient.CreateRange(ctx, NodeServerAddr(node), r); err != nil {
				err := fmt.Errorf("create range err: %s ", err.Error())
				errChain <- err
				log.Error(err.Error())
			}
		}(node, r)
	}
}

func (cs *BaseService) checkRangeOk(ctx context.Context, ranges []*basepb.Range, errChain chan error) error {
	for i := 0; i < len(ranges); i++ {
		v := 0
		for {
			v++
			select {
			case err := <-errChain:
				return cblog.LogErrAndReturn(err)
			case <-ctx.Done():
				return fmt.Errorf("timeout to check data ranges heartbeat")
			default:

			}

			rg, err := cs.QueryRange(ctx, ranges[i].TableId, ranges[i].Id)
			if v%5 == 0 {
				log.Debug("check range[%d] status, range is nil[%t]", ranges[i].Id, rg == nil)
			}
			if err != nil && errs.Code(err) != mspb.ErrorType_NotExistRange {
				if v >= 10 {
					return cblog.LogErrAndReturn(err)
				}
				log.Warn("checkRange err[%v], range[%d] table[%d] db[%d]", err, ranges[i].Id, ranges[i].TableId, ranges[i].DbId)
			}
			if rg == nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			break
		}
	}
	return nil
}

func (cs *BaseService) separateHotWarnNodes(nodes []*basepb.Node) (hot, warm []*basepb.Node, err error) {

	for _, node := range nodes {
		if node.Type == basepb.StoreType_Store_Hot {
			hot = append(hot, node)
		} else if node.Type == basepb.StoreType_Store_Warm {
			warm = append(warm, node)
		} else {
			log.Error("err type of node:[%d] type:[%d]", node.Id, node.Type)
		}
	}

	return
}

func (cs *BaseService) ConfigAutoSplit(ctx context.Context) bool {
	if config, err := cs.boolConfig(ctx, entity.ConfAutoSplit); err != nil {
		log.Error("get config err:[%s]", err.Error())
		return true
	} else if config == nil {
		return true
	} else {
		return *config
	}
}

func (cs *BaseService) ConfigFailOver(ctx context.Context) bool {
	if config, err := cs.boolConfig(ctx, entity.ConfFailOver); err != nil {
		log.Error("get config err:[%s]", err.Error())
		return true
	} else if config == nil {
		return true
	} else {
		return *config
	}
}

func (cs *BaseService) ConfigBalanced(ctx context.Context) bool {
	if config, err := cs.boolConfig(ctx, entity.ConfBalanced); err != nil {
		log.Error("get config err:[%s]", err.Error())
		return true
	} else if config == nil {
		return true
	} else {
		return *config
	}
}

func (cs *BaseService) boolConfig(ctx context.Context, key string) (*bool, error) {
	bytes, err := cs.Store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(bytes) > 1 {
		return nil, fmt.Errorf("make sure it`s config key")
	} else if len(bytes) == 0 {
		return nil, nil
	} else {
		return hack.PBool(bytes[0] == 1), nil
	}

}

func (cs *BaseService) PutBoolConfig(ctx context.Context, key string, value bool) error {
	var v byte
	if value {
		v = 1
	}
	return cs.Store.Put(ctx, key, []byte{v})
}

func NodeServerAddr(node *basepb.Node) string {
	return fmt.Sprintf("%s:%d", node.GetIp(), node.GetServerPort())
}

//new services..........................................................................................................................................
func (cs *BaseService) CreateRanges(ctx context.Context, req *mspb.CreateRangesRequest) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	//to lock cluster
	lock := cs.NewLock(ctx, entity.LockCluster(), time.Minute*5)
	if err := lock.Lock(); err != nil {
		return cblog.LogErrAndReturn(err)
	}
	lockStart := time.Now()
	defer func() {
		// may lost lock
		if time.Now().Sub(lockStart) > time.Minute*5 {
			log.Error("unlock cluster over 1 min later, may lost lock already, dbId[%d] tableId[%d]", req.DbId, req.TableId)
			return
		}
		if err := lock.Unlock(); err != nil {
			log.Error("unlock db:[%d] err:[%s] ", req.DbId, err)
		}
	}()

	createIndexOnly := true
	for _, rng := range req.Ranges {
		if rng.RangeType == basepb.RangeType_RNG_Data {
			createIndexOnly = false
			break
		}
	}

	table, err := cs.QueryTableByID(ctx, req.DbId, req.TableId)
	if err != nil {
		if createIndexOnly {
			return cblog.LogErrAndReturn(err)
		} else {
			if err != errs.Error(mspb.ErrorType_NotExistTable) {
				return cblog.LogErrAndReturn(err)
			}
		}
	} else {
		if createIndexOnly {
			if table.State != entity.RecordState_Created {
				log.Error("create index err for RecordState=[%s], dbId[%d] tableId[%d]", req.DbId, req.TableId, table.State)
				return cblog.LogErrAndReturn(errs.Error(mspb.ErrorType_NotExistTable))
			}
		} else {
			// prev master dead or lost lock because of timeout
			if table.State == entity.RecordState_Creating {
				log.Error("found a RecordState_Creating record, dbId=[%d], tableId=[%d]", req.DbId, req.TableId)
			} else if table.State == entity.RecordState_Created {
				log.Info("found a RecordState_Created record, so just return, dbId=[%d], tableId=[%d]", req.DbId, req.TableId)
				return nil
			} else if table.State == entity.RecordState_Failed {
				log.Info("found a RecordState_Failed record, re-creating... dbId=[%d] tableId=[%d]", req.DbId, req.TableId)
			} else {
				// should be impossible
				log.Error("found a RecordState_Unknown record, dbId=[%d] tableId=[%d]", req.DbId, req.TableId)
			}
		}
	}

	//get all nodes
	allNodes, err := cs.QueryOnlineNodes(ctx)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	if req.Replicas <= 0 {
		replicasInConf := int32(entity.Conf().Global.ReplicaNum)
		log.Info("replicas[%d] in request is illegal, make it to default[%d] in conf", req.Replicas, replicasInConf)
		req.Replicas = replicasInConf
	}

	if req.Replicas <= 0 {
		log.Info("replicas[%d] in conf is illegal, make it to 3 directly", req.Replicas)
		req.Replicas = 3
	}

	//filter nodes
	nodes := make([]*basepb.Node, 0, len(allNodes))
	for _, node := range allNodes {
		if node.State == basepb.NodeState_N_Online && cs.dsClient.IsAlive(ctx, NodeServerAddr(node)) {
			nodes = append(nodes, node)
		}
	}
	if len(nodes) < int(req.Replicas) {
		log.Error("not enough nodes, totally only:[%d], need replicas:[%d]", len(nodes), int(req.Replicas))
		return errs.Error(mspb.ErrorType_NodeNotEnough)
	}

	var errChain = make(chan error, 1)

	hot, warm, _ := cs.separateHotWarnNodes(nodes)
	var checkHot, checkWarm bool
	for _, rng := range req.Ranges {
		if rng.StoreType == basepb.StoreType_Store_Hot {
			checkHot = true
		}
		if rng.StoreType == basepb.StoreType_Store_Warm {
			checkWarm = true
		}
		if checkHot && checkWarm {
			break
		}
	}
	if checkHot && len(hot) < int(req.Replicas) {
		log.Error("not enough hot nodes, only:[%d], need replica:[%d]", len(hot), req.Replicas)
		return errs.Error(mspb.ErrorType_NodeNotEnough)
	}
	if checkWarm && len(warm) < int(req.Replicas) {
		log.Error("not enough warm nodes, only:[%d], need replica:[%d]", len(warm), req.Replicas)
		return errs.Error(mspb.ErrorType_NodeNotEnough)
	}

	rngIds := make([]uint64, 0)
	for _, rng := range req.Ranges {
		if rangeID, err := cs.NewIDGenerate(ctx, entity.SequenceRangeID, 1, 5*time.Second); err != nil {
			return cblog.LogErrAndReturn(err)
		} else {
			rng.Id = uint64(rangeID)
			rngIds = append(rngIds, rng.Id)
		}

		rng.DbId = req.DbId
		rng.TableId = req.TableId
		rng.RangeEpoch = &basepb.RangeEpoch{
			ConfVer: 1,
			Version: 1,
		}
	}

	createID, err := cs.NewIDGenerate(ctx, entity.SequenceCreateID, 1, 5*time.Second)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}
	createRecord := &entity.CreateRecord{
		CreateId:    createID,
		RngIds:      rngIds,
		CreateTime:  time.Now().Unix(),
		CreateState: entity.RecordState_Creating,
	}

	// store TableRecord first, in case of cycled by gc on heartbeat
	var tableRecord *entity.TableRecord
	if !createIndexOnly {
		tableRecord = &entity.TableRecord{
			DbId:       req.DbId,
			TableId:    req.TableId,
			Replicas:   req.Replicas,
			CreateTime: time.Now().Unix(),
			State:      entity.RecordState_Creating,
		}

		if tableBytes, err := json.Marshal(tableRecord); err != nil {
			return cblog.LogErrAndReturn(err)
		} else {
			err := cs.STM(context.Background(), func(stm concurrency.STM) error {
				stm.Put(entity.DBKey(tableRecord.DbId), cast.ToString(tableRecord.DbId))
				stm.Put(entity.TableKey(tableRecord.DbId, tableRecord.TableId), string(tableBytes))

				if createBytes, err := json.Marshal(createRecord); err != nil {
					return err
				} else {
					stm.Put(entity.CreateKey(createID), string(createBytes))
				}
				return nil
			})
			if err != nil {
				return cblog.LogErrAndReturn(err)
			}
		}
	} else {
		if createBytes, err := json.Marshal(createRecord); err != nil {
			return cblog.LogErrAndReturn(err)
		} else {
			if err := cs.Put(ctx, entity.CreateKey(createID), createBytes); err != nil {
				return cblog.LogErrAndReturn(err)
			}
		}
	}

	defer func() {
		if tableRecord != nil && tableRecord.State != entity.RecordState_Created {
			log.Error("failed to create table/index, dbId=[%d] tableId=[%d]", req.DbId, req.TableId)
			tableRecord.State = entity.RecordState_Failed
			if err = cs.updateTableRecord(ctx, tableRecord, req.DbId, req.TableId); err != nil {
				log.Error("failed to store RecordState_Failed for table/index, dbId=[%d] tableId=[%d]", req.DbId, req.TableId)
			}
		}

		if createRecord.CreateState != entity.RecordState_Created {
			createRecord.CreateState = entity.RecordState_Failed
			if createBytes, err := json.Marshal(createRecord); err != nil {
				log.Error("failed to marshal for CreateRecord, dbId=[%d] tableId=[%d]", req.DbId, req.TableId)
			} else {
				if err := cs.Update(ctx, entity.CreateKey(createID), createBytes); err != nil {
					log.Error("failed to store RecordState_Failed for CreateRecord, dbId=[%d] tableId=[%d]", req.DbId, req.TableId)
				}
			}
		}
	}()

	for _, rng := range req.Ranges {
		if rng.StoreType == basepb.StoreType_Store_Hot {
			targetNodes, err := cs.selectRangePeers(ctx, hot, int(req.Replicas), rng)
			if err != nil {
				log.Error("select hot nodes for range[%d] table[%d] db[%d] error", rng.Id, rng.TableId, rng.DbId)
				return cblog.LogErrAndReturn(err)
			}
			cs.createRange(targetNodes, errChain, ctx, rng)
		} else if rng.StoreType == basepb.StoreType_Store_Warm {
			targetNodes, err := cs.selectRangePeers(ctx, warm, int(req.Replicas), rng)
			if err != nil {
				log.Error("select warn nodes for range[%d] table[%d] db[%d] error", rng.Id, rng.TableId, rng.DbId)
				return err
			}
			cs.createRange(targetNodes, errChain, ctx, rng)
		} else {
			return fmt.Errorf("err type of range:[%d]", rng.StoreType)
		}
	}

	if err = cs.checkRangeOk(ctx, req.Ranges, errChain); err != nil {
		log.Error("checkRange finally err[%v], table[%d] db[%d]", err, req.TableId, req.DbId)
		return cblog.LogErrAndReturn(err)
	}

	err = cs.STM(context.Background(), func(stm concurrency.STM) error {
		if tableRecord != nil {
			tableRecord.State = entity.RecordState_Created
			if err := cs.updateTableRecord(ctx, tableRecord, req.DbId, req.TableId); err != nil {
				log.Error("failed to store RecordState_Created for table/index, dbId=[%d] tableId=[%d]", req.DbId, req.TableId)
				return err
			}
		}

		createRecord.CreateState = entity.RecordState_Created
		if createBytes, err := json.Marshal(createRecord); err != nil {
			return err
		} else {
			stm.Put(entity.CreateKey(createID), string(createBytes))
		}
		return nil
	})
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	log.Info("successfully created table/index, dbId=[%d] tableId=[%d]", req.DbId, req.TableId)
	return nil
}

func (cs *BaseService) DeleteRanges(ctx context.Context, req *mspb.DeleteRangesRequest) error {
	if req.DbId <= 0 {
		return fmt.Errorf("invalid dbId[%d]", req.DbId)
	}

	if req.TableId <= 0 {
		// delete db
		log.Info("received request dbId[%d] with tableId[%d], will delete the whole db.", req.DbId, req.TableId)
		dbID, qErr := cs.QueryDBByID(ctx, req.DbId)
		if qErr != nil && qErr != errs.Error(mspb.ErrorType_NotExistDatabase) {
			return cblog.LogErrAndReturn(qErr)
		}

		if qErr != errs.Error(mspb.ErrorType_NotExistDatabase) {
			if dbID != req.DbId {
				return fmt.Errorf("req's dbId[%d] not match its value that stored in master[%d]", req.DbId, dbID)
			}
		}

		//find db has range
		lock := cs.NewLock(ctx, entity.LockDBKey(req.DbId), time.Minute*5)
		if err := lock.Lock(); err != nil {
			return cblog.LogErrAndReturn(err)
		}
		defer func() {
			if err := lock.Unlock(); err != nil {
				log.Error("failed to unlock db:[%d] err:[%s] ", req.DbId, err.Error())
			}
		}()

		tables, err := cs.QueryTablesByDBID(ctx, req.DbId)
		if err != nil {
			return cblog.LogErrAndReturn(err)
		}
		for _, t := range tables {
			if t.DbId != req.DbId {
				continue
			}
			cs.doDsDeleteRange(t.TableId, -1)

			// delete table
			if tableRecord, err := cs.QueryTableByID(ctx, req.DbId, t.TableId); err != nil && err != errs.Error(mspb.ErrorType_NotExistTable) {
				return cblog.LogErrAndReturn(err)
			} else if tableRecord == nil || err == errs.Error(mspb.ErrorType_NotExistTable) {
				continue
			}
			if err := cs.Delete(ctx, entity.TableKey(req.DbId, t.TableId)); err != nil {
				return cblog.LogErrAndReturn(err)
			}
		}

		if qErr != errs.Error(mspb.ErrorType_NotExistDatabase) {
			if err = cs.Delete(ctx, entity.DBKey(req.DbId)); err != nil {
				return cblog.LogErrAndReturn(err)
			}
		}
	} else {
		//to lock table
		lock := cs.NewLock(ctx, entity.LockTableKey(req.DbId, req.TableId), time.Minute*5)
		if err := lock.Lock(); err != nil {
			return cblog.LogErrAndReturn(err)
		}
		defer func() {
			if err := lock.Unlock(); err != nil {
				log.Error("failed to unlock table by dbId[%d] tableId[%d] err:[%s] ", req.DbId, req.TableId, err.Error())
			}
		}()

		// delete index
		if req.RangeType == basepb.RangeType_RNG_Index && req.IndexId > 0 {
			cs.doDsDeleteRange(req.TableId, req.IndexId)
			return nil
		}

		cs.doDsDeleteRange(req.TableId, -1)

		// delete table
		if tableRecord, err := cs.QueryTableByID(ctx, req.DbId, req.TableId); err != nil && err != errs.Error(mspb.ErrorType_NotExistTable) {
			return cblog.LogErrAndReturn(err)
		} else if tableRecord == nil || err == errs.Error(mspb.ErrorType_NotExistTable) {
			return nil
		}

		if err := cs.Delete(ctx, entity.TableKey(req.DbId, req.TableId)); err != nil {
			return cblog.LogErrAndReturn(err)
		}
	}

	return nil
}

func (cs *BaseService) doDsDeleteRange(tableId, indexId int32) {
	go func() {
		background, cancel := context.WithTimeout(context.Background(), time.Minute*5)
		defer cancel()

		log.Info("to delete ranges in background, tableId[%d] indexId[%d]", tableId, indexId)
		ranges, err := cs.QueryRanges(background, tableId, indexId)
		if err != nil {
			log.Error("del table to query ranges err:[%s]", err.Error())
			return
		}
		for _, rng := range ranges {
			if queryRng, _ := cs.QueryRange(background, tableId, rng.Id); queryRng == nil {
				continue
			}
			for _, pr := range rng.Peers {
				log.Info("delete peer:[%d] in node:[%d]", pr.Id, pr.NodeId)
				if node, err := cs.GetNode(background, pr.NodeId); err != nil {
					log.Error("get node err:[%s]", err.Error())
				} else {
					if err := cs.dsClient.DeleteRange(background, NodeServerAddr(node), rng.Id, pr.Id); err != nil {
						log.Error("delete range to node err:[%s]", err.Error())
					}
				}
			}

			_ = cs.Delete(background, entity.RangeKey(tableId, rng.Id))
			if indexId > 0 {
				_ = cs.Delete(background, entity.IndexRangeKey(tableId, indexId, rng.Id))
			}
		}
	}()
}

func (cs *BaseService) selectRangePeers(ctx context.Context, nodes []*basepb.Node, replicas int, rng *basepb.Range) ([]*basepb.Node, error) {
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	targetNodes := nodes[:replicas]

	var peers []*basepb.Peer
	for _, node := range targetNodes {
		peerID, err := cs.NewIDGenerate(ctx, entity.SequencePeerID, 1, 5*time.Second)
		if err != nil {
			return nil, cblog.LogErrAndReturn(err)
		}

		node.RangePeers = append(node.RangePeers, &basepb.RangePeer{RangeId: rng.Id, PeerId: uint64(peerID)})
		peers = append(peers, &basepb.Peer{
			Id:     uint64(peerID),
			NodeId: node.Id,
			Type:   basepb.PeerType_PeerType_Normal,
		})
	}

	if len(peers) == 0 {
		return nil, fmt.Errorf("peer length of range is 0, can't create range by dbID=[%d] tableId=[%d]", rng.DbId, rng.TableId)
	}
	rng.Peers = peers
	rng.Leader = peers[0].NodeId

	return targetNodes, nil
}

func (cs *BaseService) updateTableRecord(ctx context.Context, tableRecord *entity.TableRecord, dbId, tableId int32) error {
	if tableBytes, err := json.Marshal(tableRecord); err != nil {
		return cblog.LogErrAndReturn(err)
	} else {
		if err = cs.Update(ctx, entity.TableKey(dbId, tableId), tableBytes); err != nil {
			tableRecord.State = entity.RecordState_Failed
			return cblog.LogErrAndReturn(err)
		}
	}

	return nil
}
