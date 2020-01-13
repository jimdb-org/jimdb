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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	client "github.com/jimdb-org/jimdb/master/client/ds_client"
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/errs"
	"github.com/jimdb-org/jimdb/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/master/entity/pkg/dspb"
	"github.com/jimdb-org/jimdb/master/entity/pkg/mspb"
	unsafeBytes "github.com/jimdb-org/jimdb/master/utils/bytes"
	"github.com/jimdb-org/jimdb/master/utils/cblog"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"go.etcd.io/etcd/clientv3/concurrency"
	"math"
	"math/rand"
	"sort"
	"time"
)

func (rs *BaseService) ChangeRangeStoreType(ctx context.Context, tableID int32, rangeID uint64, storeType basepb.StoreType) error {
	return rs.STM(ctx, func(stm concurrency.STM) error {

		dbRange := new(basepb.Range)

		realBytes := stm.Get(entity.RangeKey(tableID, rangeID))

		if len(realBytes) == 0 {
			return cblog.LogErrAndReturn(errs.Error(mspb.ErrorType_NotExistRange))
		}

		if err := dbRange.Unmarshal([]byte(realBytes)); err != nil {
			return cblog.LogErrAndReturn(err)
		}

		if dbRange.StoreType == storeType {
			return nil
		}

		dbRange.StoreType = storeType

		rgBytes, err := dbRange.Marshal()
		if err != nil {
			return cblog.LogErrAndReturn(err)
		}

		stm.Put(entity.RangeKey(tableID, rangeID), unsafeBytes.ByteToString(rgBytes))

		return nil
	})
}

// return true for update , false is not write
func (rs *BaseService) Heartbeat(ctx context.Context, rg *basepb.Range) (*basepb.Range, error) {

	var rng *basepb.Range

	err := rs.STM(ctx, func(stm concurrency.STM) error {
		dbRange := new(basepb.Range)

		realBytes := stm.Get(entity.RangeKey(rg.TableId, rg.Id))

		if len(realBytes) > 0 {
			if err := dbRange.Unmarshal([]byte(realBytes)); err != nil {
				return cblog.LogErrAndReturn(err)
			}
			rg.StoreType = dbRange.StoreType //use dbRange replace store type

			if dbRange.RangeEpoch != nil && (dbRange.RangeEpoch.ConfVer > rg.RangeEpoch.ConfVer || dbRange.RangeEpoch.Version > rg.RangeEpoch.Version || dbRange.Term > rg.Term) {
				rng = dbRange
				return nil
			}

			if dbRange.RangeEpoch != nil && (dbRange.RangeEpoch.ConfVer == rg.RangeEpoch.ConfVer && dbRange.RangeEpoch.Version == rg.RangeEpoch.Version && dbRange.Term == rg.Term) {
				if len(rg.Peers) == len(dbRange.Peers) {
					sort.Slice(rg.Peers, func(i, j int) bool {
						return rg.Peers[i].Id < rg.Peers[j].Id
					})

					sort.Slice(dbRange.Peers, func(i, j int) bool {
						return dbRange.Peers[i].Id < dbRange.Peers[j].Id
					})

					var flag bool
					for i, rP := range rg.Peers {
						dP := dbRange.Peers[i]
						if rP.Id != dP.Id || rP.Type != dP.Type {
							flag = true
							break
						}
					}
					if !flag {
						rng = dbRange
						return nil
					}
				}
			}

		} else if rg.ParentRangeId > 0 {
			parentRange := new(basepb.Range)
			parentBytes := stm.Get(entity.RangeKey(rg.TableId, rg.ParentRangeId))
			if len(parentBytes) > 0 {
				if err := parentRange.Unmarshal([]byte(parentBytes)); err != nil {
					_ = cblog.LogErrAndReturn(fmt.Errorf("parent range unmarshal has err:[%s]", err.Error()))
				} else {
					rg.StoreType = parentRange.StoreType //use parentRange replace store type
				}
			} else {
				_ = cblog.LogErrAndReturn(fmt.Errorf("impossibility not found parent range:[%d]", rg.ParentRangeId))
			}

		}

		rgBytes, err := rg.Marshal()

		if err != nil {
			return cblog.LogErrAndReturn(err)
		}

		if log.IsDebugEnabled() {
			newrb, _ := json.Marshal(rg)
			var oldrb = []byte{}
			if dbRange != nil {
				oldrb, _ = json.Marshal(dbRange)
			}
			log.Info("to update range to db, old:[%s] new:[%s]", string(oldrb), string(newrb))
		}

		rng = rg
		stm.Put(entity.RangeKey(rg.TableId, rg.Id), unsafeBytes.ByteToString(rgBytes))
		if rg.RangeType == basepb.RangeType_RNG_Index {
			stm.Put(entity.IndexRangeKey(rg.TableId, rg.IndexId, rg.Id), unsafeBytes.ByteToString(rgBytes))
		}
		return nil
	})
	return rng, cblog.LogErrAndReturn(err)
}

func (rs *BaseService) AskSplit(ctx context.Context, rng *basepb.Range, force bool) (uint64, []uint64, error) {

	if !rs.ConfigAutoSplit(ctx) {
		log.Warn("auto split is close , so return err:[%s]", errs.Error(mspb.ErrorType_NotAllowSplit))
		return 0, nil, cblog.LogErrAndReturn(errs.Error(mspb.ErrorType_NotAllowSplit))
	}

	table, err := rs.QueryTableByID(ctx, rng.DbId, rng.TableId)
	if err != nil {
		log.Warn("dbID:[%d] tableID:[%d] range:[%d] skip err:[%s]", rng.DbId, rng.TableId, rng.Id, err.Error())
		return 0, nil, cblog.LogErrAndReturn(err)
	}

	// if table status not runing so skip split
	if table == nil || table.State != entity.RecordState_Created {
		log.Warn("table:[%s/%d] is nil or not finished yet, so skip split", table.DbId, table.TableId)
		return 0, nil, cblog.LogErrAndReturn(errs.Error(mspb.ErrorType_NotAllowSplit))
	}

	oldRange, err := rs.QueryRange(ctx, rng.TableId, rng.Id)
	if err != nil {
		log.Warn("dbID:[%d] tableID:[%d] range:[%d] skip err:[%s]", rng.DbId, rng.TableId, rng.Id, err.Error())
		return 0, nil, cblog.LogErrAndReturn(err)
	}

	if rng.GetRangeEpoch().GetConfVer() == oldRange.GetRangeEpoch().GetConfVer() && rng.GetRangeEpoch().GetVersion() == oldRange.GetRangeEpoch().GetVersion() {
		if bytes.Compare(rng.GetStartKey(), oldRange.GetStartKey()) != 0 || bytes.Compare(rng.GetEndKey(), oldRange.GetEndKey()) != 0 {
			err = errs.Error(mspb.ErrorType_RangeMetaConflict)
			log.Error("range[%v] meta abnormal[%v] ", rng, oldRange)
			return 0, nil, cblog.LogErrAndReturn(err)
		}
	}

	var snapPeers []*basepb.Peer

	for _, p := range rng.Peers {
		if p.Type == basepb.PeerType_PeerType_Learner {
			snapPeers = append(snapPeers, p)
		}
	}

	//if len(rng.Peers)-len(snapPeers) < int(table.ReplicaNum) {
	//	return 0, nil, cblog.LogErrAndReturn(fmt.Errorf("range:[%d] has not health peer:[%d] snapshot:[%d] but need:[%d]", rng.Id, len(rng.Peers), len(snapPeers), table.ReplicaNum))
	//}

	for _, sp := range snapPeers {
		if err := rs.SyncDelMember(ctx, rng, sp.Id); err != nil {
			return 0, nil, cblog.LogErrAndReturn(err)
		}

		newPeers := make([]*basepb.Peer, 0, len(rng.Peers)-1)

		for _, p := range rng.Peers {
			if p.Id != sp.Id {
				newPeers = append(newPeers, p)
			}
		}
		rng.Peers = newPeers
	}

	newRangeID, err := rs.NewIDGenerate(ctx, entity.SequenceRangeID, 1, 5*time.Second)
	if err != nil {
		return 0, nil, cblog.LogErrAndReturn(err)
	}

	newPeerIDs := make([]uint64, len(rng.Peers))
	for i := range newPeerIDs {
		peerID, err := rs.NewIDGenerate(ctx, entity.SequencePeerID, 1, 5*time.Second)
		if err != nil {
			return 0, nil, cblog.LogErrAndReturn(err)
		}
		newPeerIDs[i] = uint64(peerID)
	}

	log.Info("ask split return  rangeID:[%d] peerIDs:[%v]", newRangeID, newPeerIDs)

	return uint64(newRangeID), newPeerIDs, nil
}

// if tableID ==0 it will return all ranges
func (rs *BaseService) GetRoute(ctx context.Context, max int, dbID, tableID int32, key []byte, hasAll bool) ([]*basepb.Range, error) {

	if dbID > 0 { //check db exist
		if _, err := rs.QueryDBByID(ctx, dbID); err != nil {
			return nil, err
		}
	}

	if tableID > 0 { //check table exist
		table, err := rs.QueryTableByID(ctx, dbID, tableID)
		if err != nil {
			return nil, err
		}
		if table == nil || table.State != entity.RecordState_Created {
			return nil, errs.Error(mspb.ErrorType_NotExistTable)
		}
	}

	ranges, err := rs.QueryRanges(ctx, tableID, -1)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	if !hasAll {
		for _, r := range ranges {
			r.Peers = nil
			r.KeySchema = nil
		}
	}

	sort.Slice(ranges, func(i, j int) bool {
		return bytes.Compare(ranges[i].StartKey, ranges[j].StartKey) >= 0
	})

	var result []*basepb.Range
	if max <= 0 {
		result = make([]*basepb.Range, 0, 100)
	} else {
		result = make([]*basepb.Range, 0, int(math.Min(float64(max), 100)))
	}

	for _, rng := range ranges {
		if len(key) == 0 || bytes.Compare(key, rng.StartKey) >= 0 {
			result = append(result, rng)
		}

		if max > 0 && len(result) >= max {
			return result, nil
		}

	}

	return result, nil
}

func (ns *BaseService) CreateRangeToNode(ctx context.Context, node *basepb.Node, old *basepb.Range) error {
	log.Info("create range to node [%d] range:[%d]", node.Id, old.Id)

	rng, err := ns.QueryRange(ctx, old.TableId, old.Id)
	if err != nil {
		return err
	}

	if rng.RangeEpoch.Version != old.RangeEpoch.Version || rng.RangeEpoch.ConfVer != old.RangeEpoch.ConfVer {
		return cblog.LogErrAndReturn(fmt.Errorf("create range has err the range:[%d] version[%d, %d] old:[%d, %d] not same so skip ", rng.Id, rng.RangeEpoch.Version, rng.RangeEpoch.ConfVer, old.RangeEpoch.Version, old.RangeEpoch.ConfVer))
	}

	for _, peer := range rng.Peers {
		if peer.NodeId == node.Id {
			return nil
		}
	}

	leader, err := ns.GetNode(ctx, rng.Leader)
	if err != nil {
		return err
	}

	peerID, err := ns.NewIDGenerate(ctx, entity.SequencePeerID, 1, 5*time.Second)
	if err != nil {
		return err
	}

	newPeer := &basepb.Peer{
		Id:     uint64(peerID),
		NodeId: node.Id,
		Type:   basepb.PeerType_PeerType_Learner,
	}

	tryTime := 0

	log.Info("change member by range:[%d] newPeer:[%d]", rng.Id, newPeer.Id)
	err = ns.dsClient.ChangeMember(ctx, NodeServerAddr(leader), &dspb.ChangeRaftMemberRequest{
		RangeId:    rng.Id,
		RangeEpoch: rng.RangeEpoch,
		ChangeType: dspb.ChangeRaftMemberRequest_CT_ADD,
		TargetPeer: newPeer,
	})

	if err != nil {
		log.Error("add memeber:[%d] err:", newPeer)
		return err
	}

	for !hasPeer(rng, newPeer.Id) {

		tryTime++
		if tryTime > 60 {
			return fmt.Errorf("create memeber too long times ")
		}
		time.Sleep(500 * time.Millisecond)
		if rng, err = ns.QueryRange(ctx, old.TableId, old.Id); err != nil {
			return err
		}
	}

	return ns.dsClient.CreateRange(ctx, NodeServerAddr(node), rng)
}

func (ns *BaseService) SyncDelMember(ctx context.Context, old *basepb.Range, peerID uint64) error {
	log.Info("del member to rangeID:[%d] peerID:[%d]", old.Id, peerID)

	rng, err := ns.QueryRange(ctx, old.TableId, old.Id)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	if rng.RangeEpoch.Version < old.RangeEpoch.Version || rng.RangeEpoch.ConfVer < old.RangeEpoch.ConfVer {
		return cblog.LogErrAndReturn(fmt.Errorf("del range has err the version[%d, %d] old:[%d, %d] not same so skip ", rng.RangeEpoch.Version, rng.RangeEpoch.ConfVer, old.RangeEpoch.Version, old.RangeEpoch.ConfVer))
	}

	//remove peers to all
	newPeers := make([]*basepb.Peer, 0, len(rng.Peers)-1)
	var removePeer *basepb.Peer
	for _, p := range rng.Peers {
		if p.Id != peerID {
			newPeers = append(newPeers, p)
		} else {
			removePeer = p
		}
	}

	if removePeer == nil {
		log.Info("peer[%d] not in range peers:[%v] so skip ", peerID, rng.Peers)
		return nil
	}

	//check changeMeber peer is leader?
	tryTime := 0

	wantLeaderNode, err := ns.QueryNode(ctx, newPeers[rand.Intn(len(newPeers))].NodeId)
	if err != nil {
		log.Error("query node has err :[%s]", err.Error())
		return err
	}
	for removePeer.NodeId == rng.Leader {
		log.Info("remove peer is leader so to transfer leader")

		if err = ns.TransferLeader(ctx, wantLeaderNode, rng); err != nil {
			log.Error("transfer leader has err :[%s]", err.Error())
			return err
		}
		tryTime++
		if tryTime > 60 {
			return fmt.Errorf("transferLeader too long times ")
		}
		time.Sleep(1 * time.Second)
		if rng, err = ns.QueryRange(ctx, old.TableId, old.Id); err != nil {
			return err
		}
	}

	tryTime = 0
	for hasPeer(rng, removePeer.Id) {
		log.Info("change member by range:[%d] removePeer:[%d]", rng.Id, removePeer.Id)
		leader, err := ns.GetNode(ctx, rng.Leader)
		if err != nil {
			return cblog.LogErrAndReturn(err)
		}
		err = ns.dsClient.ChangeMember(ctx, NodeServerAddr(leader), &dspb.ChangeRaftMemberRequest{
			RangeId:    rng.Id,
			RangeEpoch: rng.RangeEpoch,
			ChangeType: dspb.ChangeRaftMemberRequest_CT_REMOVE,
			TargetPeer: removePeer,
		})

		if err != nil {
			return cblog.LogErrAndReturn(err)
		}
		tryTime++
		if tryTime > 60 {
			return fmt.Errorf("del memeber too long times ")
		}
		time.Sleep(500 * time.Millisecond)
		if rng, err = ns.QueryRange(ctx, old.TableId, old.Id); err != nil {
			return err
		}
	}

	return nil
}

func hasPeer(rng *basepb.Range, removePeer uint64) bool {
	for _, p := range rng.Peers {
		if p.Id == removePeer {
			return true
		}
	}
	return false
}

func (ns *BaseService) SyncDeleteRangeToNode(ctx context.Context, old *basepb.Range, peerID uint64, removeNodeID uint64) error {
	log.Info("delete range to rangeID:[%d] peerID:[%d]", old.Id, peerID)

	rng, err := ns.QueryRange(ctx, old.TableId, old.Id)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	if rng.RangeEpoch.Version < old.RangeEpoch.Version || rng.RangeEpoch.ConfVer < old.RangeEpoch.ConfVer {
		return cblog.LogErrAndReturn(fmt.Errorf("del range has err the version[%d, %d] old:[%d, %d] not same so skip ", rng.RangeEpoch.Version, rng.RangeEpoch.ConfVer, old.RangeEpoch.Version, old.RangeEpoch.ConfVer))
	}

	if err := ns.SyncDelMember(ctx, old, peerID); err != nil {
		return cblog.LogErrAndReturn(err)
	}

	if removeNode, err := ns.QueryNode(ctx, removeNodeID); err != nil {
		log.Error("query node has err :[%s]", err.Error())
	} else {
		if err := ns.dsClient.DeleteRange(ctx, NodeServerAddr(removeNode), rng.Id, peerID); err != nil {
			log.Warn("delete range by node:[%d] err :[%s]", removeNode.Id, err.Error())
			return err
		} else {
			log.Info("delete range by node:[%d] peerID:[%d] ok", removeNode.Id, peerID)
		}
	}

	return nil

}

func (rs *BaseService) TransferLeader(ctx context.Context, node *basepb.Node, old *basepb.Range) error {
	log.Info("transfer leader for rangeID:[%d] to node:[%d]", old.Id, node.Id)

	table, err := rs.QueryTableByID(ctx, old.DbId, old.TableId)

	rng, err := rs.QueryRange(ctx, old.TableId, old.Id)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	if rng.StoreType != node.Type {
		return cblog.LogErrAndReturn(fmt.Errorf("range:[%d] can not transferLeader because type is not equal[%v, %v]", rng.Id, rng.StoreType, node.Type))
	}

	if len(rng.Peers) < int(table.Replicas) {
		return cblog.LogErrAndReturn(fmt.Errorf("range:[%d] transferLeader check err replica[%d / %d] num not same ", rng.Id, len(rng.Peers), table.Replicas))
	}

	if rng.RangeEpoch.Version < old.RangeEpoch.Version || rng.RangeEpoch.ConfVer < old.RangeEpoch.ConfVer {
		return cblog.LogErrAndReturn(fmt.Errorf("transfer range has err the version[%d, %d] old:[%d, %d] not same so skip ", rng.RangeEpoch.Version, rng.RangeEpoch.ConfVer, old.RangeEpoch.Version, old.RangeEpoch.ConfVer))
	}

	toNode, err := rs.GetNode(ctx, node.Id)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	if toNode.State != basepb.NodeState_N_Online {
		return cblog.LogErrAndReturn(fmt.Errorf("transfer leader err because to node:[%d] state is:[%v]", toNode.Id, toNode.State))
	}

	leader, err := rs.GetNode(ctx, old.Leader)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}
	nodeInfo, err := rs.NodeInfo(ctx, leader)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	for _, ri := range nodeInfo.RangeInfos {
		if ri.Range.Id == old.Id {
			for _, ps := range ri.PeersStatus {
				if ps.Snapshotting || ps.DownSeconds > 0 || ps.Peer.Type != basepb.PeerType_PeerType_Normal {
					return cblog.LogErrAndReturn(fmt.Errorf("%s because it snapshotting", errs.Error(mspb.ErrorType_NotAllowSplit).Error()))
				}
			}
		}
	}

	return rs.dsClient.TransferLeader(ctx, NodeServerAddr(node), old.Id)
}

func (ns *BaseService) DsClient() client.SchClient {
	return ns.dsClient
}

func (ns *BaseService) AdmClient() client.AdminClient {
	return ns.admClient
}
