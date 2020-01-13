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
	"fmt"
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/errs"
	"github.com/jimdb-org/jimdb/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/master/entity/pkg/dspb"
	"github.com/jimdb-org/jimdb/master/entity/pkg/mspb"
	"github.com/jimdb-org/jimdb/master/utils/hack"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type NodeHandlerMap map[uint64]*NodeHandler

//find can arrive min range number  node
func (maps NodeHandlerMap) MinArriveNodeByRange(storeType basepb.StoreType, rng *basepb.Range) (result *NodeHandler, err error) {

	alreadyIn := func(rng *basepb.Range, nodeID uint64) bool {
		for _, ps := range rng.Peers {
			if ps.NodeId == nodeID {
				return true
			}
		}
		return false
	}

	if storeType == basepb.StoreType_Store_Mix {
		if rng.RangeType == basepb.RangeType_RNG_Index {
			storeType = basepb.StoreType_Store_Hot
		} else {
			storeType = basepb.StoreType_Store_Warm
		}
	}

	for nodeID, nh := range maps {

		if nh.Type != storeType {
			continue
		}

		if alreadyIn(rng, nodeID) {
			continue
		}

		if nh.State != basepb.NodeState_N_Online {
			continue
		}

		//check memory size
		if err = nh.CheckMemory(); err != nil {
			log.Error(err.Error())
			continue
		}

		if result == nil || len(result.RangeHanders) >= len(nh.RangeHanders) {
			result = nh
		}
	}

	if result == nil {
		return nil, errs.Error(mspb.ErrorType_NodeNotEnough)
	}

	return result, nil
}

func (maps NodeHandlerMap) SortRangesByType(storeType basepb.StoreType) []*NodeHandler {

	result := make([]*NodeHandler, 0)

	for _, nh := range maps {
		if nh.Type != storeType {
			continue
		}

		result = append(result, nh)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RangeNum < result[j].RangeNum
	})

	return result

}

func (maps NodeHandlerMap) SortLeadersNum(storeType basepb.StoreType) []*NodeHandler {

	result := make([]*NodeHandler, 0)

	for _, nh := range maps {
		if storeType == 0 || storeType == nh.Type {
			result = append(result, nh)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LeaderNum < result[j].LeaderNum
	})

	return result

}

func (rh *RangeHandler) CanChangeLeader(handlerMap NodeHandlerMap) error {
	return rh.canChange(handlerMap, math.MaxInt32)
}

func (rh *RangeHandler) CanCreateRange(handlerMap NodeHandlerMap) error {
	return rh.canChange(handlerMap, 5)
}

func (rh *RangeHandler) CanDeleteRange(handlerMap NodeHandlerMap) error {
	return rh.canChange(handlerMap, math.MaxInt32)
}

func (rh *RangeHandler) canChange(handlerMap NodeHandlerMap, minSnapNum int) error {
	tmp := rh

	nh := handlerMap[tmp.Leader]
	if nh == nil {
		return fmt.Errorf("can not change range[%d] by peer:[%d] status because leader node:[%d] not found ", tmp.Id, tmp.Peer.NodeId, tmp.Leader)
	}

	if !tmp.IsLeader {
		tmp = nh.RangeHanders[tmp.Id]

		if tmp == nil || !tmp.IsLeader {
			return fmt.Errorf("can not change range[%d] by peer:[%d] status because leaderNode:[%d] is not leader ", tmp.Id, tmp.Peer.NodeId, tmp.Leader)
		}

	}
	for _, pr := range tmp.PeersStatus {
		if nh.OutSnapshot > minSnapNum {
			return fmt.Errorf("range:[%d] has snapshotting peer_num:[%d] more than:[%d]  so skip change ", tmp.Id, nh.OutSnapshot, minSnapNum)
		}
		if pr.Peer.Type != basepb.PeerType_PeerType_Normal {
			return fmt.Errorf("range:[%d] has not normal type peer so skip change ", tmp.Id)
		}
		if pr.DownSeconds > 0 {
			return fmt.Errorf("range:[%d] has down peer so skip change ", tmp.Id)
		}
	}
	return nil
}

type RangeHandler struct {
	*basepb.Range
	IsLeader    bool
	Peer        *basepb.Peer
	PeersStatus []*basepb.PeerStatus
	Status      *dspb.RangeStats
	Term        uint64
}

func (rh RangeHandler) MinNumberPeer(nMap NodeHandlerMap) (*basepb.Peer, error) {
	if !rh.IsLeader {
		return nil, fmt.Errorf("min number range:[%d] only can found in leader", rh.Leader)
	}
	nhs := make([]*NodeHandler, len(rh.Peers))

	for i := 0; i < len(rh.Peers); i++ {
		if nh := nMap[rh.Peers[i].NodeId]; nh == nil {
			log.Error("not found peer by node:[%d]", rh.Peers[i].Id)
			return nil, errs.Error(mspb.ErrorType_NotExistNode)
		} else {
			nhs[i] = nh
		}
	}

	sort.Slice(nhs, func(i, j int) bool {
		v := nhs[i].RangeNum - nhs[j].RangeNum
		if v < 0 {
			return true
		} else if v > 0 {
			return false
		} else {
			if nhs[i].GetRH(rh.Id).IsLeader {
				return false
			}
			return true
		}
	})

	return nhs[0].GetRH(rh.Id).Peer, nil
}

func (rh RangeHandler) MaxNumberPeer(nMap NodeHandlerMap) (*basepb.Peer, error) {
	if !rh.IsLeader {
		return nil, fmt.Errorf("min number range:[%d] only can found in leader", rh.Leader)
	}
	nhs := make([]*NodeHandler, len(rh.Peers))

	for i := 0; i < len(rh.Peers); i++ {
		if nh := nMap[rh.Peers[i].NodeId]; nh == nil {
			log.Error("not found peer by node:[%d]", rh.Peers[i].Id)
			return nil, errs.Error(mspb.ErrorType_NotExistNode)
		} else {
			nhs[i] = nh
		}
	}

	sort.Slice(nhs, func(i, j int) bool {
		v := nhs[i].RangeNum - nhs[j].RangeNum
		if v < 0 {
			return true
		} else if v > 0 {
			return false
		} else {
			if nhs[i].GetRH(rh.Id).IsLeader {
				return false
			}
			return true
		}
	})

	return nhs[len(nhs)-1].GetRH(rh.Id).Peer, nil
}

func (rh *RangeHandler) VersionEqual(to *RangeHandler) bool {
	if rh.RangeEpoch.Version == to.RangeEpoch.Version && to.RangeEpoch.ConfVer == to.RangeEpoch.ConfVer {
		return true
	}
	return false
}

func (rh *RangeHandler) Compare(o *RangeHandler) int {
	if rh.RangeEpoch.Version < o.RangeEpoch.Version || rh.RangeEpoch.ConfVer < o.RangeEpoch.ConfVer || rh.Term < o.Term {
		return -1
	}

	if rh.RangeEpoch.Version > o.RangeEpoch.Version || rh.RangeEpoch.ConfVer > o.RangeEpoch.ConfVer || rh.Term > o.Term {
		return 1
	}

	return 0
}

type NodeHandler struct {
	*basepb.Node
	RangeHanders map[uint64]*RangeHandler
	Stats        *dspb.NodeStats
	RangeNum     int
	LeaderNum    int
	OutSnapshot  int
	InSnapshot   int
}

func (nh *NodeHandler) GetRH(rangeID uint64) *RangeHandler {
	handler, _ := nh.GetRangeHandler(rangeID)
	return handler
}

func (nh *NodeHandler) GetRangeHandler(rangeID uint64) (*RangeHandler, error) {
	handler := nh.RangeHanders[rangeID]
	if handler != nil {
		return handler, nil
	}
	return nil, errs.Error(mspb.ErrorType_NotExistRange)
}

func (nh *NodeHandler) CheckMemory() error {
	if float64(nh.Stats.UsedSize)/float64(nh.Stats.Capacity) >= entity.Conf().Global.MemoryRatio {
		return fmt.Errorf("node:[%d] used memory:[%d] gather config memory_ration[%d] so skip check", nh.Id, float64(nh.Stats.UsedSize)/float64(nh.Stats.Capacity), entity.Conf().Global.MemoryRatio)
	}
	return nil
}

// only get target node , if targetNode is nil , return all
func (ps *BaseService) QueryNodeHandlerMap(ctx context.Context, targetNode map[uint64]bool) (map[uint64]*basepb.Node, map[uint64]*NodeHandler, error) {
	nodeMap := make(map[uint64]*basepb.Node)
	nodeHandlerMap := make(map[uint64]*NodeHandler)
	nodes, err := ps.QueryOnlineNodes(ctx)
	endNodeNum := hack.PInt64(int64(len(nodes)))
	if err != nil {
		log.Error("query all nodes err:[%s]", err)
		return nil, nil, err
	}

	lock := sync.Mutex{}
	for _, node := range nodes {
		if targetNode != nil && !targetNode[node.Id] {
			continue
		}
		nodeMap[node.Id] = node
		go func(n *basepb.Node) {
			defer func() {
				if r := recover(); r != nil {
					log.Error("get nodeInfo [%d]:[%s] err:[%v]", n.Id, NodeServerAddr(n), r)
				}
				atomic.AddInt64(endNodeNum, -1)
			}()
			if nodeInfo, err := ps.NodeInfo(ctx, n); err != nil {
				log.Error("get nodeInfo [%d]:[%s] err:[%s]", n.Id, NodeServerAddr(n), err.Error())
			} else {
				nh := &NodeHandler{
					Node:         n,
					Stats:        nodeInfo.Stats,
					RangeHanders: make(map[uint64]*RangeHandler),
				}
				lock.Lock()
				nodeHandlerMap[n.Id] = nh
				lock.Unlock()

				for _, ri := range nodeInfo.RangeInfos {
					rh := &RangeHandler{
						Range:       ri.Range,
						IsLeader:    ri.Range.Leader == nh.Id,
						PeersStatus: ri.PeersStatus,
						Status:      ri.Stats,
						Term:        ri.Term,
					}

					if rh.IsLeader {
						nh.LeaderNum++
					}

					for _, peer := range ri.Range.Peers {
						if peer.NodeId == nh.Id {
							rh.Peer = peer
						}
					}

					if rh.Peer == nil {
						log.Error("impossibility range:[%d] peer not in range Term:[%d]", rh.Id, ri.Term)
						rh.Peer = &basepb.Peer{Id: 0, NodeId: nh.Id, Type: basepb.PeerType_PeerType_Invalid}
					}

					nh.RangeHanders[ri.Range.Id] = rh
				}
				nh.RangeNum = len(nh.RangeHanders)

			}
		}(node)
	}

	for atomic.LoadInt64(endNodeNum) > 0 {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("context is time out")
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}

	for _, nh := range nodeHandlerMap {
		for _, rh := range nh.RangeHanders {
			if !rh.IsLeader {
				continue
			}

			for _, rs := range rh.PeersStatus {
				if rs.Snapshotting {
					nh.OutSnapshot++
				}

				if toNh := nodeHandlerMap[rs.Peer.NodeId]; toNh != nil {
					toNh.InSnapshot++
				} else {
					log.Error("range:[%d] has snapshot but inSnapshot node:[%d] not found", rh.Id, rs.Peer.NodeId)
				}
			}
		}
	}

	return nodeMap, nodeHandlerMap, nil
}
