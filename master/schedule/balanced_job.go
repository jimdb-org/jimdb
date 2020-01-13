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
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/master/entity/pkg/dspb"
	"github.com/jimdb-org/jimdb/master/service"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"math"
	"sort"
	"sync"
	"time"
)

var _ ProcessJob = &BalanceJob{}

const CHANGE_NUM_CROSS = math.MaxInt32

// check master distribution
// check range num in diff node is balanced , one job only move one range
type BalanceJob struct {
	service *service.BaseService
}

func (bj *BalanceJob) process(ctx *processContext) {
	if len(ctx.nodeHandlerMap) <= 2 || !bj.service.ConfigBalanced(ctx) {
		return
	}
	if ctx.stop {
		log.Info("got stop so skip BalanceJob")
		return
	}

	log.Info("start BalanceJob begin")

	bj.balanceMaster(ctx)

	nHbyType := ctx.nodeHandlerMap.SortRangesByType(basepb.StoreType_Store_Hot)
	if len(nHbyType) > 2 && len(nHbyType[len(nHbyType)-1].RangeHanders)-len(nHbyType[0].RangeHanders) >= 3 {
		log.Info("start balance hot range begin")
		bj.balanceRange(ctx, nHbyType)
	}

	nHbyType = ctx.nodeHandlerMap.SortRangesByType(basepb.StoreType_Store_Warm)
	if len(nHbyType) > 2 && len(nHbyType[len(nHbyType)-1].RangeHanders)-len(nHbyType[0].RangeHanders) >= 3 {
		log.Info("start balance warm range begin")
		bj.balanceRange(ctx, nHbyType)
	}

}

func (bj *BalanceJob) balanceMaster(ctx *processContext) {

	nhs := ctx.nodeHandlerMap.SortLeadersNum(0)
	hotLeaderNum, hotNum := 0, 0
	warmLeaderNum, warmNum := 0, 0

	for _, v := range nhs {
		if v.Type == basepb.StoreType_Store_Hot {
			hotLeaderNum += v.LeaderNum
			hotNum++
		} else {
			warmLeaderNum += v.LeaderNum
			warmNum++
		}

	}
	var avgHot, avgWarm int
	if hotNum > 0 {
		avgHot = hotLeaderNum/hotNum + 1
	}
	if warmNum > 0 {
		avgWarm = warmLeaderNum/warmNum + 1
	}

	for i := len(nhs) - 1; i >= 0; i-- {
		maxNode := nhs[i]

		if maxNode.Type == basepb.StoreType_Store_Hot {
			if maxNode.LeaderNum <= avgHot {
				continue
			}
		} else {
			if maxNode.LeaderNum <= avgWarm {
				continue
			}
		}

		for _, to := range nhs {
			if maxNode.Type != to.Type {
				continue
			}

			if maxNode.LeaderNum-to.LeaderNum >= 3 {
				midNum := float64((maxNode.LeaderNum - to.LeaderNum) / 2)

				if midNum == 0 {
					continue
				}

				var average float64
				if to.Type == basepb.StoreType_Store_Hot {
					average = float64(avgHot - to.LeaderNum)
				} else {
					average = float64(avgWarm - to.LeaderNum)
				}

				transferNum := int(math.Min(midNum, average))

				if transferNum == 0 {
					continue
				}

				log.Info("start balance leader begin")
				if bj.transferLeader(ctx, transferNum, maxNode.Id, to.Id) {
					ctx.stop = true
				}

			}
		}
	}

}

func (bj *BalanceJob) transferLeader(ctx *processContext, num int, from uint64, to uint64) bool {

	log.Info("try transferLeader num:[%d]", num)

	fromNh := ctx.nodeHandlerMap[from]
	toNh := ctx.nodeHandlerMap[to]
	m := entity.Monitor()
	for _, fromRh := range fromNh.RangeHanders {
		if !fromRh.IsLeader {
			continue
		}

		if err := fromRh.CanChangeLeader(ctx.nodeHandlerMap); err != nil {
			log.Error(err.Error())
			return false
		}

		toRh := toNh.RangeHanders[fromRh.Id]
		if toRh != nil && toRh.VersionEqual(fromRh) {
			if err := bj.service.TransferLeader(ctx, toNh.Node, toRh.Range); err != nil {
				log.Error("leader balance to node:[%d] rangeID:[%d] has err:[%s] ", toNh.Id, toRh.Id, err.Error())
				if m != nil {
					m.GetGauge(m.GetCluster(), "schedule", "event", "transfer_leader", "fail").Add(1)
				}
			} else {
				fromNh.LeaderNum--
				toNh.LeaderNum++
				num--
				log.Info("leader balance to node:[%d] rangeID:[%d] ok need num:[%d]", toNh.Id, toRh.Id, num)
				if m != nil {
					m.GetGauge(m.GetCluster(), "schedule", "event", "transfer_leader", "success").Add(1)
				}
				if num <= 0 {
					return true
				}
			}
		}
	}

	return num <= 0
}

func (bj *BalanceJob) balanceRange(ctx *processContext, nhs []*service.NodeHandler) {
	for i := len(nhs) - 1; i >= 0; i-- {
		if bj.balanceRangeByFromIndex(ctx, nhs, i) != 0 {
			break
		}
	}
}

func (bj *BalanceJob) balanceRangeByFromIndex(ctx *processContext, nhs []*service.NodeHandler, index int) int {

	changeNum := 0
	allCount := 0
	average := 0

	for _, nh := range nhs {
		allCount += nh.RangeNum
	}
	if len(nhs) > 0 {
		average = int(math.Max(float64(allCount/len(nhs)), 1))
	}

	fromNh := nhs[index]
	toNh := nhs[0]

	//check memory size
	if err := toNh.CheckMemory(); err != nil {
		log.Error(err.Error())
		return CHANGE_NUM_CROSS
	}

	if toNh.RangeNum >= average {
		log.Info("the nodeNum:[%d] is average:[%d] so skip blance ", toNh.RangeNum, average)
		return CHANGE_NUM_CROSS
	}

	locker := sync.Mutex{}
	m := entity.Monitor()
	//shuffle because master is often continuous
	for _, fromRh := range fromNh.RangeHanders {

		if toNh.GetRH(fromRh.Id) != nil {
			continue
		}

		if !fromRh.IsLeader {
			continue
		}

		if err := fromRh.CanCreateRange(ctx.nodeHandlerMap); err != nil {
			log.Warn("node:[%d] range:[%d] peer:[%d] has err so skip this balance err:[%s]", fromRh.Peer.NodeId, fromRh.Id, fromRh.Peer.Id, err.Error())
			return CHANGE_NUM_CROSS
		}

		if err := bj.service.CreateRangeToNode(ctx, toNh.Node, fromRh.Range); err != nil {
			log.Error("balance range to node err:[%s]", err.Error())
			if m != nil {
				m.GetGauge(m.GetCluster(), "schedule", "event", "create_range", "fail").Add(1)
			}
		} else {
			locker.Lock()
			changeNum++
			toNh.RangeNum++
			toNh.InSnapshot++
			fromNh.OutSnapshot++
			locker.Unlock()
			ctx.stop = true
			log.Info("create range to node:[%d] range:[%d]", toNh.Id, fromRh.Id)
			if m != nil {
				m.GetGauge(m.GetCluster(), "schedule", "event", "create_range", "success").Add(1)
			}
			//wait snapshot ok
			go func(tableID int32, rangeID uint64, from, to *service.NodeHandler) {
				time.Sleep(1 * time.Second)
				select {
				case <-ctx.Done():
					return
				default:

				}

				log.Info("check add range to node is ok ")

				for {

					select {
					case <-ctx.Done():
						return
					default:

					}

					log.Info("to wait node:[%d] range:[%d] status to normal ", to.Id, rangeID)

					if rng, err := bj.service.QueryRange(ctx, tableID, rangeID); err != nil {
						log.Error(err.Error())
					} else {
						for _, peer := range rng.Peers {
							if peer.NodeId != to.Id {
								continue
							}

							if peer.Type == basepb.PeerType_PeerType_Normal {
								locker.Lock()
								toNh.InSnapshot--
								fromNh.OutSnapshot--
								locker.Unlock()
								return
							}
						}
					}
					time.Sleep(2 * time.Second)
				}

			}(fromRh.TableId, fromRh.Id, fromNh, toNh)
		}

		if fromNh.RangeNum-toNh.RangeNum <= 3 || toNh.RangeNum >= average {
			return CHANGE_NUM_CROSS
		}

		for fromNh.OutSnapshot >= 5 { //wait for over got to next

			time.Sleep(3 * time.Second)
			select {
			case <-ctx.Done():
				return CHANGE_NUM_CROSS
			default:

			}

			log.Info("balance range not ok now wait snapshot num:[%d] to next step", fromNh.OutSnapshot)

		}

	}

	return changeNum
}

func sortToList(nhMap service.NodeHandlerMap) []*kvStruct {
	kvList := make([]*kvStruct, 0, len(nhMap))
	for k, v := range nhMap {
		kvList = append(kvList, &kvStruct{nodeID: k, num: v.LeaderNum})
	}

	sort.Slice(kvList, func(i, j int) bool {
		return kvList[i].num < kvList[j].num
	})
	return kvList
}

//it return num of not master
func (bj *BalanceJob) groupNodeInfoMaster(nodeInfoMap map[uint64]*dspb.NodeInfoResponse) map[uint64]int {
	nodeMasterNum := make(map[uint64]int)

	for nodeID, _ := range nodeInfoMap {
		nodeMasterNum[nodeID] = 0
	}
	for nodeID, nodeInfo := range nodeInfoMap {
		for _, ri := range nodeInfo.RangeInfos {
			if ri.Range.Leader == nodeID {
				nodeMasterNum[nodeID] = nodeMasterNum[nodeID] + 1
			}
		}
	}
	return nodeMasterNum
}

type kvStruct struct {
	nodeID uint64
	num    int
}
