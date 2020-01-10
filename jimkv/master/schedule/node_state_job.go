package schedule

import (
	"fmt"
	"github.com/jimdb-org/jimdb/jimkv/master/entity"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/jimkv/master/service"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/cblog"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"github.com/spf13/cast"
	"math/rand"
	"sync"
	"time"
)

var _ ProcessJob = &nodeStateJob{}

type nodeStateJob struct {
	service *service.BaseService
}

func (nsj *nodeStateJob) process(ctx *processContext) {
	if ctx.stop {
		log.Info("got stop so skip NodeStateJob")
		return
	}

	log.Info("start NodeStateJob begin")

	for _, nh := range ctx.nodeHandlerMap {
		if nh.State == basepb.NodeState_N_Updating {
			log.Info("found node:[%d] state is updating to move leader begin", nh.Id)
			ctx.stop = true
			nsj.moveLeader(ctx, nh)
		}

		if nh.State == basepb.NodeState_N_Offlining {
			log.Info("found node:[%d] state is offlining to move range begin", nh.Id)
			ctx.stop = true
			nsj.moveRange(ctx, nh)
		}
	}
}

func (nsj *nodeStateJob) moveLeader(ctx *processContext, nh *service.NodeHandler) {
	m := entity.Monitor()
	for _, rh := range nh.RangeHanders {
		if rh.IsLeader {
			peers := rh.Peers

			rand.Shuffle(len(peers), func(i, j int) {
				peers[i], peers[j] = peers[j], peers[i]
			})

			flag := false

			for _, peer := range rh.Peers {
				if peer.NodeId == nh.Id {
					continue
				}

				toNH := ctx.nodeHandlerMap[peer.NodeId]

				if toNH.State != basepb.NodeState_N_Online {
					log.Warn("transfer leader fromNode:[%d] toNode:[%d] by range:[%d] fail because to state:[%v]", nh.Id, toNH.Id, rh.Id, toNH.State)
					continue
				}

				if err := nsj.service.TransferLeader(ctx.Context, toNH.Node, rh.Range); err != nil {
					_ = cblog.LogErrAndReturn(fmt.Errorf("transfer leader fromNode:[%d] toNode:[%d] by range:[%d] err:[%s]", nh.Id, toNH.Id, rh.Id, err.Error()))
					if m != nil {
						m.GetGauge(m.GetCluster(), "schedule", "event", "transfer_leader", "fail").Add(1)
					}
				} else {
					flag = true
					if m != nil {
						m.GetGauge(m.GetCluster(), "schedule", "event", "transfer_leader", "success").Add(1)
					}
				}

				break
			}

			if !flag {
				log.Error("transfer leader by node:[%d] range:[%d] is fail", nh.Id, rh.Id)
			}

		}
	}
}

func (nsj *nodeStateJob) moveRange(ctx *processContext, nh *service.NodeHandler) {

	leaderQueueMap := make(map[uint64][]*service.RangeHandler)

	for _, rh := range nh.RangeHanders {
		leaderQueueMap[rh.Leader] = append(leaderQueueMap[rh.Leader], rh)
	}

	lock := &sync.Mutex{}

	wg := sync.WaitGroup{}

	for _, queue := range leaderQueueMap {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				if e := recover(); e != nil {
					log.Error("move range by queue has panice :%s", cast.ToString(e))
				}
			}()
			nsj.moveRangeByQueue(ctx, lock, nh, queue)
		}()
	}

	wg.Wait()
	log.Info("move range node:[%d] rangeNum:[%d] ok ", nh.Id, nh.RangeNum)
}

func (nsj *nodeStateJob) moveRangeByQueue(ctx *processContext, lock *sync.Mutex, nh *service.NodeHandler, queue []*service.RangeHandler) {
	m := entity.Monitor()
	for _, rh := range queue {

		table := ctx.tableMap[rh.TableId]

		replicasCount := int(table.Replicas)
		if table != nil && len(rh.Peers) <= replicasCount {
			leaderNh := ctx.nodeHandlerMap[rh.Leader]

			if leaderNh.GetRH(rh.Id) == nil {
				log.Error("not found leader range:[%d] in node:[%d]", rh.Id, rh.Leader)
				return
			}

			nh, err := ctx.nodeHandlerMap.MinArriveNodeByRange(rh.StoreType, rh.Range)
			if err != nil {
				log.Error(err.Error())
				return
			}

			err = nsj.service.CreateRangeToNode(ctx, nh.Node, rh.Range)
			if err == nil {

				lock.Lock()
				nh.RangeNum++
				nh.InSnapshot++
				leaderNh.OutSnapshot++
				lock.Unlock()
				if m != nil {
					m.GetGauge(m.GetCluster(), "schedule", "event", "create_range", "success").Add(1)
				}
				for {

					select {
					case <-ctx.Context.Done():
						return
					default:

					}

					log.Info("check add range:[%d] to node is ok ", rh.Id)

					if rng, err := nsj.service.QueryRange(ctx, rh.TableId, rh.Id); err != nil {
						log.Error(err.Error())
					} else {
						for _, peer := range rng.Peers {
							if peer.NodeId != nh.Id {
								continue
							}

							if peer.Type == basepb.PeerType_PeerType_Normal {
								lock.Lock()
								nh.InSnapshot--
								leaderNh.OutSnapshot--
								lock.Unlock()
								goto addRangeOK
							}

						}

						time.Sleep(time.Second)
					}
				}
			}
		}

	addRangeOK:

		//check can del one

		rng, err := nsj.service.QueryRange(ctx, rh.TableId, rh.Id)
		if err != nil {
			log.Error(err.Error())
			continue
		}

		if table != nil && len(rng.Peers) <= replicasCount {
			log.Info("can del range:[%d] in node:[%d] because number not sure:[%d/%d] ", rh.Id, nh.Id, len(rng.Peers), replicasCount)
			continue
		}

		if err := nsj.service.SyncDeleteRangeToNode(ctx, rng, rh.Peer.Id, rh.Peer.NodeId); err != nil {
			log.Error("delete range by node:[%d] peer:[%d] has err:[%s]", rh.Peer.NodeId, rh.Peer.Id, err.Error())
		} else {
			lock.Lock()
			nh.RangeNum--
			lock.Unlock()
			log.Info("delete range by node:[%d] peer:[%d]", rh.Peer.NodeId, rh.Peer.Id)
		}
	}
}
