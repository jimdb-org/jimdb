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

package master

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/master/entity/pkg/mspb"
	"github.com/jimdb-org/jimdb/master/service"
	"github.com/jimdb-org/jimdb/master/utils/cblog"
	"github.com/jimdb-org/jimdb/master/utils/ginutil"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"github.com/jimdb-org/jimdb/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/spf13/cast"
	"net/http"
	"sort"
	"time"
)

type RangeChange int

const (
	LeaderChange RangeChange = 1 << iota
	PeerChange
)

func (r RangeChange) IsChanged() bool {
	return r > 0
}

func (r RangeChange) IsLeaderChange() bool {
	return r&LeaderChange != 0
}

func (r RangeChange) IsPeerChange() bool {
	return r&PeerChange != 0
}

type rangeApi struct {
	router         *gin.Engine
	watcherService *service.WatcherService
	rangeService   *service.BaseService
	monitor        monitoring.Monitor
}

func ExportToRangeHandler(router *gin.Engine, rangeService *service.BaseService, watcherService *service.WatcherService) {
	m := entity.Monitor()
	c := &rangeApi{router: router, rangeService: rangeService, watcherService: watcherService, monitor: m}
	base30 := newBaseHandler(30, m)

	router.Handle(http.MethodPost, "/range/create", base30.TimeOutHandler, base30.PaincHandler(c.createRanges), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/range/delete", base30.TimeOutHandler, base30.PaincHandler(c.deleteRanges), base30.TimeOutEndHandler)

	router.Handle(http.MethodPost, "/range/heartbeat", base30.TimeOutHandler, base30.PaincHandler(c.rangeHeartbeat), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/range/ask_split", base30.TimeOutHandler, base30.PaincHandler(c.askSplit), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/range/routers", base30.TimeOutHandler, base30.PaincHandler(c.getRouter), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/range/change_store_type", base30.TimeOutHandler, base30.PaincHandler(c.changeRangeStoreType), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/range/peer_move", base30.TimeOutHandler, base30.PaincHandler(c.peerMove), base30.TimeOutEndHandler)
}

var rangeCache = cache.New(20*time.Minute, 30*time.Minute)

func oldRangeCacheValue(key string) *mspb.RangeHeartbeatRequest {
	if get, b := rangeCache.Get(key); b && get != nil {
		return get.(*mspb.RangeHeartbeatRequest)
	}
	return nil
}

// range will register to store
func (ra *rangeApi) rangeHeartbeat(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.RangeHeartbeatRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.RangeHeartbeatResponse{Header: entity.Err(err)})
		return
	}

	if req.Range.RangeEpoch == nil {
		req.Range.RangeEpoch = &basepb.RangeEpoch{}
	}

	key := cast.ToString(req.Range.Id)

	oldRequest := oldRangeCacheValue(key)

	var (
		cr  RangeChange
		err error
	)

	if oldRequest != nil {
		if cr, err = isChangeRange(oldRequest, req); err != nil {
			ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.RangeHeartbeatResponse{Header: entity.Err(cblog.LogErrAndReturn(err))})
			return
		}
	} else {
		cr = LeaderChange
	}
	defer func() {
		if ra.monitor != nil {
			if cr.IsLeaderChange() {
				ra.monitor.GetGauge(ra.monitor.GetCluster(), "event", "range_leader_change").Add(1)
			}
			if cr.IsPeerChange() {
				ra.monitor.GetGauge(ra.monitor.GetCluster(), "event", "range_peer_change").Add(1)
			}
		}
	}()
	if cr.IsChanged() {
		log.Info("range:[%d] has changed:[%d] so to heartbeat service ", req.Range.Id, cr)
		req.Range.Term = req.Term
		rng, err := ra.rangeService.Heartbeat(ctx.(context.Context), req.Range)
		if err != nil {
			ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.RangeHeartbeatResponse{Header: entity.Err(err)})
			return
		}
		rangeCache.Set(key, &mspb.RangeHeartbeatRequest{
			Header: req.Header,
			Range:  rng,
			Term:   rng.Term,
		}, 20*time.Minute)
	}

	ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.RangeHeartbeatResponse{Header: entity.OK(), RangeId: req.Range.Id, Epoch: &basepb.RangeEpoch{ConfVer: req.Range.RangeEpoch.ConfVer, Version: req.Range.RangeEpoch.Version}})
}

func isChangeRange(old, new *mspb.RangeHeartbeatRequest) (RangeChange, error) {
	var c RangeChange
	if old.Term == new.Term && old.Range.RangeEpoch.ConfVer == new.Range.RangeEpoch.ConfVer && old.Range.RangeEpoch.Version == new.Range.RangeEpoch.Version {
		if bytes.Compare(old.Range.StartKey, new.Range.StartKey) != 0 || bytes.Compare(old.Range.EndKey, new.Range.EndKey) != 0 {
			return c, cblog.LogErrAndReturn(fmt.Errorf("version check rangeID:[%d] start[%s, %s] and end:[%s, %s] key not same ", old.Range.Id, old.Range.StartKey, new.Range.StartKey, old.Range.EndKey, new.Range.EndKey))
		}

		if old.Range.Leader != new.Range.Leader {
			return c, cblog.LogErrAndReturn(fmt.Errorf("term[%d/%d] same leader[%d/%d] not same", old.Term, new.Term, old.Range.Leader, new.Range.Leader))
		}

		if len(old.Range.Peers) != len(new.Range.Peers) {
			c = c | PeerChange
			return c, nil
		}

		sort.Slice(old.Range.Peers, func(i, j int) bool {
			return old.Range.Peers[i].Id < old.Range.Peers[j].Id
		})

		sort.Slice(new.Range.Peers, func(i, j int) bool {
			return new.Range.Peers[i].Id < new.Range.Peers[j].Id
		})

		for i, oP := range old.Range.Peers {
			nP := new.Range.Peers[i]
			if oP.Id != nP.Id || oP.Type != nP.Type {
				c = c | PeerChange
				break
			}
		}
		return c, nil
	}

	flag := old.Term > new.Term || old.Range.RangeEpoch.ConfVer > new.Range.RangeEpoch.ConfVer || old.Range.RangeEpoch.Version > new.Range.RangeEpoch.Version

	flag2 := old.Term < new.Term || old.Range.RangeEpoch.ConfVer < new.Range.RangeEpoch.ConfVer || old.Range.RangeEpoch.Version < new.Range.RangeEpoch.Version

	if flag == flag2 {
		//impossibility
		return c, fmt.Errorf("impossibility version check rangeID:[%d] version term:[%d] , confVer:[%d], version:[%d] contradiction cache term:[%d] , confVer:[%d], version:[%d] err", new.Range.Id, new.Term, new.Range.RangeEpoch.ConfVer, new.Range.RangeEpoch.Version, old.Term, old.Range.RangeEpoch.ConfVer, old.Range.RangeEpoch.Version)
	}

	if flag {
		return c, fmt.Errorf("version check rangeID:[%d] version term:[%d] , confVer:[%d], version:[%d] less than cache term:[%d] , confVer:[%d], version:[%d] err", new.Range.Id, new.Term, new.Range.RangeEpoch.ConfVer, new.Range.RangeEpoch.Version, old.Term, old.Range.RangeEpoch.ConfVer, old.Range.RangeEpoch.Version)
	}
	if new.Term > old.Term {
		c = c | LeaderChange
	}
	if new.Range.RangeEpoch.ConfVer > old.Range.RangeEpoch.ConfVer {
		c = c | PeerChange
	}

	if new.Range.RangeEpoch.Version > old.Range.RangeEpoch.Version {
		c = c | LeaderChange
	}
	return c, nil

}

// ask to create a new range , it will return newRangeID , and peer ids , all id is unique
func (ra *rangeApi) askSplit(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.AskSplitRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.AskSplitResponse{Header: entity.Err(err)})
		return
	}

	newRangeID, newPeerIDs, err := ra.rangeService.AskSplit(ctx.(context.Context), req.Range, req.Force)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.AskSplitResponse{Header: entity.Err(err)})
		return
	}
	defer func() {
		if ra.monitor != nil {
			ra.monitor.GetGauge(ra.monitor.GetCluster(), "event", "range_split").Add(1)
		}
	}()
	ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.AskSplitResponse{Header: entity.OK(), Range: req.Range, NewRangeId: newRangeID, NewPeerIds: newPeerIDs, SplitKey: req.SplitKey})
}

//get router for proxy , if max is zero it will return all
func (ra *rangeApi) getRouter(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetRouteRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GetRouteResponse{Header: entity.Err(err)})
		return
	}

	version := ra.watcherService.Version()

	ranges, err := ra.rangeService.GetRoute(ctx.(context.Context), int(req.Max), req.DbId, req.TableId, req.Key, req.HasAll)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GetRouteResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GetRouteResponse{Header: entity.OK(), Routes: ranges, Version: uint64(version), Master: entity.Conf().Masters.Self().Name})
}

func (ra *rangeApi) changeRangeStoreType(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.ChangeRangeStoreTypeRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.ChangeRangeStoreTypeResponse{Header: entity.Err(err)})
		return
	}

	err := ra.rangeService.ChangeRangeStoreType(ctx.(context.Context), req.TableId, req.RangeId, req.StoreType)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.ChangeRangeStoreTypeResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.ChangeRangeStoreTypeResponse{Header: entity.OK()})
}

func (ra *rangeApi) peerMove(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.ChangeRangeStoreTypeRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.ChangeRangeStoreTypeResponse{Header: entity.Err(err)})
		return
	}

	err := ra.rangeService.ChangeRangeStoreType(ctx.(context.Context), req.TableId, req.RangeId, req.StoreType)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.ChangeRangeStoreTypeResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.ChangeRangeStoreTypeResponse{Header: entity.OK()})
}

func (ra *rangeApi) createRanges(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.CreateRangesRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GeneralResponse{Header: entity.Err(err)})
		return
	}

	err := ra.rangeService.CreateRanges(ctx.(context.Context), req)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GeneralResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GeneralResponse{Header: entity.OK()})
}

func (ra *rangeApi) deleteRanges(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.DeleteRangesRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GeneralResponse{Header: entity.Err(err)})
		return
	}

	err := ra.rangeService.DeleteRanges(ctx.(context.Context), req)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GeneralResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GeneralResponse{Header: entity.OK()})
}
