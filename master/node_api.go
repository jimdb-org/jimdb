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
	"context"
	"fmt"
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/errs"
	"github.com/jimdb-org/jimdb/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/master/entity/pkg/dspb"
	"github.com/jimdb-org/jimdb/master/entity/pkg/mspb"
	"github.com/jimdb-org/jimdb/master/service"
	"github.com/jimdb-org/jimdb/master/utils/ginutil"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"github.com/jimdb-org/jimdb/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
	"strings"
)

type nodeApi struct {
	router      *gin.Engine
	nodeService *service.BaseService
	monitor     monitoring.Monitor
}

func ExportToNodeHandler(router *gin.Engine, nodeService *service.BaseService) {
	m := entity.Monitor()
	c := &nodeApi{router: router, nodeService: nodeService, monitor: m}
	base30 := newBaseHandler(30, m)
	router.Handle(http.MethodPost, "/node/heartbeat", base30.TimeOutHandler, base30.PaincHandler(c.nodeHeartbeat), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/node/register", base30.PaincHandler(base30.TimeOutHandler), c.registerNode, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/node/get", base30.TimeOutHandler, base30.PaincHandler(c.getNode), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/node/list", base30.TimeOutHandler, base30.PaincHandler(c.listNode), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/node/change_state", base30.TimeOutHandler, base30.PaincHandler(c.changeState), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/node/check_state", base30.TimeOutHandler, base30.PaincHandler(c.checkState), base30.TimeOutEndHandler)

	router.Handle(http.MethodPost, "/node/set_loglevel", base30.TimeOutHandler, base30.PaincHandler(c.setNodeLogLevel), base30.TimeOutEndHandler)
}

func (na *nodeApi) nodeHeartbeat(c *gin.Context) {
	ctx, _ := c.Get(Ctx)
	var err error
	req := &mspb.NodeHeartbeatRequest{}
	if err = bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.NodeHeartbeatResponse{Header: entity.Err(err)})
		return
	}

	err = na.nodeService.NodeHeartbeat(ctx.(context.Context), req.NodeId)
	if err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.NodeHeartbeatResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.NodeHeartbeatResponse{Header: entity.OK(), NodeId: req.NodeId})
}

// register a node , when new  or restart
func (na *nodeApi) registerNode(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.RegisterNodeRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.RegisterNodeResponse{Header: entity.Err(err)})
		return
	}

	ip := c.Request.RemoteAddr[0:strings.LastIndex(c.Request.RemoteAddr, ":")]

	var typed basepb.StoreType

	switch req.StEngine {
	case mspb.StorageEngine_STE_MassTree:
		typed = basepb.StoreType_Store_Hot
	case mspb.StorageEngine_STE_RocksDB:
		typed = basepb.StoreType_Store_Warm
	default:
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.RegisterNodeResponse{Header: entity.Err(fmt.Errorf("engine type:[%d] not maped", req.StEngine))})
		return

	}

	node := &basepb.Node{
		Ip:         ip,
		ServerPort: req.ServerPort,
		RaftPort:   req.RaftPort,
		AdminPort:  req.AdminPort,
		State:      basepb.NodeState_N_Online,
		Type:       typed,
		Version:    1,
	}

	node, invalidRanges, err := na.nodeService.RegisterNode(ctx.(context.Context), node, req.Ranges)
	if err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.RegisterNodeResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.RegisterNodeResponse{Header: entity.OK(), NodeId: node.Id, InvalidRanges: invalidRanges})
}

// to find a node by nodeID
func (na *nodeApi) getNode(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetNodeRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodeResponse{Header: entity.Err(err)})
		return
	}

	node, err := na.nodeService.GetNode(ctx.(context.Context), req.Id)
	if err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodeResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodeResponse{Header: entity.OK(), Node: node})
}

// to find all node
func (na *nodeApi) listNode(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetNodesRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodesResponse{Header: entity.Err(err)})
		return
	}

	nodes, err := na.nodeService.QueryAllNodes(ctx.(context.Context))
	if err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodesResponse{Header: entity.Err(err)})
		return
	}

	for _, node := range nodes {
		online, err := na.nodeService.Online(ctx.(context.Context), node.Id)
		if err != nil {
			log.Error("get node by ttl key err:[%s]", err.Error())
		}

		if !online {
			node.State = basepb.NodeState_N_Offline
		}
	}

	ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodesResponse{Header: entity.OK(), Nodes: nodes})
}

// to find all node
func (na *nodeApi) changeState(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.ChangeNodeStateRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.ChangeNodeStateResponse{Header: entity.Err(err)})
		return
	}

	if req.State == 0 {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.ChangeNodeStateResponse{Header: entity.Err(errs.Error(mspb.ErrorType_NodeStateConfused))})
		return
	}

	node, err := na.nodeService.ChangeState(ctx.(context.Context), req.Id, req.State)
	if err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.ChangeNodeStateResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.ChangeNodeStateResponse{Header: entity.OK(), Node: node})
}

// to check range it will return leader num and range num
func (na *nodeApi) checkState(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.CheckNodeStateRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.CheckNodeStateResponse{Header: entity.Err(err)})
		return
	}

	nodeState, leaderNum, rangeNum, err := na.nodeService.CheckState(ctx.(context.Context), req.Id)
	if err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.CheckNodeStateResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.CheckNodeStateResponse{Header: entity.OK(), State: nodeState, LeaderNum: leaderNum, RangeNum: rangeNum})
}

func (na *nodeApi) setNodeLogLevel(c *gin.Context) {
	cc, _ := c.Get(Ctx)
	var ctx = cc.(context.Context)

	nodeIdStr := c.PostForm("nodeId")
	if nodeIdStr == "" {
		_, _ = c.Writer.WriteString("nodeId cannot be null or empty")
		return
	}
	nodeId, err := strconv.ParseUint(nodeIdStr, 10, 64)
	if err != nil {
		_, _ = c.Writer.WriteString("string of nodeId parse to int error")
		return
	}
	logLevel := c.PostForm("logLevel")
	if logLevel == "" {
		_, _ = c.Writer.WriteString("logLevel cannot be null or empty")
		return
	}

	node, err := na.nodeService.QueryNode(ctx, nodeId)
	if err != nil {
		_, _ = c.Writer.WriteString(fmt.Sprintf("cannt find node by nodeId[%d], err=[%s]", nodeId, err.Error()))
		return
	}
	if node == nil {
		_, _ = c.Writer.WriteString(fmt.Sprintf("find node=nil by nodeId[%d]", nodeId))
		return
	}

	item := &dspb.ConfigItem{
		Key: &dspb.ConfigKey{
			Section: "log",
			Name:    "level",
		},
		Value: logLevel,
	}
	var configs []*dspb.ConfigItem
	configs = append(configs, item)

	nodeAdmAddr := fmt.Sprintf("%s:%d", node.Ip, node.AdminPort)
	err = na.nodeService.AdmClient().SetConfig(nodeAdmAddr, configs)
	if err != nil {
		_, _ = c.Writer.WriteString(fmt.Sprintf("addr=%s set loglevel failed, err=[%s]", nodeAdmAddr, err.Error()))
		return
	}

	log.Info(fmt.Sprintf("set ds admAddr=[%s] loglevel to [%s]", nodeAdmAddr, logLevel))
	_, _ = c.Writer.WriteString("OK")
	return
}
