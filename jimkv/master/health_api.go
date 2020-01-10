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

package master

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/jimdb-org/jimdb/jimkv/master/entity"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/basepb"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/mspb"
	"github.com/jimdb-org/jimdb/jimkv/master/service"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/ginutil"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"net/http"
)

//it not support proto api
type healthApi struct {
	router  *gin.Engine
	service *service.BaseService
	monitor monitoring.Monitor
}

func ExportToHealthHandler(router *gin.Engine, service *service.BaseService) {
	m := entity.Monitor()
	c := &healthApi{router: router, service: service, monitor: m}
	base30 := newBaseHandler(30, m)
	router.Handle(http.MethodGet, "/health/node_info", base30.TimeOutHandler, base30.PaincHandler(c.nodeInfo), base30.TimeOutEndHandler)

	router.Handle(http.MethodGet, "/health/view_info", base30.TimeOutHandler, base30.PaincHandler(c.clusterInfoView), base30.TimeOutEndHandler)

	router.Handle(http.MethodGet, "/health/conf", base30.TimeOutHandler, base30.PaincHandler(c.config), base30.TimeOutEndHandler)
	router.Handle(http.MethodGet, "/health/master_list", base30.TimeOutHandler, base30.PaincHandler(c.masterList), base30.TimeOutEndHandler)

	//router.Handle(http.MethodPost, "/health/topo_check", base30.TimeOutHandler, base30.PaincHandler(c.topologyCheck), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/health/set_loglevel", base30.TimeOutHandler, base30.PaincHandler(c.setMasterLogLevel), base30.TimeOutEndHandler)
}

//it not support proto api
func (ha *healthApi) nodeInfo(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	nodes, err := ha.service.QueryOnlineNodes(ctx.(context.Context))
	if err != nil {
		ginutil.NewAutoMehtodName(c, ha.monitor).Send(&mspb.GetNodeInfoResponse{Header: entity.Err(err)})
		return
	}

	nodeInfos := make([]*mspb.GetNodeInfo, 0, len(nodes))

	for _, node := range nodes {
		nodeInfo, err := ha.service.NodeInfo(ctx.(context.Context), node)
		var gnir *mspb.GetNodeInfo
		if err != nil {
			gnir = &mspb.GetNodeInfo{Err: err.Error(), Node: node}
		} else {
			gnir = &mspb.GetNodeInfo{Node: node, Info: nodeInfo}
		}
		nodeInfos = append(nodeInfos, gnir)
	}

	ginutil.NewAutoMehtodName(c, ha.monitor).Send(&mspb.GetNodeInfoResponse{Header: entity.OK(), NodeInfos: nodeInfos})
}

func (ha *healthApi) clusterInfoView(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	nodes, err := ha.service.QueryOnlineNodes(ctx.(context.Context))
	if err != nil {
		ginutil.NewAutoMehtodName(c, ha.monitor).Send(&mspb.GetNodeInfoResponse{Header: entity.Err(err)})
		return
	}

	nodeInfos := make([]*mspb.GetNodeInfo, 0, len(nodes))

	for _, node := range nodes {
		nodeInfo, err := ha.service.NodeInfo(ctx.(context.Context), node)
		var gnir *mspb.GetNodeInfo
		if err != nil {
			gnir = &mspb.GetNodeInfo{Err: err.Error(), Node: node}
		} else {
			gnir = &mspb.GetNodeInfo{Node: node, Info: nodeInfo}
		}
		nodeInfos = append(nodeInfos, gnir)
	}

	buf := bytes.Buffer{}

	snapshot := make(map[[2]uint64]bool)

	down := make(map[[2]uint64]bool)
	for _, info := range nodeInfos {
		if info == nil || info.GetErr() != "" {
			continue
		}
		for _, r := range info.Info.RangeInfos {
			for _, ps := range r.PeersStatus {
				if ps.DownSeconds > 0 {
					down[[2]uint64{ps.Peer.NodeId, r.Range.Id}] = true
				} else if ps.Snapshotting || ps.Peer.Type != basepb.PeerType_PeerType_Normal {
					snapshot[[2]uint64{ps.Peer.NodeId, r.Range.Id}] = true
				}
			}
		}
	}

	buf.WriteString("<table border=1>")

	for _, info := range nodeInfos {
		if info == nil {
			continue
		}
		buf.WriteString("<tr>")

		if info.Node.Type == basepb.StoreType_Store_Warm {
			buf.WriteString("<td  bgcolor=#9F9756 width=100> Node" + cast.ToString(info.Node.Id) + ":[" + info.Node.Ip + ":" + cast.ToString(info.Node.ServerPort) + "] </td>")
		} else {
			buf.WriteString("<td  bgcolor=#99CCCC width=100> Node" + cast.ToString(info.Node.Id) + ":[" + info.Node.Ip + ":" + cast.ToString(info.Node.ServerPort) + "] </td>")
		}

		leaderNum := 0
		if info.Err != "" {
			buf.WriteString("<td width=50 bgcolor=#FF6666>" + info.Err + "</td>")
		} else {
			line := bytes.Buffer{}
			for _, r := range info.Info.RangeInfos {

				var bg string

				if r.Range.RangeType == basepb.RangeType_RNG_Index {
					bg = "#FFCCCC"
					if r.Range.Leader == info.Node.Id {
						leaderNum++
						bg = "#FF6666"
					}
				} else {
					bg = "#9999ff"
					if r.Range.Leader == info.Node.Id {
						leaderNum++
						bg = "#9900ff"
					}
				}

				line.WriteString("<td  width=50 bgcolor=" + bg)
				if down[[2]uint64{info.Node.Id, r.Range.Id}] {
					line.WriteString(` style="color:#FF0000" `)
				} else if snapshot[[2]uint64{info.Node.Id, r.Range.Id}] {
					line.WriteString(` style="color:#FFF" `)
				}
				line.WriteString(">")
				line.WriteString(cast.ToString(r.Range.Id))
				line.WriteString("</td>")
			}
			buf.WriteString("<td width=50 bgcolor=#99CC00>" + cast.ToString(leaderNum) + "/" + cast.ToString(len(info.Info.RangeInfos)) + "</td>")
			buf.Write(line.Bytes())
		}

		buf.WriteString("</tr>\n")
	}
	buf.WriteString("</table>\n")
	_, _ = c.Writer.WriteString(buf.String())
}

type ConfigResult struct {
	Error     string `json:"error,omitempty"`
	AutoSplit bool   `json:"auto_split"`
	FailOver  bool   `json:"fail_over"`
	Balanced  bool   `json:"balanced"`
}

func (ha *healthApi) config(c *gin.Context) {
	cc, _ := c.Get(Ctx)
	var ctx = cc.(context.Context)

	autoSplit := c.Query("auto_split")
	failOver := c.Query("fail_over")
	balanced := c.Query("balanced")

	result := &ConfigResult{}

	if autoSplit != "" {
		ha.configOK(ctx, autoSplit, entity.ConfAutoSplit, result)
	}

	if failOver != "" {
		ha.configOK(ctx, failOver, entity.ConfFailOver, result)
	}

	if balanced != "" {
		ha.configOK(ctx, balanced, entity.ConfBalanced, result)
	}

	result.AutoSplit = ha.service.ConfigAutoSplit(ctx)
	result.FailOver = ha.service.ConfigFailOver(ctx)
	result.Balanced = ha.service.ConfigBalanced(ctx)

	ginutil.NewAutoMehtodName(c, ha.monitor).SendJson(result)
}

func (ha *healthApi) configOK(ctx context.Context, val, key string, result *ConfigResult) {
	if val == "" {
		return
	}
	b, err := cast.ToBoolE(val)
	if err != nil {
		result.Error = err.Error()
		return
	}
	err = ha.service.PutBoolConfig(ctx, key, b)
	if err != nil {
		result.Error = err.Error()
	}

}

func (ha *healthApi) masterList(c *gin.Context) {
	masterList, e := json.Marshal(entity.Conf().Masters)
	if e != nil {
		_, _ = c.Writer.WriteString("json encode master list err.")
		return
	}

	_, _ = c.Writer.WriteString(string(masterList))
}

//func (ha *healthApi) topologyCheck(c *gin.Context) {
//	cc, _ := c.Get(Ctx)
//	var ctx = cc.(context.Context)
//
//	dbName := c.PostForm("dbName")
//	tableName := c.PostForm("tableName")
//	if dbName == "" || tableName == "" {
//		_, _ = c.Writer.WriteString(fmt.Sprintf("invalid param: dbName[%s] tableName[%s]", dbName, tableName))
//		return
//	}
//
//	dataBase, err := ha.service.QueryDBByName(ctx, dbName)
//	if err != nil {
//		_, _ = c.Writer.WriteString(fmt.Sprintf("query db by name[%s] failed, err[%s]", dbName, err.Error()))
//		return
//	}
//	table, err := ha.service.QueryTableByName(ctx, dataBase.GetId(), tableName)
//	if err != nil {
//		_, _ = c.Writer.WriteString(fmt.Sprintf("query table by name[%s] failed, err[%s]", tableName, err.Error()))
//		return
//	}
//	ranges, err := ha.service.QueryRanges(ctx, table.GetId())
//	if err != nil {
//		_, _ = c.Writer.WriteString(fmt.Sprintf("query table ranges by tableId[%d] failed, err[%s]", table.GetId(), err.Error()))
//		return
//	}
//
//	idxRange := make([]*basepb.Range, 0)
//	dataRange := make([]*basepb.Range, 0)
//	for _, rng := range ranges {
//		if rng.RangeType == basepb.RangeType_RNG_Index {
//			idxRange = append(idxRange, rng)
//		}
//		if rng.RangeType == basepb.RangeType_RNG_Data {
//			dataRange = append(dataRange, rng)
//		}
//	}
//
//	sort.Slice(idxRange, func(i, j int) bool {
//		return bytes.Compare(idxRange[i].StartKey, idxRange[j].StartKey) >= 0
//	})
//	sort.Slice(dataRange, func(i, j int) bool {
//		return bytes.Compare(dataRange[i].StartKey, dataRange[j].StartKey) >= 0
//	})
//	idxStartKey, idxEndKey := utils.EncodeStorePrefix(utils.Store_Prefix_INDEX, table.GetId())
//	dataStartKey, dataEndKey := utils.EncodeStorePrefix(utils.Store_Prefix_KV, table.GetId())
//
//	idxCheckKey := idxStartKey
//	for i := range idxRange {
//		if bytes.Compare(idxCheckKey, idxRange[i].GetStartKey()) != 0 {
//			_, _ = c.Writer.WriteString(fmt.Sprintf("check topo failed, err[%s]", "index-startKey not matching"))
//			return
//		} else {
//			idxCheckKey = idxRange[i].GetEndKey()
//		}
//
//		if i == len(idxRange)-1 {
//			if bytes.Compare(idxEndKey, idxRange[i].GetEndKey()) != 0 {
//				_, _ = c.Writer.WriteString(fmt.Sprintf("check topo failed, err[%s]", "index-endKey not matching"))
//				return
//			}
//		}
//	}
//
//	dataCheckKey := dataStartKey
//	for i := range dataRange {
//		if bytes.Compare(dataCheckKey, dataRange[i].GetStartKey()) != 0 {
//			_, _ = c.Writer.WriteString(fmt.Sprintf("check topo failed, err[%s]", "data-startKey not matching"))
//			return
//		} else {
//			dataCheckKey = dataRange[i].GetEndKey()
//		}
//
//		if i == len(dataRange)-1 {
//			if bytes.Compare(dataEndKey, dataRange[i].GetEndKey()) != 0 {
//				_, _ = c.Writer.WriteString(fmt.Sprintf("check topo failed, err[%s]", "data-endKey not matching"))
//				return
//			}
//		}
//	}
//
//	_, _ = c.Writer.WriteString("check topo succeed")
//}

func (ha *healthApi) setMasterLogLevel(c *gin.Context) {
	logLevel := c.PostForm("logLevel")
	if logLevel == "" {
		_, _ = c.Writer.WriteString("logLevel cannot be null or empty")
		return
	}

	log.Info("ready to set master's loglevel to [%s]", logLevel)
	log.SetLogLevel(logLevel)
	_, _ = c.Writer.WriteString("OK")
	return
}
