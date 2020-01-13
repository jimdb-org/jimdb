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
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/pkg/mspb"
	"github.com/jimdb-org/jimdb/master/service"
	"github.com/jimdb-org/jimdb/master/utils/ginutil"
	"github.com/jimdb-org/jimdb/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"net/http"
)

type clusterApi struct {
	router  *gin.Engine
	service *service.BaseService
	monitor monitoring.Monitor
}

func ExportToClusterHandler(router *gin.Engine, service *service.BaseService) {
	m := entity.Monitor()
	c := &clusterApi{router: router, service: service, monitor: m}

	//database handler
	base30 := newBaseHandler(30, m)
	//base60 := newBaseHandler(60, m)

	router.Handle(http.MethodGet, "/", base30.TimeOutHandler, base30.PaincHandler(c.clusterInfo), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/", base30.TimeOutHandler, base30.PaincHandler(c.clusterInfo), base30.TimeOutEndHandler)

	//clean lock
	router.Handle(http.MethodGet, "/clean_lock", base30.TimeOutHandler, base30.PaincHandler(c.cleanLock), base30.TimeOutEndHandler)

}

func (ca *clusterApi) clusterInfo(c *gin.Context) {
	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.ClusterInfoResponse{
		Header:       entity.OK(),
		BuildVersion: entity.GetBuildVersion(),
		BuildTime:    entity.GetBuildTime(),
		CommitId:     entity.GetCommitID(),
	})
}

//clean lock for admin , when space locked , waring make sure not create space ing , only support json
func (ca *clusterApi) cleanLock(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	removed := make([]string, 0, 1)

	if keys, _, err := ca.service.PrefixScan(ctx.(context.Context), entity.PrefixLock); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GeneralResponse{Header: entity.Err(err)})
		return
	} else {
		for _, key := range keys {
			if err := ca.service.Delete(ctx.(context.Context), string(key)); err != nil {
				ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GeneralResponse{Header: entity.Err(err)})
				return
			}
			removed = append(removed, string(key))
		}
	}

	result := &struct {
		Header  *mspb.ResponseHeader `json:"header"`
		Removed []string             `json:"removed"`
	}{
		Header:  entity.OK(),
		Removed: removed,
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).SendJson(result)
}
