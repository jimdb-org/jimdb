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
	"encoding/json"
	"github.com/jimdb-org/jimdb/master/entity"
	"github.com/jimdb-org/jimdb/master/entity/errs"
	"github.com/jimdb-org/jimdb/master/entity/pkg/mspb"
	"github.com/jimdb-org/jimdb/master/utils/log"
	"github.com/jimdb-org/jimdb/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const (
	Timeout          = "timeout"
	CancelFunc       = "__cancelFunc"
	Ctx              = "__ctx"
	Start            = "__start_time"
	ProtoContentType = "application/proto"
)

//default 30s to timout
var base30 = newBaseHandler(30, nil)

//default 300s to timout
var base300 = newBaseHandler(300, nil)

type baseHandler struct {
	timeout int64 //default timeout Second
	m       monitoring.Monitor
}

func newBaseHandler(timeout int64, m monitoring.Monitor) *baseHandler {
	return &baseHandler{timeout: timeout, m: m}
}

func (bh *baseHandler) Timeout() int64 {
	return bh.timeout
}

func (b *baseHandler) PaincHandler(handlerFunc gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				var msg string
				switch r.(type) {
				case error:
					msg = r.(error).Error()
				default:
					if str, err := cast.ToStringE(r); err != nil {
						msg = "Server internal error "
					} else {
						msg = str
					}
				}
				log.Error(msg)

				c.JSON(http.StatusInternalServerError, map[string]string{"message": msg})
			}
		}()
		handlerFunc(c)
	}
}

func (b *baseHandler) TimeOutHandler(c *gin.Context) {
	param := c.Query(Timeout)

	// add start time for monitoring
	c.Set(Start, time.Now())

	ctx := context.Background()

	if param != "" {
		if v, err := cast.ToInt64E(param); err != nil {
			log.Error("parse timeout err , it must int value")
		} else {
			ctx, cancelFunc := context.WithTimeout(ctx, time.Duration(v*int64(time.Second)))
			c.Set(Ctx, ctx)
			c.Set(CancelFunc, cancelFunc)
			return
		}
	}

	if b.timeout > 0 {
		ctx, cancelFunc := context.WithTimeout(ctx, time.Duration(b.timeout*int64(time.Second)))
		c.Set(Ctx, ctx)
		c.Set(CancelFunc, cancelFunc)
		return
	}

	c.Set(Ctx, ctx)
}

func (b *baseHandler) TimeOutEndHandler(c *gin.Context) {
	//write monitor info
	if b.m != nil {
		var gauge monitoring.Gauge
		gauge = b.m.GetGauge(b.m.GetCluster(), "rpc", "http", "master", "request")
		gauge.Add(float64(1))
		if value, exists := c.Get(Ctx); exists {
			if value.(context.Context).Err() == context.DeadlineExceeded {
				gauge = b.m.GetGauge(b.m.GetCluster(), "rpc", "http", "master", "timeout")
				gauge.Add(float64(1))
			}
		}
	}
	if value, exists := c.Get(CancelFunc); exists && value != nil {
		value.(context.CancelFunc)()
	}
}

//validate head is can use
func validateHead(header *mspb.RequestHeader) (err error) {
	if entity.Conf().Global.ClusterID != header.ClusterId {
		err = errs.Error(mspb.ErrorType_ClusterIDNotSame)
	}
	return
}

func bindByBytes(c *gin.Context, bytes []byte, req entity.ProtoReq) (err error) {
	if isProtoType(c) {
		err = req.Unmarshal(bytes)
	} else {
		err = json.Unmarshal(bytes, req)
	}
	if err != nil {
		return err
	}
	return validateHead(req.GetHeader())
}

func bind(c *gin.Context, req entity.ProtoReq) error {
	bytes, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		return err
	}

	if isProtoType(c) {
		err = req.Unmarshal(bytes)
	} else {
		err = json.Unmarshal(bytes, req)
	}
	if err != nil {
		return err
	}
	return validateHead(req.GetHeader())
}

func isProtoType(c *gin.Context) bool {
	cType := c.Request.Header.Get("Content-Type")
	if cType == "" {
		return false
	}
	return strings.HasPrefix(cType, ProtoContentType)
}
