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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/jimdb-org/jimdb/jimkv/master"
	"github.com/jimdb-org/jimdb/jimkv/master/entity"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/log"
	_ "github.com/jimdb-org/jimdb/jimkv/master/utils/monitoring/baudtime"
	_ "github.com/jimdb-org/jimdb/jimkv/master/utils/monitoring/prometheus"
	"github.com/jimdb-org/jimdb/jimkv/master/utils/reflect"
	"github.com/spf13/cast"
)

var (
	BuildVersion = "0.0"
	BuildTime    = "0"
	CommitID     = "xxxxx"
	confPath     string
	masterName   string
)

func init() {
	flag.StringVar(&confPath, "conf", getDefaultConfigFile(), "baud config path")
	flag.StringVar(&masterName, "master", "", "baud config for master name , is on local start two master must use it")
}

func main() {

	log.Info("start server by version:[%s] commitID:[%s]", BuildVersion, CommitID)
	entity.SetConfigVersion(BuildVersion, BuildTime, CommitID)

	flag.Parse()

	if strings.Compare(confPath, "") == 0 {
		log.Error("Can not get the config file ,then exit the program!")
		os.Exit(1)
	}
	log.Info("The Config File Is: %v", confPath)

	entity.InitConfig(confPath)

	log.Info("The configuration content is:\n%s", reflect.ToString("conf", entity.Conf()))

	if err := entity.Conf().CurrentByMasterNameDomainIp(masterName); err != nil {
		panic(err)
	}

	if err := entity.Conf().Validate(); err != nil {
		panic(err)
	}
	// must init monitor after load config
	entity.InitMonitor()

	server, err := master.NewServer(context.WithCancel(context.Background()))
	if err != nil {
		panic(fmt.Sprintf("new master error : %s", err.Error()))
	}

	go func() {
		if err := server.Start(); err != nil {
			log.Error(fmt.Sprintf("start master error :%v", err))
			os.Exit(-1)
		}
	}()

	if port := entity.Conf().Masters.Self().PprofPort; port > 0 {
		go func() {
			if err := http.ListenAndServe("0.0.0.0:"+cast.ToString(port), nil); err != nil {
				log.Error(err.Error())
			}
		}()
	}

	sigsC := make(chan os.Signal)

	signal.Notify(sigsC, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)

	<-sigsC //wait stop signal

	server.Stop()

}

func getDefaultConfigFile() (defaultConfigFile string) {
	if currentExePath, err := getCurrentPath(); err == nil {
		path := currentExePath + "config/config.toml"
		if ok, err := pathExists(path); ok {
			return path
		} else if err != nil {
			log.Error("check path:%s err : %s", path, err.Error())
		}
	}

	if sourceCodeFileName, err := getCurrentSourceCodePath(); nil == err {
		lastIndex := strings.LastIndex(sourceCodeFileName, "/")
		path := sourceCodeFileName[0:lastIndex+1] + "config/config.toml"
		if ok, err := pathExists(path); ok {
			return path
		} else if err != nil {
			log.Error("check path:%s err : %s", path, err.Error())
		}
	}
	return
}

func getCurrentPath() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	path, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}
	i := strings.LastIndex(path, "/")
	if i < 0 {
		i = strings.LastIndex(path, "\\")
	}
	if i < 0 {
		return "", errors.New(`error: Can't find "/" or "\"`)
	}
	return path[0 : i+1], nil
}

func getCurrentSourceCodePath() (fileName string, err error) {
	_, fileName, _, ok := callerName(2)
	if !ok {
		err = errors.New("error: Can't get the current source code path")
	}
	return fileName, err
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func callerName(skip int) (funcName, file string, line int, ok bool) {
	var pc uintptr
	if pc, file, line, ok = runtime.Caller(skip); !ok {
		return
	}
	funcName = runtime.FuncForPC(pc).Name()
	return
}
