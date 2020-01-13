package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

func main2() {

	fmt.Println("hello etcd ")

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            []string{"10.194.112.227:2371", "10.194.113.200:2371", "10.194.114.36:2371"},
		DialTimeout:          3 * time.Second,
		DialKeepAliveTime:    1 * time.Second,
		DialKeepAliveTimeout: 3 * time.Second,
	})

	defer func() {
		e := cli.Close()
		if e != nil {
			fmt.Println("close err", err.Error())
		}
	}()

	if err != nil {
		panic(err)
	}

	for i := 0; i < 60; i++ {
		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
		defer cancelFunc()
		response, err := cli.Get(ctx, "/sequence/db/")
		if err != nil {
			fmt.Println("has err :" + err.Error())
		} else {
			fmt.Println(string(response.Kvs[0].Value))
		}

		time.Sleep(1 * time.Second)

		fmt.Println("abccccc")

	}

}
