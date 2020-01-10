package client

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jimdb-org/jimdb/jimkv/master/entity/pkg/dspb"
	"time"

	"golang.org/x/net/context"
)

// AdminClient admin client
type AdminClient interface {
	// Close should release all data.
	Close() error

	// SetConfig set config
	SetConfig(addr string, configs []*dspb.ConfigItem) error

	// GetConfig get config
	//GetConfig(addr string, keys []*dspb.ConfigKey) (*dspb.GetConfigResponse, error)
}

type adminClient struct {
	token string
	pool  *ResourcePool
}

// NewAdminClient new admin client
func NewAdminClient(token string, poolSize int) AdminClient {
	return &adminClient{
		token: token,
		pool:  NewResourcePool(poolSize),
	}
}

// Close should release all data.
func (c *adminClient) Close() error {
	c.pool.Close()
	return nil
}

func (c *adminClient) signAuth() *dspb.AdminAuth {
	m := md5.New()
	epoch := time.Now().Unix()
	m.Write([]byte(fmt.Sprintf("%d", epoch)))
	m.Write([]byte(c.token))
	return &dspb.AdminAuth{
		Epoch: epoch,
		Sign:  hex.EncodeToString(m.Sum(nil)),
	}
}

func (c *adminClient) newRequest() *dspb.AdminRequest {
	return &dspb.AdminRequest{
		Auth: c.signAuth(),
	}
}

func (c *adminClient) getConn(addr string) (RpcClient, error) {
	if len(addr) == 0 {
		return nil, errors.New("invalid address")
	}
	return c.pool.GetConn(addr)
}

func (c *adminClient) send(addr string, req *dspb.AdminRequest) (*dspb.AdminResponse, error) {
	conn, err := c.getConn(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), ReadTimeoutShort)
	defer cancel()
	resp, err := conn.Admin(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != 0 {
		return nil, fmt.Errorf("code=<%d>: %s", resp.GetCode(), resp.GetErrorMsg())
	}
	return resp, nil
}

// SetConfig set config
func (c *adminClient) SetConfig(addr string, configs []*dspb.ConfigItem) error {
	req := c.newRequest()
	req.Req = &dspb.AdminRequest_SetCfg{
		SetCfg: &dspb.SetConfigRequest{
			Configs: configs,
		},
	}

	_, err := c.send(addr, req)
	return err
}
