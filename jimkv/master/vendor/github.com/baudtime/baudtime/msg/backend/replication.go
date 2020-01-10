//go:generate msgp -tests=false

package backend

import (
	"github.com/baudtime/baudtime/msg"
)

type HandshakeStatus byte

const (
	HandshakeStatus_FailedToSync HandshakeStatus = iota
	HandshakeStatus_NoLongerMySlave
	HandshakeStatus_NewSlave
	HandshakeStatus_AlreadyMySlave
)

func (z HandshakeStatus) String() string {
	switch z {
	case HandshakeStatus_FailedToSync:
		return "FailedToSync"
	case HandshakeStatus_NoLongerMySlave:
		return "NoLongerMySlave"
	case HandshakeStatus_NewSlave:
		return "NewSlave"
	case HandshakeStatus_AlreadyMySlave:
		return "AlreadyMySlave"
	}
	return "<Invalid>"
}

type SlaveOfCommand struct {
	MasterAddr string `msg:"masterAddr"`
}

type SyncHandshake struct {
	SlaveAddr    string `msg:"slaveAddr"`
	BlocksMinT   int64  `msg:"blocksMinT"`
	SlaveOfNoOne bool   `msg:"slaveOfNoOne"`
}

type SyncHandshakeAck struct {
	Status     HandshakeStatus `msg:"status"`
	RelationID string          `msg:"relationID"`
	Message    string          `msg:"message"`
}

type BlockSyncOffset struct {
	Ulid   string `msg:"ulid"`
	MinT   int64  `msg:"minT"`
	MaxT   int64  `msg:"maxT"`
	Path   string `msg:"path"`
	Offset int64  `msg:"Offset"`
}

type SyncHeartbeat struct {
	MasterAddr    string           `msg:"masterAddr"`
	SlaveAddr     string           `msg:"slaveAddr"`
	RelationID    string           `msg:"relationID"`
	BlkSyncOffset *BlockSyncOffset `msg:"blkSyncOffset"`
}

type SyncHeartbeatAck struct {
	Status        msg.StatusCode   `msg:"status"`
	Message       string           `msg:"message"`
	BlkSyncOffset *BlockSyncOffset `msg:"blkSyncOffset"`
	Data          []byte           `msg:"data"`
}
