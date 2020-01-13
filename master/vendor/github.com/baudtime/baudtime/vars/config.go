package vars

import (
	"time"

	"github.com/baudtime/baudtime/util/toml"
)

type EtcdConfig struct {
	Endpoints     []string      `toml:"endpoints"`
	DialTimeout   toml.Duration `toml:"dial_timeout"`
	RWTimeout     toml.Duration `toml:"rw_timeout"`
	RetryNum      int           `toml:"retry_num"`
	RetryInterval toml.Duration `toml:"retry_interval"`
}

type RouteConfig struct {
	ShardGroupTTL          toml.Duration `toml:"shard_group_ttl"`
	ShardGroupTickInterval toml.Duration `toml:"shard_group_tick_interval"`
	HostMinDGBiskLeft      uint64        `toml:"host_min_gb_disk_left"`
}

type AppenderConfig struct {
	AsyncTransfer bool `toml:"async_transfer"`
}

type QueryEngineConfig struct {
	Concurrency int           `toml:"concurrency"`
	Timeout     toml.Duration `toml:"timeout"`
}

type RuleConfig struct {
	EvalInterval toml.Duration `toml:"eval_interval"`
	RuleFileDir  string        `toml:"rules_dir"`
}

type GatewayConfig struct {
	ConnNumPerBackend int                `toml:"conn_num_per_backend"`
	QueryStrategy     string             `toml:"query_strategy"`
	Route             RouteConfig        `toml:"route"`
	Appender          *AppenderConfig    `toml:"appender,omitempty"`
	QueryEngine       *QueryEngineConfig `toml:"query_engine,omitempty"`
	Rule              *RuleConfig        `toml:"rule,omitempty"`
}

type TSDBConfig struct {
	Path              string        `toml:"path"`
	RetentionDuration toml.Duration `toml:"retention_duration"` // Duration of persisted data to keep.
	BlockRanges       []int64       `toml:"block_ranges"`       // The sizes of the Blocks.
	EnableWal         bool          `toml:"enable_wal,omitempty"`
	NoLockfile        bool          `toml:"no_lockfile,omitempty"` // NoLockfile disables creation and consideration of a lock file.
}

type StatReportConfig struct {
	HeartbeartInterval toml.Duration `toml:"heartbeart_interval"`
	SessionExpireTTL   toml.Duration `toml:"session_expire_ttl"`
}

type ReplicationConfig struct {
	HandleOffSize     toml.Size     `toml:"handleoff_size"`
	HeartbeatInterval toml.Duration `toml:"heartbeart_interval"`
}

type StorageConfig struct {
	TSDB        TSDBConfig         `toml:"tsdb"`
	StatReport  StatReportConfig   `toml:"stat_report"`
	Replication *ReplicationConfig `toml:"replication"`
}

type JaegerConfig struct {
	SamplerType       string `toml:"sampler_type"`
	SampleNumPerSec   int    `toml:"sample_num_per_sec"`
	AgentHostPort     string `toml:"agent_host_port"`
	CollectorEndpoint string `toml:"collector_endpoint"`
}

type Config struct {
	TcpPort       string         `toml:"tcp_port"`
	HttpPort      string         `toml:"http_port"`
	MaxConn       int            `toml:"max_conn"`
	NameSpace     string         `toml:"namespace,omitempty"`
	LookbackDelta toml.Duration  `toml:"lookback_delta"`
	Etcd          EtcdConfig     `toml:"etcd"`
	Gateway       *GatewayConfig `toml:"gateway,omitempty"`
	Storage       *StorageConfig `toml:"storage,omitempty"`
	Jaeger        *JaegerConfig  `toml:"jaeger,omitempty"`
}

var Cfg = Config{
	TcpPort:       "8121",
	HttpPort:      "8080",
	MaxConn:       10000,
	NameSpace:     "baudtime",
	LookbackDelta: toml.Duration(5 * time.Second),

	Etcd: EtcdConfig{
		Endpoints:     []string{"localhost:2379"},
		DialTimeout:   toml.Duration(5 * time.Second),
		RWTimeout:     toml.Duration(15 * time.Second),
		RetryNum:      2,
		RetryInterval: toml.Duration(2 * time.Second),
	},
}

func LoadConfig(tomlFile string) error {
	err := toml.LoadFromToml(tomlFile, &Cfg)
	if err != nil {
		return err
	}

	return nil
}
