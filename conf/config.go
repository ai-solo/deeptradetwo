package conf

import (
	"os"
	"sync"

	"github.com/8treenet/freedom"
)

func init() {
	EntryPoint()
}

// Get .
func Get() *Configuration {
	once.Do(func() {
		cfg = newConfig()
	})
	return cfg
}

var once sync.Once
var cfg *Configuration

// Configuration .
type Configuration struct {
	Binance  BinanceConf  `toml:"binance" yaml:"binance"`
	LLM      []LLMConf    `toml:"llm" yaml:"llm"`
	Trading  TradingConf  `toml:"trading" yaml:"trading"`
	TongLian TongLianConf `toml:"tonglian" yaml:"tonglian"`
	Storage  StorageConf  `toml:"storage" yaml:"storage"`
}

// GetBinanceEnvironment 获取当前环境的币安配置
func (cg *Configuration) GetBinanceEnvironment() BinanceEnvironment {
	// 检查配置是否为空
	if cg.Binance.CurrentEnvironment == "production" {
		// 返回默认的模拟盘配置
		return cg.Binance.BinanceEnvironmentProduction
	}

	return cg.Binance.BinanceEnvironmentTest
}

// GetLLM 获取llm
func (cg *Configuration) GetLLM(trackEnable ...bool) (result LLMConf) {
	if len(trackEnable) > 0 && trackEnable[0] {
		for _, v := range cg.LLM {
			if v.TrackEnable {
				result = v
				return
			}
		}
	}

	for _, v := range cg.LLM {
		if v.EntryEnable {
			result = v
			return
		}
	}
	panic("LLM undefined")
}

// DBConf .
type DBConf struct {
	Addr            string `toml:"addr" yaml:"addr"`
	MaxOpenConns    int    `toml:"max_open_conns" yaml:"max_open_conns"`
	MaxIdleConns    int    `toml:"max_idle_conns" yaml:"max_idle_conns"`
	ConnMaxLifeTime int    `toml:"conn_max_life_time" yaml:"conn_max_life_time"`
}

// RedisConf .
type RedisConf struct {
	Addr               string `toml:"addr" yaml:"addr"`
	Password           string `toml:"password" yaml:"password"`
	DB                 int    `toml:"db" yaml:"db"`
	MaxRetries         int    `toml:"max_retries" yaml:"max_retries"`
	PoolSize           int    `toml:"pool_size" yaml:"pool_size"`
	ReadTimeout        int    `toml:"read_timeout" yaml:"read_timeout"`
	WriteTimeout       int    `toml:"write_timeout" yaml:"write_timeout"`
	IdleTimeout        int    `toml:"idle_timeout" yaml:"idle_timeout"`
	IdleCheckFrequency int    `toml:"idle_check_frequency" yaml:"idle_check_frequency"`
	MaxConnAge         int    `toml:"max_conn_age" yaml:"max_conn_age"`
	PoolTimeout        int    `toml:"pool_timeout" yaml:"pool_timeout"`
}

// BinanceConf 币安交易配置
type BinanceConf struct {
	// 当前环境: testnet, production
	CurrentEnvironment string `toml:"current_environment" yaml:"current_environment"`
	// 默认代理设置
	DefaultProxy                 string             `toml:"default_proxy" yaml:"default_proxy"`
	BinanceEnvironmentTest       BinanceEnvironment `toml:"testnet" yaml:"testnet"`
	BinanceEnvironmentProduction BinanceEnvironment `toml:"production" yaml:"production"`
	ProxyURL                     string             `toml:"proxy_url" yaml:"proxy_url"`
	Timeout                      int                `toml:"timeout" yaml:"timeout"`
	MaxRetries                   int                `toml:"max_retries" yaml:"max_retries"`
}

// BinanceEnvironment 币安环境配置
type BinanceEnvironment struct {
	Name             string `toml:"name" yaml:"name"`
	APIKey           string `toml:"api_key" yaml:"api_key"`
	SecretKey        string `toml:"secret_key" yaml:"secret_key"`
	FuturesBaseURL   string `toml:"futures_base_url" yaml:"futures_base_url"`
	FuturesStreamURL string `toml:"futures_stream_url" yaml:"futures_stream_url"`
	Debug            bool   `toml:"debug" yaml:"debug"`
}

// LLMConf LLM 模型配置
type LLMConf struct {
	APIKey      string `toml:"api_key" yaml:"api_key"`
	Model       string `toml:"model" yaml:"model"`
	BaseURL     string `toml:"base_url" yaml:"base_url"`
	EntryEnable bool   `toml:"entry_enable" yaml:"entry_enable"`
	TrackEnable bool   `toml:"track_enable" yaml:"track_enable"`
	Extra       string `toml:"extra" yaml:"extra"`
}

// TradingConf 交易相关配置
type TradingConf struct {
	// 固定仓位百分比，例如 20 表示使用 20% 的资金作为保证金
	PositionPercent float64 `toml:"position_percent" yaml:"position_percent"`
	TriggerTime     int     `toml:"trigger_time" yaml:"trigger_time"`
}

// TongLianConf 通联数据源配置
type TongLianConf struct {
	// 通联客户端地址
	ClientAddress string `toml:"client_address" yaml:"client_address"`
	// 数据格式: json 或 csv
	DataFormat string `toml:"data_format" yaml:"data_format"`
	// 最大重试次数
	MaxRetries int `toml:"max_retries" yaml:"max_retries"`
	// 最大退避时间(毫秒)
	BackoffMaxMs int `toml:"backoff_max_ms" yaml:"backoff_max_ms"`
	// 订阅限制
	SubscriptionLimit int `toml:"subscription_limit" yaml:"subscription_limit"`
	// 连接超时时间(秒)
	ConnectionTimeout int `toml:"connection_timeout" yaml:"connection_timeout"`
}

// StorageConf 存储配置
type StorageConf struct {
	MySQL MySQLStorageConf `toml:"mysql" yaml:"mysql"`
	Redis RedisStorageConf `toml:"redis" yaml:"redis"`
	// 数据保留策略
	Retention RetentionConf `toml:"retention" yaml:"retention"`
	// 批处理大小
	BatchSizes BatchSizeConf `toml:"batch_sizes" yaml:"batch_sizes"`
	// 通道缓冲区大小
	ChannelBufferSize int `toml:"channel_buffer_size" yaml:"channel_buffer_size"`
}

// MySQLStorageConf MySQL存储配置
type MySQLStorageConf struct {
	Host     string `toml:"host" yaml:"host"`
	Port     int    `toml:"port" yaml:"port"`
	User     string `toml:"user" yaml:"user"`
	Password string `toml:"password" yaml:"password"`
	Database string `toml:"database" yaml:"database"`
	// 连接池配置
	MaxOpenConns    int `toml:"max_open_conns" yaml:"max_open_conns"`
	MaxIdleConns    int `toml:"max_idle_conns" yaml:"max_idle_conns"`
	ConnMaxLifeTime int `toml:"conn_max_life_time" yaml:"conn_max_life_time"`
}

// RedisStorageConf Redis存储配置
type RedisStorageConf struct {
	Host     string `toml:"host" yaml:"host"`
	Port     int    `toml:"port" yaml:"port"`
	Password string `toml:"password" yaml:"password"`
	DB       int    `toml:"db" yaml:"db"`
	// 连接池配置
	PoolSize int `toml:"pool_size" yaml:"pool_size"`
}

// RetentionConf 数据保留策略配置
type RetentionConf struct {
	// 快照数据保留时间(分钟)
	SnapshotMinutes int `toml:"snapshot_minutes" yaml:"snapshot_minutes"`
	// 1分钟K线保留时间(小时)
	Kline1mHours int `toml:"kline_1m_hours" yaml:"kline_1m_hours"`
	// 5分钟K线保留时间(天)
	Kline5mDays int `toml:"kline_5m_days" yaml:"kline_5m_days"`
	// 1小时K线保留时间(天)
	Kline1hDays int `toml:"kline_1h_days" yaml:"kline_1h_days"`
}

// BatchSizeConf 批处理大小配置
type BatchSizeConf struct {
	// MySQL批插入大小
	MySQLInsertBatch int `toml:"mysql_insert_batch" yaml:"mysql_insert_batch"`
	// MySQL刷盘间隔(秒)
	MySQLFlushInterval int `toml:"mysql_flush_interval" yaml:"mysql_flush_interval"`
}

func newConfig() *Configuration {
	result := &Configuration{}
	err := freedom.Configure(&result, "config.toml")
	// err := freedom.Configure(&result, "config.yaml")
	if err != nil {
		panic(err)
	}
	return result
}

// EntryPoint .
func EntryPoint() {
	envConfig := os.Getenv("deeptrade_conf")
	if envConfig != "" {
		os.Setenv(freedom.ProfileENV, envConfig)
		return
	}

	// [./base -c ./server/conf]
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] != "-c" {
			continue
		}
		if i+1 >= len(os.Args) {
			break
		}
		os.Setenv(freedom.ProfileENV, os.Args[i+1])
	}
}
