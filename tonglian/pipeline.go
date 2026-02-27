package tonglian

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"deeptrade/conf"
	"deeptrade/storage"
)

// Helper functions to encode arrays to JSON strings for Level 2 data
func encodeFloatArray(arr []float64) string {
	if arr == nil || len(arr) == 0 {
		return ""
	}
	data, _ := json.Marshal(arr)
	return string(data)
}

func encodeIntArray(arr []int64) string {
	if arr == nil || len(arr) == 0 {
		return ""
	}
	data, _ := json.Marshal(arr)
	return string(data)
}

// Pipeline manages the async data processing pipeline
type Pipeline struct {
	client          *Client
	rawMessageChan  chan []byte
	parsedDataChan  chan *MarketData
	snapshotBuffer  []storage.MarketSnapshot
	klineBuffer     []storage.Kline
	bucketMutex     sync.RWMutex
	buckets         map[string]*KlineBucket // key: securityID:timeframe
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	metrics         Metrics
	metricsMutex    sync.Mutex
}

// Metrics tracks pipeline metrics
type Metrics struct {
	MessagesReceived    int64
	MessagesParsed      int64
	ParseErrors         int64
	RedisWrites         int64
	RedisErrors         int64
	MySQLInserts        int64
	MySQLErrors         int64
	ChannelDepth        int
	LastUpdateTime      time.Time
}

// NewPipeline creates a new pipeline instance
func NewPipeline(client *Client) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())

	return &Pipeline{
		client:         client,
		rawMessageChan: make(chan []byte, conf.Get().Storage.ChannelBufferSize),
		parsedDataChan: make(chan *MarketData, conf.Get().Storage.ChannelBufferSize),
		snapshotBuffer: make([]storage.MarketSnapshot, 0, conf.Get().Storage.BatchSizes.MySQLInsertBatch),
		klineBuffer:    make([]storage.Kline, 0, conf.Get().Storage.BatchSizes.MySQLInsertBatch),
		buckets:        make(map[string]*KlineBucket),
		ctx:            ctx,
		cancel:         cancel,
		metrics:        Metrics{LastUpdateTime: time.Now()},
	}
}

// Start starts the pipeline goroutines
func (p *Pipeline) Start() error {
	log.Printf("[管道] 启动数据处理管道")

	// Start parser goroutine
	p.wg.Add(1)
	go p.parserLoop()

	// Start storage writer goroutine
	p.wg.Add(1)
	go p.storageWriterLoop()

	// Start metrics logger goroutine
	p.wg.Add(1)
	go p.metricsLogger()

	// Start channel forwarder (from client to pipeline)
	p.wg.Add(1)
	go p.channelForwarder()

	return nil
}

// Stop gracefully stops the pipeline
func (p *Pipeline) Stop() error {
	log.Printf("[管道] 停止数据处理管道")

	// Cancel context
	p.cancel()

	// Close channels
	close(p.rawMessageChan)
	close(p.parsedDataChan)

	// Flush remaining data
	p.flushSnapshots()
	p.flushKlines()

	// Wait for goroutines to finish
	p.wg.Wait()

	log.Printf("[管道] 数据处理管道已停止")
	return nil
}

// channelForwarder forwards messages from client to pipeline
func (p *Pipeline) channelForwarder() {
	defer p.wg.Done()

	clientChan := p.client.GetMessageChannel()

	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, ok := <-clientChan:
			if !ok {
				return
			}

			p.metricsMutex.Lock()
			p.metrics.MessagesReceived++
			p.metricsMutex.Unlock()

			select {
			case p.rawMessageChan <- msg:
				// Message forwarded
			default:
				log.Printf("[管道] 警告: 原始消息通道已满")
			}
		}
	}
}

// parserLoop parses raw messages and forwards to storage
func (p *Pipeline) parserLoop() {
	defer p.wg.Done()

	dataFormat := conf.Get().TongLian.DataFormat

	for {
		select {
		case <-p.ctx.Done():
			return

		case rawMsg, ok := <-p.rawMessageChan:
			if !ok {
				return
			}

			// Parse message
			var data *MarketData
			var err error

			if dataFormat == "csv" {
				data, err = ParseCSVMessage(rawMsg)
			} else {
				// For JSON format, check if it's a Level 2 message first
				// Parse outer message to get Sid/Mid
				var tlMsg TongLianMessage
				if jsonErr := json.Unmarshal(rawMsg, &tlMsg); jsonErr != nil {
					err = jsonErr
				} else {
					// Use Level 2 parser for Level 2 messages
					if IsLevel2Message(tlMsg.Sid, tlMsg.Mid) {
						data, err = ParseLevel2JSONMessage(rawMsg)
					} else {
						data, err = ParseJSONMessage(rawMsg)
					}
				}
			}

			if err != nil {
				p.metricsMutex.Lock()
				p.metrics.ParseErrors++
				p.metricsMutex.Unlock()
				log.Printf("[管道] 解析错误: %v", err)
				continue
			}

			p.metricsMutex.Lock()
			p.metrics.MessagesParsed++
			p.metricsMutex.Unlock()

			// Forward to storage
			select {
			case p.parsedDataChan <- data:
				// Data forwarded
			default:
				log.Printf("[管道] 警告: 解析数据通道已满")
			}
		}
	}
}

// storageWriterLoop writes parsed data to Redis and MySQL
func (p *Pipeline) storageWriterLoop() {
	defer p.wg.Done()

	flushTicker := time.NewTicker(time.Duration(conf.Get().Storage.BatchSizes.MySQLFlushInterval) * time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return

		case data, ok := <-p.parsedDataChan:
			if !ok {
				return
			}

			// Write to Redis
			if err := p.writeToRedis(data); err != nil {
				p.metricsMutex.Lock()
				p.metrics.RedisErrors++
				p.metricsMutex.Unlock()
				log.Printf("[管道] Redis写入错误: %v", err)
			} else {
				p.metricsMutex.Lock()
				p.metrics.RedisWrites++
				p.metricsMutex.Unlock()
			}

			// Aggregate to snapshot buffer
			p.addToSnapshotBuffer(data)

			// Aggregate to K-line buckets
			p.aggregateKline(data)

		case <-flushTicker.C:
			// Periodic flush
			p.flushSnapshots()
			p.flushKlines()
		}
	}
}

// writeToRedis writes market data to Redis
func (p *Pipeline) writeToRedis(data *MarketData) error {
	// Cache snapshot
	timestamp := data.UpdateTime
	if timestamp == 0 {
		timestamp = data.LocalTime.Unix()
	}

	return storage.CacheMarketSnapshot(data.SecurityID, data, timestamp)
}

// addToSnapshotBuffer adds data to snapshot buffer
func (p *Pipeline) addToSnapshotBuffer(data *MarketData) {
	snapshot := storage.MarketSnapshot{
		SecurityID:   data.SecurityID,
		SecurityName: data.SecurityName,
		Sid:          data.Sid,
		Mid:          data.Mid,
		LastPrice:    data.LastPrice,
		Volume:       data.Volume,
		Turnover:     data.Turnover,
		UpdateTime:   data.UpdateTime,
		LocalTime:    data.LocalTime.Unix(),
		// Level 2 fields
		AskPrices:    encodeFloatArray(data.AskPrices),
		BidPrices:    encodeFloatArray(data.BidPrices),
		AskVolumes:   encodeIntArray(data.AskVolumes),
		BidVolumes:   encodeIntArray(data.BidVolumes),
	}

	p.snapshotBuffer = append(p.snapshotBuffer, snapshot)

	// Check if buffer is full
	batchSize := conf.Get().Storage.BatchSizes.MySQLInsertBatch
	if len(p.snapshotBuffer) >= batchSize {
		p.flushSnapshots()
	}
}

// flushSnapshots flushes snapshot buffer to MySQL
func (p *Pipeline) flushSnapshots() {
	if len(p.snapshotBuffer) == 0 {
		return
	}

	if err := storage.BatchInsertSnapshots(p.snapshotBuffer); err != nil {
		p.metricsMutex.Lock()
		p.metrics.MySQLErrors++
		p.metricsMutex.Unlock()
		log.Printf("[管道] MySQL批量插入快照失败: %v", err)
	} else {
		p.metricsMutex.Lock()
		p.metrics.MySQLInserts += int64(len(p.snapshotBuffer))
		p.metricsMutex.Unlock()
	}

	p.snapshotBuffer = p.snapshotBuffer[:0]
}

// aggregateKline aggregates data into K-line buckets
func (p *Pipeline) aggregateKline(data *MarketData) {
	timeframes := []string{"1m", "5m", "15m", "1h"}

	for _, tf := range timeframes {
		bucketKey := data.SecurityID + ":" + tf
		openTime := GetBucketOpenTime(data.UpdateTime, tf)

		p.bucketMutex.Lock()
		bucket, exists := p.buckets[bucketKey]
		if !exists || bucket.OpenTime != openTime {
			// Save old bucket if exists
			if exists {
				p.klineBuffer = append(p.klineBuffer, *convertBucketToKline(bucket))
			}
			// Create new bucket
			bucket = NewKlineBucket(data.SecurityID, tf, openTime)
			p.buckets[bucketKey] = bucket
		}

		// Update bucket with new tick
		bucket.Update(data.LastPrice, data.Volume, data.Turnover)
		p.bucketMutex.Unlock()
	}
}

// flushKlines flushes K-line buffer to MySQL
func (p *Pipeline) flushKlines() {
	p.bucketMutex.Lock()
	// Flush all active buckets
	for _, bucket := range p.buckets {
		p.klineBuffer = append(p.klineBuffer, *convertBucketToKline(bucket))
	}
	p.buckets = make(map[string]*KlineBucket)
	p.bucketMutex.Unlock()

	if len(p.klineBuffer) == 0 {
		return
	}

	if err := storage.BatchInsertKlines(p.klineBuffer); err != nil {
		p.metricsMutex.Lock()
		p.metrics.MySQLErrors++
		p.metricsMutex.Unlock()
		log.Printf("[管道] MySQL批量插入K线失败: %v", err)
	} else {
		p.metricsMutex.Lock()
		p.metrics.MySQLInserts += int64(len(p.klineBuffer))
		p.metricsMutex.Unlock()
	}

	p.klineBuffer = p.klineBuffer[:0]
}

// convertBucketToKline converts a KlineBucket to storage.Kline
func convertBucketToKline(bucket *KlineBucket) *storage.Kline {
	return &storage.Kline{
		SecurityID:  bucket.SecurityID,
		Timeframe:   bucket.Timeframe,
		OpenTime:    bucket.OpenTime,
		CloseTime:   bucket.CloseTime,
		OpenPrice:   bucket.OpenPrice,
		HighPrice:   bucket.HighPrice,
		LowPrice:    bucket.LowPrice,
		ClosePrice:  bucket.ClosePrice,
		Volume:      bucket.Volume,
		Turnover:    bucket.Turnover,
		TradeCount:  bucket.TradeCount,
	}
}

// metricsLogger logs metrics periodically
func (p *Pipeline) metricsLogger() {
	defer p.wg.Done()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.metricsMutex.Lock()
			p.metrics.ChannelDepth = len(p.parsedDataChan)
			p.metrics.LastUpdateTime = time.Now()

			log.Printf("[管道] 指标: 接收=%d 解析=%d 解析错误=%d Redis写入=%d Redis错误=%d MySQL插入=%d MySQL错误=%d 通道深度=%d",
				p.metrics.MessagesReceived,
				p.metrics.MessagesParsed,
				p.metrics.ParseErrors,
				p.metrics.RedisWrites,
				p.metrics.RedisErrors,
				p.metrics.MySQLInserts,
				p.metrics.MySQLErrors,
				p.metrics.ChannelDepth,
			)
			p.metricsMutex.Unlock()
		}
	}
}

// GetMetrics returns current metrics
func (p *Pipeline) GetMetrics() Metrics {
	p.metricsMutex.Lock()
	defer p.metricsMutex.Unlock()
	return p.metrics
}
