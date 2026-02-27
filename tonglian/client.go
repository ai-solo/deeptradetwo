package tonglian

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"deeptrade/conf"
	"deeptrade/storage"

	"github.com/gorilla/websocket"
)

var (
	client *Client
	clientOnce sync.Once
)

// GetClient returns the singleton TongLian client instance
func GetClient() (*Client, error) {
	var initErr error
	clientOnce.Do(func() {
		cfg := conf.Get().TongLian
		client, initErr = NewClient(cfg.ClientAddress)
	})
	return client, initErr
}

// Client represents a TongLian WebSocket client
type Client struct {
	addr         string
	conn         *websocket.Conn
	state        ConnectionState
	stateMutex   sync.RWMutex
	subscriptions map[string]bool
	subMutex     sync.RWMutex
	messageChan  chan []byte
	done         chan struct{}
	wg           sync.WaitGroup
	retryCount   int
	format       string
}

// NewClient creates a new TongLian WebSocket client
func NewClient(addr string) (*Client, error) {
	if addr == "" {
		addr = conf.Get().TongLian.ClientAddress
	}

	return &Client{
		addr:         addr,
		state:        StateDisconnected,
		subscriptions: make(map[string]bool),
		messageChan:  make(chan []byte, conf.Get().Storage.ChannelBufferSize),
		done:         make(chan struct{}),
		format:       conf.Get().TongLian.DataFormat,
	}, nil
}

// Connect establishes WebSocket connection to TongLian
func (c *Client) Connect() error {
	c.stateMutex.Lock()
	if c.state == StateConnected {
		c.stateMutex.Unlock()
		return nil
	}
	c.state = StateConnecting
	c.stateMutex.Unlock()

	url := fmt.Sprintf("ws://%s", c.addr)
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = time.Duration(conf.Get().TongLian.ConnectionTimeout) * time.Second

	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		c.stateMutex.Lock()
		c.state = StateDisconnected
		c.stateMutex.Unlock()

		// Log to database
		storage.InsertConnectionStatus("error", fmt.Sprintf("连接失败: %v", err))
		return fmt.Errorf("WebSocket连接失败: %w", err)
	}

	c.conn = conn
	c.stateMutex.Lock()
	c.state = StateConnected
	c.stateMutex.Unlock()
	c.retryCount = 0

	log.Printf("[通联] WebSocket连接成功: %s", url)
	storage.InsertConnectionStatus("connected", "连接成功")

	return nil
}

// Disconnect closes the WebSocket connection
func (c *Client) Disconnect() error {
	c.stateMutex.Lock()
	defer c.stateMutex.Unlock()

	if c.conn == nil {
		return nil
	}

	// Signal all goroutines to stop
	close(c.done)

	// Close connection
	if err := c.conn.Close(); err != nil {
		log.Printf("[通联] 关闭连接错误: %v", err)
		return err
	}

	c.conn = nil
	c.state = StateDisconnected

	log.Printf("[通联] WebSocket连接已关闭")
	storage.InsertConnectionStatus("disconnected", "连接已关闭")

	return nil
}

// reconnect implements exponential backoff reconnection logic
func (c *Client) reconnect() error {
	maxRetries := conf.Get().TongLian.MaxRetries
	maxBackoff := time.Duration(conf.Get().TongLian.BackoffMaxMs) * time.Millisecond

	backoff := time.Second
	for c.retryCount < maxRetries {
		c.retryCount++

		log.Printf("[通联] 尝试重连 (%d/%d), 等待 %v", c.retryCount, maxRetries, backoff)
		storage.InsertConnectionStatus("disconnected", fmt.Sprintf("尝试重连 (%d/%d)", c.retryCount, maxRetries))

		time.Sleep(backoff)

		if err := c.Connect(); err == nil {
			// Reconnect successful, resend subscriptions
			if err := c.resendSubscriptions(); err != nil {
				log.Printf("[通联] 重发订阅失败: %v", err)
			}
			return nil
		}

		// Exponential backoff
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return fmt.Errorf("重连失败，已达到最大重试次数: %d", maxRetries)
}

// Start starts the message receiver goroutine
func (c *Client) Start() error {
	if err := c.Connect(); err != nil {
		return err
	}

	c.wg.Add(1)
	go c.receiveLoop()

	return nil
}

// Stop stops the client gracefully
func (c *Client) Stop() error {
	if err := c.Disconnect(); err != nil {
		return err
	}

	c.wg.Wait()
	return nil
}

// receiveLoop continuously receives messages from WebSocket
func (c *Client) receiveLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.done:
			return
		default:
			if c.conn == nil {
				time.Sleep(time.Second)
				continue
			}

			_, message, err := c.conn.ReadMessage()
			if err != nil {
				log.Printf("[通联] 读取消息错误: %v", err)
				storage.InsertConnectionStatus("error", fmt.Sprintf("读取消息错误: %v", err))

				// Attempt reconnection
				if err := c.reconnect(); err != nil {
					log.Printf("[通联] 重连失败: %v", err)
					return
				}
				continue
			}

			// Send message to channel (non-blocking with backpressure handling)
			select {
			case c.messageChan <- message:
				// Message sent successfully
			default:
				// Channel full, log warning
				log.Printf("[通联] 警告: 消息通道已满，丢弃消息")
			}
		}
	}
}

// Subscribe subscribes to market data categories
func (c *Client) Subscribe(categories []string) error {
	c.stateMutex.RLock()
	state := c.state
	c.stateMutex.RUnlock()

	if state != StateConnected {
		return fmt.Errorf("未连接到通联服务器")
	}

	// Check subscription limit
	limit := conf.Get().TongLian.SubscriptionLimit
	c.subMutex.RLock()
	currentCount := len(c.subscriptions)
	c.subMutex.RUnlock()

	if currentCount+len(categories) > limit {
		log.Printf("[通联] 警告: 订阅数量超限 (%d+%d > %d), 截断订阅", currentCount, len(categories), limit)
		// Truncate subscriptions
		available := limit - currentCount
		if available <= 0 {
			return fmt.Errorf("已达到订阅上限: %d", limit)
		}
		categories = categories[:available]
	}

	// Build subscription request
	req := SubscriptionRequest{
		Format:    c.format,
		Subscribe: categories,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化订阅请求失败: %w", err)
	}

	// Send subscription request
	if err := c.conn.WriteMessage(websocket.TextMessage, reqData); err != nil {
		return fmt.Errorf("发送订阅请求失败: %w", err)
	}

	// Wait for response (simplified - should use proper message correlation)
	// For now, just mark as subscribed
	c.subMutex.Lock()
	for _, cat := range categories {
		c.subscriptions[cat] = true
	}
	c.subMutex.Unlock()

	log.Printf("[通联] 发送订阅请求: %d 个类别", len(categories))
	return nil
}

// Unsubscribe unsubscribes from market data categories
func (c *Client) Unsubscribe(categories []string) error {
	c.subMutex.Lock()
	defer c.subMutex.Unlock()

	for _, cat := range categories {
		delete(c.subscriptions, cat)
	}

	// Send new subscription request without these categories
	allCategories := make([]string, 0, len(c.subscriptions))
	for cat := range c.subscriptions {
		allCategories = append(allCategories, cat)
	}

	req := SubscriptionRequest{
		Format:    c.format,
		Subscribe: allCategories,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化取消订阅请求失败: %w", err)
	}

	if c.conn != nil {
		if err := c.conn.WriteMessage(websocket.TextMessage, reqData); err != nil {
			return fmt.Errorf("发送取消订阅请求失败: %w", err)
		}
	}

	log.Printf("[通联] 取消订阅: %d 个类别", len(categories))
	return nil
}

// resendSubscriptions resends all active subscriptions after reconnection
func (c *Client) resendSubscriptions() error {
	c.subMutex.RLock()
	categories := make([]string, 0, len(c.subscriptions))
	for cat := range c.subscriptions {
		categories = append(categories, cat)
	}
	c.subMutex.RUnlock()

	if len(categories) == 0 {
		return nil
	}

	return c.Subscribe(categories)
}

// GetState returns the current connection state
func (c *Client) GetState() ConnectionState {
	c.stateMutex.RLock()
	defer c.stateMutex.RUnlock()
	return c.state
}

// GetMessageChannel returns the message channel
func (c *Client) GetMessageChannel() <-chan []byte {
	return c.messageChan
}

// GetSubscriptionCount returns the number of active subscriptions
func (c *Client) GetSubscriptionCount() int {
	c.subMutex.RLock()
	defer c.subMutex.RUnlock()
	return len(c.subscriptions)
}
