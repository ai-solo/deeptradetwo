package tonglian

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"deeptrade/conf"
	"deeptrade/storage"
)

var (
	service *Service
	serviceOnce sync.Once
)

// GetService returns the singleton service instance
func GetService() (*Service, error) {
	var initErr error
	serviceOnce.Do(func() {
		service, initErr = NewService()
	})
	return service, initErr
}

// Service represents the main TongLian data ingestion service
type Service struct {
	client   *Client
	pipeline *Pipeline
	running  bool
 mutex    sync.RWMutex
}

// NewService creates a new service instance
func NewService() (*Service, error) {
	log.Println("[服务] 初始化通联数据采集服务")

	// Initialize storage
	if err := InitStorage(); err != nil {
		return nil, err
	}

	// Create client
	client, err := GetClient()
	if err != nil {
		return nil, err
	}

	// Create pipeline
	pipeline := NewPipeline(client)

	return &Service{
		client:   client,
		pipeline: pipeline,
		running:  false,
	}, nil
}

// InitStorage initializes storage connections
func InitStorage() error {
	log.Println("[服务] 初始化存储层")

	// Initialize MySQL
	if err := storage.InitMySQL(); err != nil {
		return err
	}

	// Initialize Redis
	if err := storage.InitRedis(); err != nil {
		return err
	}

	return nil
}

// Start starts the service
func (s *Service) Start() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.running {
		log.Println("[服务] 服务已在运行中")
		return nil
	}

	log.Println("[服务] 启动通联数据采集服务")

	// Load configuration
	cfg := conf.Get()
	log.Printf("[服务] 配置: 通联地址=%s, 数据格式=%s, 订阅限制=%d",
		cfg.TongLian.ClientAddress,
		cfg.TongLian.DataFormat,
		cfg.TongLian.SubscriptionLimit)
	log.Printf("[服务] MySQL: %s@%s:%d/%s",
		cfg.Storage.MySQL.User,
		cfg.Storage.MySQL.Host,
		cfg.Storage.MySQL.Port,
		cfg.Storage.MySQL.Database)
	log.Printf("[服务] Redis: %s:%d DB:%d",
		cfg.Storage.Redis.Host,
		cfg.Storage.Redis.Port,
		cfg.Storage.Redis.DB)

	// Load subscriptions from database
	if err := s.loadSubscriptions(); err != nil {
		log.Printf("[服务] 警告: 加载订阅失败: %v", err)
	}

	// Start client
	if err := s.client.Start(); err != nil {
		return err
	}

	// Start pipeline
	if err := s.pipeline.Start(); err != nil {
		s.client.Stop()
		return err
	}

	s.running = true
	log.Println("[服务] 通联数据采集服务已启动")

	return nil
}

// Stop stops the service gracefully
func (s *Service) Stop() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return nil
	}

	log.Println("[服务] 停止通联数据采集服务")

	// Stop pipeline first (drain message queues)
	if err := s.pipeline.Stop(); err != nil {
		log.Printf("[服务] 停止管道错误: %v", err)
	}

	// Stop client
	if err := s.client.Stop(); err != nil {
		log.Printf("[服务] 停止客户端错误: %v", err)
	}

	// Close storage connections
	if err := storage.CloseMySQL(); err != nil {
		log.Printf("[服务] 关闭MySQL连接错误: %v", err)
	}

	if err := storage.CloseRedis(); err != nil {
		log.Printf("[服务] 关闭Redis连接错误: %v", err)
	}

	s.running = false
	log.Println("[服务] 通联数据采集服务已停止")

	return nil
}

// Restart restarts the service
func (s *Service) Restart() error {
	log.Println("[服务] 重启通联数据采集服务")

	if err := s.Stop(); err != nil {
		return err
	}

	return s.Start()
}

// loadSubscriptions loads active subscriptions from database
func (s *Service) loadSubscriptions() error {
	subs, err := storage.GetActiveSubscriptions()
	if err != nil {
		return err
	}

	if len(subs) == 0 {
		log.Println("[服务] 没有找到活动订阅")
		return nil
	}

	categories := make([]string, 0, len(subs))
	for _, sub := range subs {
		categories = append(categories, sub.CategoryID)
	}

	log.Printf("[服务] 从数据库加载 %d 个订阅", len(categories))
	return s.client.Subscribe(categories)
}

// AddSubscription adds a new subscription
func (s *Service) AddSubscription(categoryID, securityID, securityName string, sid, mid int) error {
	// Add to database
	if err := storage.AddSubscription(categoryID, securityID, securityName, sid, mid); err != nil {
		return err
	}

	// Add to Redis
	if err := storage.AddSubscriptionToRedis(categoryID); err != nil {
		return err
	}

	// Subscribe via WebSocket
	return s.client.Subscribe([]string{categoryID})
}

// RemoveSubscription removes a subscription
func (s *Service) RemoveSubscription(categoryID string) error {
	// Remove from database
	if err := storage.RemoveSubscription(categoryID); err != nil {
		return err
	}

	// Remove from Redis
	if err := storage.RemoveSubscriptionFromRedis(categoryID); err != nil {
		return err
	}

	// Unsubscribe via WebSocket
	return s.client.Unsubscribe([]string{categoryID})
}

// IsRunning returns whether the service is running
func (s *Service) IsRunning() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.running
}

// GetMetrics returns pipeline metrics
func (s *Service) GetMetrics() Metrics {
	return s.pipeline.GetMetrics()
}

// WaitForShutdown waits for shutdown signals
func (s *Service) WaitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("[服务] 收到信号: %v, 开始关闭...", sig)

	if err := s.Stop(); err != nil {
		log.Printf("[服务] 关闭失败: %v", err)
		os.Exit(1)
	}

	log.Println("[服务] 服务已正常关闭")
	os.Exit(0)
}
