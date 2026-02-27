package main

import (
	"log"

	"deeptrade/tonglian"
)

func main() {
	log.Println("========================================")
	log.Println("通联数据采集服务")
	log.Println("========================================")

	// Get service instance
	service, err := tonglian.GetService()
	if err != nil {
		log.Fatalf("初始化服务失败: %v", err)
	}

	// Start service
	if err := service.Start(); err != nil {
		log.Fatalf("启动服务失败: %v", err)
	}

	// Add subscriptions based on actual Token permissions (Bond market data)
	log.Println("[服务] 添加债券市场订阅...")

	// Token has bond market permissions (ServiceID 4=SH, 6=SZ)
	subscriptions := []struct {
		categoryID string
		securityID string
		name       string
		sid        int
		mid        int
	}{
		{"4.23.*", "", "上交所债券快照", 4, 23},
		{"6.51.*", "", "深交所债券快照", 6, 51},
		{"6.54.*", "", "深交所债券其他", 6, 54},
	}

	for _, sub := range subscriptions {
		if err := service.AddSubscription(sub.categoryID, sub.securityID, sub.name, sub.sid, sub.mid); err != nil {
			log.Printf("[服务] 警告: 添加订阅 %s 失败: %v", sub.categoryID, err)
		} else {
			log.Printf("[服务] ✓ 添加订阅: %s", sub.categoryID)
		}
	}

	log.Println("服务正在运行，按 Ctrl+C 停止")

	// Wait for shutdown signal
	service.WaitForShutdown()
}
