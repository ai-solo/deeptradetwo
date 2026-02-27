package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var (
	totalMessages int64
	totalErrors   int64
)

type TongLianMessage struct {
	Sid  int    `json:"sid"`
	Mid  int    `json:"mid"`
	Lt   int    `json:"lt"`
	Data string `json:"data"`
}

type BondData struct {
	SecurityID    string  `json:"SecurityID"`
	SecurityName  string  `json:"SecurityName"`
	PreClosePrice float64 `json:"PreClosePrice"`
	HighLimitPrice float64 `json:"HighLimitPrice"`
	LowLimitPrice  float64 `json:"LowLimitPrice"`
	UpdateTime     string  `json:"UpdateTime"`
}

func main() {
	log.Println("========================================")
	log.Println("通联数据实时测试工具 (JSON格式)")
	log.Println("========================================")

	// 服务器地址
	serverURL := "ws://47.101.149.89:9020"
	if len(os.Args) > 1 {
		serverURL = os.Args[1]
	}

	log.Printf("连接到: %s", serverURL)
	log.Println("订阅: 4.23.* (上交所债券)")
	log.Println("      6.51.* (深交所债券)")
	log.Println("      6.54.* (深交所债券)")
	log.Println("")

	// 连接 WebSocket
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	conn, _, err := dialer.Dial(serverURL, nil)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()

	log.Println("✓ 连接成功")

	// 发送订阅请求（JSON格式）
	subscribeReq := map[string]interface{}{
		"format": "json",
		"subscribe": []string{
			"4.23.*",
			"6.51.*",
			"6.54.*",
		},
	}

	reqData, _ := json.Marshal(subscribeReq)
	if err := conn.WriteMessage(websocket.TextMessage, reqData); err != nil {
		log.Fatalf("发送订阅请求失败: %v", err)
	}

	log.Println("✓ 订阅请求已发送")
	log.Println("")
	log.Println("----------------------------------------")
	log.Println("等待数据... (按 Ctrl+C 退出)")
	log.Println("----------------------------------------")
	log.Println("")

	// 启动统计打印器
	go printStats()

	// 设置中断信号处理
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// 启动消息接收循环
	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("连接错误: %v", err)
					atomic.AddInt64(&totalErrors, 1)
				}
				return
			}

			if messageType == websocket.TextMessage {
				atomic.AddInt64(&totalMessages, 1)

				// 解析消息
				var tlMsg TongLianMessage
				if err := json.Unmarshal(message, &tlMsg); err != nil {
					log.Printf("[解析错误] %s", string(message))
					atomic.AddInt64(&totalErrors, 1)
					continue
				}

				// 打印实时数据
				printRealtimeData(&tlMsg, string(message))
			}
		}
	}()

	// 等待中断信号
	select {
	case <-interrupt:
		log.Println("\n")
		log.Println("========================================")
		log.Println("收到退出信号，正在关闭...")
	case <-done:
		log.Println("\n")
		log.Println("========================================")
		log.Println("连接已关闭")
	}

	// 优雅关闭
	conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	time.Sleep(time.Second)

	// 打印最终统计
	finalStats()
}

func printRealtimeData(tlMsg *TongLianMessage, rawMsg string) {
	timestamp := time.Now().Format("15:04:05.000")

	var marketType string
	switch tlMsg.Sid {
	case 4:
		marketType = "上交所债券"
	case 6:
		marketType = "深交所债券"
	default:
		marketType = fmt.Sprintf("市场%d", tlMsg.Sid)
	}

	// 尝试解析数据字段
	var bondData BondData
	if err := json.Unmarshal([]byte(tlMsg.Data), &bondData); err == nil {
		// JSON 格式数据
		fmt.Printf("[%s] %s | %s | %s | 价格: %.3f | 涨停: %.3f | 跌停: %.3f\n",
			timestamp,
			marketType,
			bondData.SecurityID,
			bondData.SecurityName,
			bondData.PreClosePrice,
			bondData.HighLimitPrice,
			bondData.LowLimitPrice,
		)
	} else {
		// 无法解析，显示原始数据
		fmt.Printf("[%s] %s | Message %d | Raw: %s\n",
			timestamp,
			marketType,
			tlMsg.Mid,
			tlMsg.Data[:min(len(tlMsg.Data), 100)],
		)
	}
}

func printStats() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		total := atomic.LoadInt64(&totalMessages)
		errors := atomic.LoadInt64(&totalErrors)

		if total > 0 || errors > 0 {
			log.Printf("[统计] 已接收: %d 条 | 错误: %d 条\n", total, errors)
		}
	}
}

func finalStats() {
	total := atomic.LoadInt64(&totalMessages)
	errors := atomic.LoadInt64(&totalErrors)

	log.Println("========================================")
	log.Printf("统计信息:")
	log.Printf("  总消息数: %d", total)
	log.Printf("  错误数: %d", errors)
	if total > 0 {
		log.Printf("  成功率: %.2f%%", float64(total-errors)*100/float64(total))
	}
	log.Println("========================================")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
