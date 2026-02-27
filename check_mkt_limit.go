package main

import (
	"fmt"
	"log"
	
	"deeptrade/conf"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	cfg := conf.Get().Storage.MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}

	// 查看mkt_limit表结构
	type Column struct {
		Field string
		Type  string
	}
	var columns []Column
	db.Raw("DESCRIBE mkt_limit").Scan(&columns)
	fmt.Println("mkt_limit 表结构:")
	for _, col := range columns {
		fmt.Printf("  %s: %s\n", col.Field, col.Type)
	}
	
	// 查看样例数据
	type MktLimit struct {
		Code      string
		TradeDate string
		HighLimit float64
		LowLimit  float64
	}
	var samples []map[string]interface{}
	db.Raw("SELECT * FROM mkt_limit WHERE trade_date = '2025-12-01' LIMIT 5").Scan(&samples)
	fmt.Println("\n样例数据 (2025-12-01):")
	for _, row := range samples {
		fmt.Printf("  %v\n", row)
	}
	
	var count int64
	db.Raw("SELECT COUNT(*) FROM mkt_limit WHERE trade_date = '2025-12-01'").Scan(&count)
	fmt.Printf("\n2025-12-01 共有 %d 条记录\n", count)
}
