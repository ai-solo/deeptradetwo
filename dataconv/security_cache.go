package dataconv

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

// SecurityCache 证券代码缓存
// 从 md_security 表查询 SECURITY_ID，用于 Code 字符串到 Int32 的映射
type SecurityCache struct {
	mu       sync.RWMutex
	codeToID map[string]int32 // "600000.XSHG" -> SECURITY_ID
	db       *sql.DB
}

// NewSecurityCache 创建证券代码缓存
func NewSecurityCache(db *sql.DB) *SecurityCache {
	return &SecurityCache{
		codeToID: make(map[string]int32),
		db:       db,
	}
}

// GetID 获取 Code 对应的 SECURITY_ID
// code 格式: "600000.XSHG" 或 "000001.XSHE"
func (sc *SecurityCache) GetID(code string) (int32, error) {
	sc.mu.RLock()
	if id, ok := sc.codeToID[code]; ok {
		sc.mu.RUnlock()
		return id, nil
	}
	sc.mu.RUnlock()

	// 解析 code 格式
	tickerSymbol, exchangeCD, err := parseCode(code)
	if err != nil {
		return 0, fmt.Errorf("解析 code 失败: %w", err)
	}

	// 查询数据库
	var securityID int32
	query := "SELECT SECURITY_ID FROM md_security WHERE TICKER_SYMBOL = ? AND EXCHANGE_CD = ?"
	err = sc.db.QueryRow(query, tickerSymbol, exchangeCD).Scan(&securityID)
	if err != nil {
		return 0, fmt.Errorf("查询 SECURITY_ID 失败: %w", err)
	}

	// 缓存结果
	sc.mu.Lock()
	sc.codeToID[code] = securityID
	sc.mu.Unlock()

	return securityID, nil
}

// BatchLoad 批量加载 Code 到缓存
// 避免逐条查询数据库
func (sc *SecurityCache) BatchLoad(codes []string) error {
	if len(codes) == 0 {
		return nil
	}

	// 先检查缓存，只查询未缓存的
	sc.mu.RLock()
	needLoad := make([]string, 0, len(codes))
	for _, code := range codes {
		if _, ok := sc.codeToID[code]; !ok {
			needLoad = append(needLoad, code)
		}
	}
	sc.mu.RUnlock()

	if len(needLoad) == 0 {
		return nil
	}

	// 解析并去重
	parsed := make(map[string]struct{ ticker, exchange string })
	for _, code := range needLoad {
		ticker, exchange, err := parseCode(code)
		if err != nil {
			log.Printf("[警告] 跳过无效 code: %s, 错误: %v", code, err)
			continue
		}
		parsed[code] = struct{ ticker, exchange string }{ticker, exchange}
	}

	if len(parsed) == 0 {
		return nil
	}

	// 构建批量查询 SQL
	values := make([]interface{}, 0, len(parsed)*2)
	placeholders := make([]string, 0, len(parsed))
	for _, p := range parsed {
		values = append(values, p.ticker, p.exchange)
		placeholders = append(placeholders, "(?, ?)")
	}

	query := fmt.Sprintf(`
		SELECT TICKER_SYMBOL, EXCHANGE_CD, SECURITY_ID
		FROM md_security
		WHERE (TICKER_SYMBOL, EXCHANGE_CD) IN (%s)
	`, strings.Join(placeholders, ", "))

	rows, err := sc.db.Query(query, values...)
	if err != nil {
		return fmt.Errorf("批量查询失败: %w", err)
	}
	defer rows.Close()

	// 缓存结果
	sc.mu.Lock()
	defer sc.mu.Unlock()

	loadedCount := 0
	for rows.Next() {
		var ticker, exchange string
		var securityID int32
		if err := rows.Scan(&ticker, &exchange, &securityID); err != nil {
			log.Printf("[警告] 扫描行失败: %v", err)
			continue
		}
		code := fmt.Sprintf("%s.%s", ticker, exchange)
		sc.codeToID[code] = securityID
		loadedCount++
	}

	if loadedCount > 0 {
		log.Printf("[缓存] 批量加载 %d 个证券代码", loadedCount)
	}

	return rows.Err()
}

// GetCode 根据 SECURITY_ID 反向查询 Code
func (sc *SecurityCache) GetCode(id int32) (string, error) {
	var ticker, exchange string
	query := "SELECT TICKER_SYMBOL, EXCHANGE_CD FROM md_security WHERE SECURITY_ID = ?"
	err := sc.db.QueryRow(query, id).Scan(&ticker, &exchange)
	if err != nil {
		return "", fmt.Errorf("查询 Code 失败: %w", err)
	}
	return fmt.Sprintf("%s.%s", ticker, exchange), nil
}

// parseCode 解析 code 格式
// 输入: "600000.XSHG" -> 输出: "600000", "XSHG"
func parseCode(code string) (ticker string, exchange string, err error) {
	parts := strings.Split(code, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("无效的 code 格式: %s", code)
	}

	// 验证 ticker 是否为数字
	if _, err := strconv.Atoi(parts[0]); err != nil {
		return "", "", fmt.Errorf("ticker 必须是数字: %s", parts[0])
	}

	exchange = strings.ToUpper(parts[1])
	if exchange != "XSHG" && exchange != "XSHE" {
		return "", "", fmt.Errorf("无效的交易所代码: %s", exchange)
	}

	return parts[0], exchange, nil
}
