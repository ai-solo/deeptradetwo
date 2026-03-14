package dataconv

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// codeChangeEntry 股票代码变更条目（新代码 -> 旧代码 + 变更日期）
type codeChangeEntry struct {
	OldCode    string    // 变更前的旧代码（历史行情数据中出现的代码）
	ChangeDate time.Time // 变更日期（含当天）
}

// codeChangeNewMap 新代码 -> (旧代码, 变更日期)，与 Python code_change_map 完全对应
var codeChangeNewMap = map[string]codeChangeEntry{
	"001914.XSHE": {OldCode: "000043.XSHE", ChangeDate: time.Date(2019, 12, 13, 0, 0, 0, 0, time.UTC)},
	"601607.XSHG": {OldCode: "600849.XSHG", ChangeDate: time.Date(2010, 3, 8, 0, 0, 0, 0, time.UTC)},
	"601360.XSHG": {OldCode: "601313.XSHG", ChangeDate: time.Date(2018, 2, 27, 0, 0, 0, 0, time.UTC)},
	"001872.XSHE": {OldCode: "000022.XSHE", ChangeDate: time.Date(2018, 12, 25, 0, 0, 0, 0, time.UTC)},
	"302132.XSHE": {OldCode: "300114.XSHE", ChangeDate: time.Date(2025, 2, 14, 0, 0, 0, 0, time.UTC)},
}

// oldCodeToNewCode 旧代码 -> 新代码的反向索引，在初始化时由 codeChangeNewMap 生成
// 用于：原始数据中出现旧代码 -> 数据库中只有新代码 -> 需要用新代码的 SECURITY_ID
var oldCodeToNewCode = func() map[string]string {
	m := make(map[string]string, len(codeChangeNewMap))
	for newCode, entry := range codeChangeNewMap {
		m[entry.OldCode] = newCode
	}
	return m
}()

// SecurityCache 证券代码缓存
// 从 md_security 表查询 SECURITY_ID，用于 Code 字符串到 Int32 的映射
type SecurityCache struct {
	mu       sync.RWMutex
	codeToID map[string]int32 // "600000.XSHG" -> SECURITY_ID, -1表示不存在
	db       *sql.DB
}

const (
	// SecurityNotFound 表示证券代码在数据库中不存在（负缓存）
	SecurityNotFound = -1
)

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
	return sc.GetIDWithDate(code, time.Time{})
}

// GetIDWithDate 获取 Code 对应的 SECURITY_ID，支持历史代码变更回退。
//
// 原始市场数据中的代码与数据库可能不一致的场景：
//   - 某只股票历史上更换过代码，原始数据用的是旧代码，但 md_security 只保留新代码。
//   - 例如：300114.XSHE 在 2025-02-14 更名为 302132.XSHE，2025-02-14 及之前的行情
//     文件中代码为 300114.XSHE，但数据库里只有 302132.XSHE 的 SECURITY_ID。
//
// 处理流程：
//  1. 直接用原始代码查询（缓存 -> 数据库）。
//  2. 若未找到，检查旧代码反向索引，若存在对应新代码，改用新代码查询。
//  3. 最终仍未找到时，写入负缓存并返回错误。
func (sc *SecurityCache) GetIDWithDate(code string, tradingDay time.Time) (int32, error) {
	// 第一步：直接查缓存
	sc.mu.RLock()
	if id, ok := sc.codeToID[code]; ok {
		sc.mu.RUnlock()
		if id == SecurityNotFound {
			return 0, fmt.Errorf("证券代码不存在（已缓存）")
		}
		return id, nil
	}
	sc.mu.RUnlock()

	// 第二步：直接查数据库
	if id, err := sc.queryDB(code); err == nil {
		sc.mu.Lock()
		sc.codeToID[code] = id
		sc.mu.Unlock()
		return id, nil
	}

	// 第三步：旧代码 -> 新代码回退
	// 场景：原始数据里出现旧代码，但 md_security 只有新代码
	if newCode, ok := oldCodeToNewCode[code]; ok {
		// 只在变更日期当天及之前的数据中回退（零值表示无限制，始终回退）
		entry := codeChangeNewMap[newCode]
		if tradingDay.IsZero() || !tradingDay.After(entry.ChangeDate) {
			sc.mu.RLock()
			if id, ok := sc.codeToID[newCode]; ok {
				sc.mu.RUnlock()
				if id != SecurityNotFound {
					// 同时以旧代码为 key 缓存，加速后续查找
					sc.mu.Lock()
					sc.codeToID[code] = id
					sc.mu.Unlock()
					log.Printf("[代码映射] %s -> %s (交易日 %s <= 变更日 %s), SECURITY_ID=%d",
						code, newCode, tradingDay.Format("2006-01-02"), entry.ChangeDate.Format("2006-01-02"), id)
					return id, nil
				}
			}
			sc.mu.RUnlock()

			if id, err := sc.queryDB(newCode); err == nil {
				sc.mu.Lock()
				sc.codeToID[newCode] = id
				sc.codeToID[code] = id // 旧代码也缓存同一 ID
				sc.mu.Unlock()
				log.Printf("[代码映射] %s -> %s (交易日 %s <= 变更日 %s), SECURITY_ID=%d",
					code, newCode, tradingDay.Format("2006-01-02"), entry.ChangeDate.Format("2006-01-02"), id)
				return id, nil
			}
		}
	}

	// 所有路径均失败，写负缓存
	sc.mu.Lock()
	sc.codeToID[code] = SecurityNotFound
	sc.mu.Unlock()
	return 0, fmt.Errorf("证券代码不存在: %s", code)
}

// queryDB 向数据库查询单个 code 的 SECURITY_ID，不操作缓存
// 当 XSHG/XSHE 查不到时，回退到 OFCN（场外基金在行情数据中用交易所代码，但数据库登记为OFCN）
func (sc *SecurityCache) queryDB(code string) (int32, error) {
	tickerSymbol, exchangeCD, err := parseCode(code)
	if err != nil {
		return 0, fmt.Errorf("解析 code 失败: %w", err)
	}
	var id int32
	query := "SELECT SECURITY_ID FROM md_security WHERE TICKER_SYMBOL = ? AND EXCHANGE_CD = ?"
	if err := sc.db.QueryRow(query, tickerSymbol, exchangeCD).Scan(&id); err == nil {
		return id, nil
	}
	if exchangeCD == "XSHG" || exchangeCD == "XSHE" {
		if err2 := sc.db.QueryRow(query, tickerSymbol, "OFCN").Scan(&id); err2 == nil {
			return id, nil
		}
	}
	return 0, fmt.Errorf("证券代码不存在: %s", code)
}

// BatchLoad 批量加载 Code 到缓存，tradingDay 用于旧代码回退判断（零值表示不限制）
func (sc *SecurityCache) BatchLoad(codes []string, tradingDay time.Time) error {
	if len(codes) == 0 {
		return nil
	}

	// 只加载尚未缓存的代码
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

	// 解析有效代码；遇到旧代码时，同时把对应的新代码加入查询集合
	// queryCodeToOriginals: 实际查询的 code -> 原始 code 列表（一个新代码可能对应多个旧代码）
	type parsedEntry struct{ ticker, exchange string }
	queryParsed := make(map[string]parsedEntry)          // 实际要查询的 code -> 解析结果
	queryCodeToOriginals := make(map[string][]string)    // 实际查询 code -> 原始 code 列表

	for _, code := range needLoad {
		actualCode := code

		// 检查是否为已知旧代码 -> 替换为新代码查询
		if newCode, ok := oldCodeToNewCode[code]; ok {
			entry := codeChangeNewMap[newCode]
			if tradingDay.IsZero() || !tradingDay.After(entry.ChangeDate) {
				actualCode = newCode
			}
		}

		ticker, exchange, err := parseCode(actualCode)
		if err != nil {
			log.Printf("[警告] 跳过无效 code: %s, 错误: %v", code, err)
			continue
		}
		queryParsed[actualCode] = parsedEntry{ticker, exchange}
		queryCodeToOriginals[actualCode] = append(queryCodeToOriginals[actualCode], code)
	}

	if len(queryParsed) == 0 {
		return nil
	}

	// 构建批量查询 SQL
	values := make([]interface{}, 0, len(queryParsed)*2)
	placeholders := make([]string, 0, len(queryParsed))
	for _, p := range queryParsed {
		values = append(values, p.ticker, p.exchange)
		placeholders = append(placeholders, "(?, ?)")
	}

	query := fmt.Sprintf(
		"SELECT TICKER_SYMBOL, EXCHANGE_CD, SECURITY_ID FROM md_security WHERE (TICKER_SYMBOL, EXCHANGE_CD) IN (%s)",
		strings.Join(placeholders, ", "),
	)

	rows, err := sc.db.Query(query, values...)
	if err != nil {
		return fmt.Errorf("批量查询失败: %w", err)
	}
	defer rows.Close()

	sc.mu.Lock()
	defer sc.mu.Unlock()

	found := make(map[string]bool)
	loadedCount := 0
	for rows.Next() {
		var ticker, exchange string
		var securityID int32
		if err := rows.Scan(&ticker, &exchange, &securityID); err != nil {
			log.Printf("[警告] 扫描行失败: %v", err)
			continue
		}
		qCode := fmt.Sprintf("%s.%s", ticker, exchange)
		sc.codeToID[qCode] = securityID
		found[qCode] = true
		loadedCount++

		// 同时把旧代码（原始请求代码）指向相同的 SECURITY_ID
		for _, origCode := range queryCodeToOriginals[qCode] {
			if origCode != qCode {
				sc.codeToID[origCode] = securityID
				found[origCode] = true
			}
		}
	}

	// 未找到的 XSHG/XSHE 代码，尝试 OFCN 回退（场外基金在行情数据中用交易所代码，但数据库登记为OFCN）
	ofcnValues := make([]interface{}, 0)
	ofcnPlaceholders := make([]string, 0)
	ofcnQCodeToOriginals := make(map[string][]string)
	for qCode, originals := range queryCodeToOriginals {
		if !found[qCode] {
			p := queryParsed[qCode]
			if p.exchange == "XSHG" || p.exchange == "XSHE" {
				ofcnCode := fmt.Sprintf("%s.OFCN", p.ticker)
				ofcnValues = append(ofcnValues, p.ticker, "OFCN")
				ofcnPlaceholders = append(ofcnPlaceholders, "(?, ?)")
				ofcnQCodeToOriginals[ofcnCode] = append(ofcnQCodeToOriginals[ofcnCode], originals...)
				ofcnQCodeToOriginals[ofcnCode] = append(ofcnQCodeToOriginals[ofcnCode], qCode)
				found[qCode] = true // 先标记，避免下面写负缓存
			}
		}
	}
	if len(ofcnPlaceholders) > 0 {
		ofcnQuery := fmt.Sprintf(
			"SELECT TICKER_SYMBOL, EXCHANGE_CD, SECURITY_ID FROM md_security WHERE (TICKER_SYMBOL, EXCHANGE_CD) IN (%s)",
			strings.Join(ofcnPlaceholders, ", "),
		)
		ofcnRows, err2 := sc.db.Query(ofcnQuery, ofcnValues...)
		if err2 == nil {
			for ofcnRows.Next() {
				var ticker, exchange string
				var securityID int32
				if err3 := ofcnRows.Scan(&ticker, &exchange, &securityID); err3 != nil {
					continue
				}
				ofcnCode := fmt.Sprintf("%s.%s", ticker, exchange)
				sc.codeToID[ofcnCode] = securityID
				for _, origCode := range ofcnQCodeToOriginals[ofcnCode] {
					sc.codeToID[origCode] = securityID
				}
				delete(ofcnQCodeToOriginals, ofcnCode)
			}
			ofcnRows.Close()
		}
		// 仍未找到的写负缓存
		for _, originals := range ofcnQCodeToOriginals {
			for _, origCode := range originals {
				sc.codeToID[origCode] = SecurityNotFound
			}
		}
	}

	// 未找到的代码写负缓存
	notFoundCount := 0
	for qCode, originals := range queryCodeToOriginals {
		if !found[qCode] {
			sc.codeToID[qCode] = SecurityNotFound
			notFoundCount++
			for _, origCode := range originals {
				if origCode != qCode {
					sc.codeToID[origCode] = SecurityNotFound
				}
			}
		}
	}

	if loadedCount > 0 {
		log.Printf("[缓存] 批量加载 %d 个证券代码", loadedCount)
	}
	if notFoundCount > 0 {
		log.Printf("[缓存] %d 个证券代码不存在（已记录）", notFoundCount)
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
