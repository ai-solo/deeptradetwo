package dataconv

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"deeptrade/storage"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	parquetlib "github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

// DailyBasicConfig daily basic data 生成配置
type DailyBasicConfig struct {
	TradingDay    time.Time
	OutputDir     string
	OSSConfig     *OSSConfig // nil 表示不上传
	ForceRegen    bool       // 是否强制重新生成（删除 OSS 中已存在的文件）
}

// GenerateDailyBasicData 为指定交易日生成一份 Parquet 文件并（可选）上传 OSS。
//
// 生成的文件：
//
//	{YYYYMMDD}_daily_basic_data.parquet
//	    —— mkt_equd JOIN mkt_equd_adj_af LEFT JOIN dy1d_exposure_sw21 LEFT JOIN mkt_idxd_csi
//	    —— 每行对应一只股票当日的行情、市值、因子暴露及市场指数数据
//
// 返回 1（成功）或 0（失败）。
func GenerateDailyBasicData(cfg DailyBasicConfig) (int, error) {
	db, err := storage.GetMySQLClient()
	if err != nil {
		return 0, fmt.Errorf("MySQL连接失败: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return 0, fmt.Errorf("获取sql.DB失败: %w", err)
	}

	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return 0, fmt.Errorf("创建输出目录失败: %w", err)
	}

	var uploader *OSSUploader
	if cfg.OSSConfig != nil {
		uploader, err = NewOSSUploader(*cfg.OSSConfig)
		if err != nil {
			log.Printf("[daily_basic] OSS初始化失败，跳过上传: %v", err)
		}
	}

	dateStr    := cfg.TradingDay.Format("20060102")
	dateFilter := cfg.TradingDay.Format("2006-01-02")
	filename   := dateStr + "_daily_basic_data.parquet"
	outputPath := filepath.Join(cfg.OutputDir, filename)

	// 强制重新生成：先删除 OSS 中已存在的文件
	if cfg.ForceRegen && uploader != nil {
		ossKey := uploader.BuildFilePath(cfg.TradingDay, filename)
		if uploader.ObjectExists(ossKey) {
			log.Printf("[daily_basic] ForceRegen=true，删除 OSS 中的已存在文件: %s", ossKey)
			if err := uploader.DeleteFile(ossKey); err != nil {
				log.Printf("[daily_basic] 删除 OSS 文件失败: %v", err)
			}
		}
	}

	// 构建合并查询：equity + exposure + mkt_idx 全部 JOIN 进同一张表
	query, err := buildDailyBasicSQL(sqlDB, dateFilter)
	if err != nil {
		log.Printf("[daily_basic] 构建SQL失败: %v，降级为仅 equity 数据", err)
		query = buildEquitySQL(dateFilter)
	}

	n, err := writeQueryToParquet(sqlDB, query, outputPath)
	if err != nil {
		return 0, fmt.Errorf("写入 %s 失败: %w", filename, err)
	}
	log.Printf("[daily_basic] %s 写入成功: %d 行", filename, n)

	if uploader != nil {
		ossKey := uploader.BuildFilePath(cfg.TradingDay, filename)
		if err := uploader.UploadLocalFile(outputPath, ossKey); err != nil {
			log.Printf("[daily_basic] OSS上传失败: %v", err)
		}
	}

	return 1, nil
}

// buildEquitySQL 构建股票行情+市值查询
func buildEquitySQL(dateFilter string) string {
	return fmt.Sprintf(`
SELECT
    t1.TRADE_DATE        AS TS,
    t1.TICKER_SYMBOL     AS ID_QI,
    t1.SECURITY_ID       AS SECURITY_ID,
    t2.OPEN_PRICE_2      AS open,
    t2.HIGHEST_PRICE     AS high,
    t2.LOWEST_PRICE      AS low,
    t2.CLOSE_PRICE       AS close,
    t2.OPEN_PRICE_2      AS adj_open,
    t2.CLOSE_PRICE_2     AS adj_close,
    t2.HIGHEST_PRICE_2   AS adj_high,
    t2.LOWEST_PRICE_2    AS adj_low,
    t2.PRE_CLOSE_PRICE_2 AS adj_pre_close,
    t1.DEAL_AMOUNT       AS deal_amount,
    t1.TURNOVER_VOL      AS volume,
    t1.TURNOVER_VALUE    AS amount,
    t1.MARKET_VALUE      AS mkt_cap,
    t1.NEG_MARKET_VALUE  AS float_mkt_cap,
    t1.TURNOVER_RATE     AS turnover_rate,
    t1.PE                AS pe_ttm,
    t1.PB                AS pb
FROM mkt_equd t1
JOIN mkt_equd_adj_af t2
    ON  t1.SECURITY_ID = t2.SECURITY_ID
    AND t1.TRADE_DATE  = t2.TRADE_DATE
WHERE t1.TRADE_DATE   = '%s'
  AND t1.EXCHANGE_CD IN ('XSHG', 'XSHE')
ORDER BY t1.TICKER_SYMBOL`, dateFilter)
}

// buildDailyBasicSQL 构建一条合并 SQL：
//   mkt_equd (行情) JOIN mkt_equd_adj_af (复权)
//   LEFT JOIN dy1d_exposure_sw21 (SW21因子暴露)
//   LEFT JOIN mkt_idxd_csi (市场指数，取 000300/000905 等主要指数的当日涨跌)
//
// 动态读取 exposure 表列名，排除 key 列，避免 SELECT * 产生列名冲突。
func buildDailyBasicSQL(db *sql.DB, dateFilter string) (string, error) {
	// --- 1. 动态获取 exposure 表的因子列 ---
	rows, err := db.Query("SHOW COLUMNS FROM dy1d_exposure_sw21")
	if err != nil {
		return "", fmt.Errorf("SHOW COLUMNS dy1d_exposure_sw21 失败: %w", err)
	}
	defer rows.Close()

	skipCols := map[string]bool{
		"TRADE_DATE": true, "TICKER_SYMBOL": true, "SECURITY_ID": true,
	}
	var expCols []string
	for rows.Next() {
		var field, colType, null, key, defVal, extra sql.NullString
		if err := rows.Scan(&field, &colType, &null, &key, &defVal, &extra); err != nil {
			continue
		}
		if !skipCols[strings.ToUpper(field.String)] {
			expCols = append(expCols, fmt.Sprintf("    e.%s", field.String))
		}
	}

	expSelect := ""
	if len(expCols) > 0 {
		expSelect = ",\n" + strings.Join(expCols, ",\n")
	}

	// --- 2. 动态获取 mkt_idxd_csi 表的指数列（排除 key 列）---
	idxRows, err := db.Query("SHOW COLUMNS FROM mkt_idxd_csi")
	if err != nil {
		// 表不存在时降级，不包含指数列
		idxRows = nil
	}

	skipIdxCols := map[string]bool{
		"TRADE_DATE": true, "TICKER_SYMBOL": true, "SECURITY_ID": true,
	}
	var idxCols []string
	if idxRows != nil {
		defer idxRows.Close()
		for idxRows.Next() {
			var field, colType, null, key, defVal, extra sql.NullString
			if err := idxRows.Scan(&field, &colType, &null, &key, &defVal, &extra); err != nil {
				continue
			}
			if !skipIdxCols[strings.ToUpper(field.String)] {
				idxCols = append(idxCols, fmt.Sprintf("    idx.%s AS idx_%s", field.String, field.String))
			}
		}
	}

	idxSelect := ""
	if len(idxCols) > 0 {
		idxSelect = ",\n" + strings.Join(idxCols, ",\n")
	}

	// mkt_idxd_csi 里沪深市场综合指数的代码（如 000300 = 沪深300）
	// 用一个子查询聚合成一行，拼到每只股票上
	idxJoin := ""
	if idxSelect != "" {
		idxJoin = fmt.Sprintf(`
LEFT JOIN (
    SELECT *
    FROM mkt_idxd_csi
    WHERE TRADE_DATE = '%s'
      AND TICKER_SYMBOL = '000300'   -- 沪深300，可按需调整
) idx ON 1=1`, dateFilter)
	}

	return fmt.Sprintf(`
SELECT
    t1.TRADE_DATE        AS TS,
    t1.TICKER_SYMBOL     AS ID_QI,
    t1.SECURITY_ID       AS SECURITY_ID,
    t2.OPEN_PRICE_2      AS open,
    t2.HIGHEST_PRICE     AS high,
    t2.LOWEST_PRICE      AS low,
    t2.CLOSE_PRICE       AS close,
    t2.OPEN_PRICE_2      AS adj_open,
    t2.CLOSE_PRICE_2     AS adj_close,
    t2.HIGHEST_PRICE_2   AS adj_high,
    t2.LOWEST_PRICE_2    AS adj_low,
    t2.PRE_CLOSE_PRICE_2 AS adj_pre_close,
    t1.DEAL_AMOUNT       AS deal_amount,
    t1.TURNOVER_VOL      AS volume,
    t1.TURNOVER_VALUE    AS amount,
    t1.MARKET_VALUE      AS mkt_cap,
    t1.NEG_MARKET_VALUE  AS float_mkt_cap,
    t1.TURNOVER_RATE     AS turnover_rate,
    t1.PE                AS pe_ttm,
    t1.PB                AS pb%s%s
FROM mkt_equd t1
JOIN mkt_equd_adj_af t2
    ON  t1.SECURITY_ID = t2.SECURITY_ID
    AND t1.TRADE_DATE  = t2.TRADE_DATE
LEFT JOIN dy1d_exposure_sw21 e
    ON  t1.TICKER_SYMBOL = e.TICKER_SYMBOL
    AND t1.TRADE_DATE    = e.TRADE_DATE%s
WHERE t1.TRADE_DATE   = '%s'
  AND t1.EXCHANGE_CD IN ('XSHG', 'XSHE')
ORDER BY t1.TICKER_SYMBOL`, expSelect, idxSelect, idxJoin, dateFilter), nil
}

// writeQueryToParquet 执行 SQL 查询并将结果写入 Parquet 文件。
// 列类型由 MySQL 返回的元数据动态推断：
//   - 字符/日期类型 → String
//   - 整型 → Int64
//   - 其他数值 → Float64
// 重复列名会自动追加 _2 / _3 … 后缀。
// 返回写入的行数。
func writeQueryToParquet(db *sql.DB, query, outputPath string) (int, error) {
	rows, err := db.Query(query)
	if err != nil {
		return 0, fmt.Errorf("执行SQL失败: %w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return 0, fmt.Errorf("获取列类型失败: %w", err)
	}

	// 列类型枚举
	const (
		colTypeString = iota
		colTypeInt64
		colTypeFloat64
	)

	// 推断 Arrow 列类型，并处理重复列名
	fields    := make([]arrow.Field, len(colTypes))
	colTypes_ := make([]int, len(colTypes))
	seenNames := make(map[string]int, len(colTypes))

	for i, ct := range colTypes {
		name := ct.Name()
		if count, ok := seenNames[name]; ok {
			seenNames[name]++
			name = fmt.Sprintf("%s_%d", name, count+1)
		} else {
			seenNames[name] = 1
		}

		switch ct.DatabaseTypeName() {
		case "VARCHAR", "CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT",
			"DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
			fields[i]    = arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true}
			colTypes_[i] = colTypeString
		case "BIGINT", "INT", "INTEGER", "SMALLINT", "TINYINT", "MEDIUMINT":
			fields[i]    = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
			colTypes_[i] = colTypeInt64
		default:
			fields[i]    = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: true}
			colTypes_[i] = colTypeFloat64
		}
	}

	schema  := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	vals := make([]interface{}, len(colTypes))
	ptrs := make([]interface{}, len(colTypes))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return rowCount, fmt.Errorf("扫描行失败 (row %d): %w", rowCount, err)
		}
		for i, v := range vals {
			switch colTypes_[i] {
			case colTypeString:
				b := builder.Field(i).(*array.StringBuilder)
				if v == nil {
					b.AppendNull()
				} else {
					// MySQL 驱动返回 VARCHAR 时可能是 []byte，需要正确转换
					b.Append(stringVal(v))
				}
			case colTypeInt64:
				b := builder.Field(i).(*array.Int64Builder)
				if v == nil {
					b.AppendNull()
				} else {
					appendInt64(b, v)
				}
			default: // colTypeFloat64
				b := builder.Field(i).(*array.Float64Builder)
				if v == nil {
					b.AppendNull()
				} else {
					appendFloat64(b, v)
				}
			}
		}
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return rowCount, err
	}
	if rowCount == 0 {
		return 0, fmt.Errorf("查询结果为空")
	}

	rec := builder.NewRecord()
	defer rec.Release()

	f, err := os.Create(outputPath)
	if err != nil {
		return rowCount, fmt.Errorf("创建文件失败: %w", err)
	}
	defer f.Close()

	props  := parquetlib.NewWriterProperties(parquetlib.WithCompression(compress.Codecs.Snappy))
	writer, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return rowCount, fmt.Errorf("创建Parquet写入器失败: %w", err)
	}
	if err := writer.Write(rec); err != nil {
		writer.Close()
		return rowCount, fmt.Errorf("写入Parquet失败: %w", err)
	}
	writer.Close()

	return rowCount, nil
}

// stringVal 将 SQL 扫描到的值转换为字符串，正确处理 []byte 类型
func stringVal(v interface{}) string {
	switch tv := v.(type) {
	case string:
		return tv
	case []byte:
		return string(tv)
	default:
		return fmt.Sprintf("%v", tv)
	}
}

// appendInt64 将 SQL 扫描到的任意整型追加到 Int64Builder
func appendInt64(b *array.Int64Builder, v interface{}) {
	switch tv := v.(type) {
	case int64:
		b.Append(tv)
	case int32:
		b.Append(int64(tv))
	case int:
		b.Append(int64(tv))
	case uint64:
		b.Append(int64(tv))
	case uint32:
		b.Append(int64(tv))
	case []byte:
		if i, err := strconv.ParseInt(string(tv), 10, 64); err == nil {
			b.Append(i)
		} else {
			b.AppendNull()
		}
	case string:
		if i, err := strconv.ParseInt(tv, 10, 64); err == nil {
			b.Append(i)
		} else {
			b.AppendNull()
		}
	default:
		b.AppendNull()
	}
}

// appendFloat64 将 SQL 扫描到的任意数值类型追加到 Float64Builder
func appendFloat64(b *array.Float64Builder, v interface{}) {
	switch tv := v.(type) {
	case float64:
		b.Append(tv)
	case float32:
		b.Append(float64(tv))
	case int64:
		b.Append(float64(tv))
	case int32:
		b.Append(float64(tv))
	case int:
		b.Append(float64(tv))
	case []byte:
		if f, err := strconv.ParseFloat(string(tv), 64); err == nil {
			b.Append(f)
		} else {
			b.AppendNull()
		}
	case string:
		if f, err := strconv.ParseFloat(tv, 64); err == nil {
			b.Append(f)
		} else {
			b.AppendNull()
		}
	default:
		b.AppendNull()
	}
}
