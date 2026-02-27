package storage

import (
	"fmt"
	"log"
	"time"

	"deeptrade/conf"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var mysqlDB *gorm.DB

// GetMySQLClient returns the MySQL database instance
func GetMySQLClient() (*gorm.DB, error) {
	if mysqlDB != nil {
		return mysqlDB, nil
	}

	cfg := conf.Get().Storage.MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	// Configure connection pool
	maxOpenConns := cfg.MaxOpenConns
	if maxOpenConns == 0 {
		maxOpenConns = 100
	}

	maxIdleConns := cfg.MaxIdleConns
	if maxIdleConns == 0 {
		maxIdleConns = 10
	}

	connMaxLifeTime := cfg.ConnMaxLifeTime
	if connMaxLifeTime == 0 {
		connMaxLifeTime = 3600
	}

	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetConnMaxLifetime(time.Duration(connMaxLifeTime) * time.Second)

	// Test connection
	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}

	mysqlDB = db
	log.Printf("[存储] MySQL连接成功: %s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, cfg.Database)

	return mysqlDB, nil
}

// InitMySQL initializes MySQL database and runs migrations
func InitMySQL() error {
	db, err := GetMySQLClient()
	if err != nil {
		return err
	}

	// Run auto migration
	if err := AutoMigrate(db); err != nil {
		return fmt.Errorf("failed to run auto migration: %w", err)
	}

	log.Println("[存储] MySQL数据库迁移成功")
	return nil
}

// CloseMySQL closes the MySQL connection
func CloseMySQL() error {
	if mysqlDB == nil {
		return nil
	}

	sqlDB, err := mysqlDB.DB()
	if err != nil {
		return err
	}

	return sqlDB.Close()
}
