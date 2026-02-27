#!/bin/bash

# 通联数据采集系统部署脚本

set -e

echo "========================================="
echo "通联数据采集系统 - 部署脚本"
echo "========================================="

# 检查通联客户端是否运行
check_feeder_client() {
    echo "检查通联客户端 (feeder_client)..."

    if netstat -tlnp 2>/dev/null | grep -q ":9020 "; then
        echo "✓ 通联客户端正在运行 (端口 9020)"
        return 0
    else
        echo "✗ 通联客户端未运行！"
        echo "  请先启动通联客户端："
        echo "  cd /opt/feeder_client && ./feeder_client -d"
        return 1
    fi
}

# 检查 MySQL 连接
check_mysql() {
    echo "检查 MySQL 连接..."

    # 从配置文件读取 MySQL 配置
    MYSQL_HOST=$(grep -A 10 '\[storage.mysql\]' config.toml | grep 'host' | awk '{print $2}' | tr -d '"')
    MYSQL_PORT=$(grep -A 10 '\[storage.mysql\]' config.toml | grep 'port' | awk '{print $2}')
    MYSQL_USER=$(grep -A 10 '\[storage.mysql\]' config.toml | grep 'user' | awk '{print $2}' | tr -d '"')
    MYSQL_DB=$(grep -A 10 '\[storage.mysql\]' config.toml | grep 'database' | awk '{print $2}' | tr -d '"')

    if mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$(grep -A 10 '\[storage.mysql\]' config.toml | grep 'password' | awk '{print $2}' | tr -d '"')" -e "USE $MYSQL_DB;" 2>/dev/null; then
        echo "✓ MySQL 连接成功"
        return 0
    else
        echo "✗ MySQL 连接失败！"
        echo "  请检查配置文件中的 MySQL 配置"
        return 1
    fi
}

# 检查 Redis 连接
check_redis() {
    echo "检查 Redis 连接..."

    REDIS_HOST=$(grep -A 10 '\[storage.redis\]' config.toml | grep 'host' | awk '{print $2}' | tr -d '"')
    REDIS_PORT=$(grep -A 10 '\[storage.redis\]' config.toml | grep 'port' | awk '{print $2}')

    if redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping 2>/dev/null | grep -q "PONG"; then
        echo "✓ Redis 连接成功"
        return 0
    else
        echo "✗ Redis 连接失败！"
        echo "  请检查 Redis 服务是否运行"
        return 1
    fi
}

# 编译程序
build_application() {
    echo "编译通联数据采集程序..."

    if go build -o tonglian-ingestion cmd/tonglian-ingestion/main.go; then
        echo "✓ 编译成功"
        return 0
    else
        echo "✗ 编译失败！"
        return 1
    fi
}

# 创建 systemd 服务
create_systemd_service() {
    echo "创建 systemd 服务..."

    CURRENT_DIR=$(pwd)

    sudo tee /lib/systemd/system/tonglian-ingestion.service > /dev/null <<EOF
[Unit]
Description=TongLian Data Ingestion Service
After=network.target mysql.service redis.service

[Service]
Type=simple
User=root
WorkingDirectory=$CURRENT_DIR
ExecStart=$CURRENT_DIR/tonglian-ingestion
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    echo "✓ systemd 服务创建成功"
}

# 主函数
main() {
    echo ""
    echo "步骤 1/6: 检查通联客户端"
    check_feeder_client || exit 1

    echo ""
    echo "步骤 2/6: 检查 MySQL"
    check_mysql || exit 1

    echo ""
    echo "步骤 3/6: 检查 Redis"
    check_redis || exit 1

    echo ""
    echo "步骤 4/6: 编译程序"
    build_application || exit 1

    echo ""
    echo "步骤 5/6: 创建 systemd 服务"
    create_systemd_service

    echo ""
    echo "步骤 6/6: 启动服务"
    echo "是否启动通联数据采集服务？ [y/N]"
    read -r response

    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        sudo systemctl start tonglian-ingestion.service
        sleep 2

        if sudo systemctl is-active --quiet tonglian-ingestion.service; then
            echo "✓ 服务启动成功"
            echo ""
            echo "查看服务状态:"
            echo "  sudo systemctl status tonglian-ingestion.service"
            echo ""
            echo "查看日志:"
            echo "  sudo journalctl -u tonglian-ingestion.service -f"
            echo ""
            echo "停止服务:"
            echo "  sudo systemctl stop tonglian-ingestion.service"
        else
            echo "✗ 服务启动失败！"
            echo "查看错误日志:"
            echo "  sudo journalctl -u tonglian-ingestion.service -n 50"
            exit 1
        fi
    else
        echo "跳过启动服务"
        echo ""
        echo "手动启动命令:"
        echo "  sudo systemctl start tonglian-ingestion.service"
        echo ""
        echo "或直接运行:"
        echo "  ./tonglian-ingestion"
    fi

    echo ""
    echo "========================================="
    echo "部署完成！"
    echo "========================================="
}

# 运行主函数
cd "$(dirname "$0")/.."
main "$@"
