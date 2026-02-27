#!/bin/bash

# 通联客户端诊断脚本

echo "========================================="
echo "通联客户端 (feeder_client) 诊断工具"
echo "========================================="
echo ""

echo "1. 检查进程是否运行"
echo "-------------------------------------------"
if ps aux | grep -v grep | grep -q feeder_client; then
    echo "✓ feeder_client 进程正在运行"
    ps aux | grep -v grep | grep feeder_client
else
    echo "✗ feeder_client 进程未运行"
fi
echo ""

echo "2. 检查端口监听"
echo "-------------------------------------------"
# 检查 9020 端口 (WebSocket)
if netstat -tlnp 2>/dev/null | grep -q ":9020 "; then
    echo "✓ 端口 9020 (WebSocket) 正在监听"
    netstat -tlnp 2>/dev/null | grep ":9020 "
else
    echo "✗ 端口 9020 (WebSocket) 未监听"
fi

# 检查 9012 端口 (TCP)
if netstat -tlnp 2>/dev/null | grep -q ":9012 "; then
    echo "✓ 端口 9012 (TCP) 正在监听"
    netstat -tlnp 2>/dev/null | grep ":9012 "
else
    echo "✗ 端口 9012 (TCP) 未监听"
fi

# 检查 9379 端口 (Redis)
if netstat -tlnp 2>/dev/null | grep -q ":9379 "; then
    echo "✓ 端口 9379 (Redis) 正在监听"
    netstat -tlnp 2>/dev/null | grep ":9379 "
else
    echo "✗ 端口 9379 (Redis) 未监听"
fi
echo ""

echo "3. 检查配置文件"
echo "-------------------------------------------"
if [ -f "feeder_client.cfg" ]; then
    echo "✓ 配置文件存在: feeder_client.cfg"

    # 检查 Token 是否已配置
    if grep -q '"Token".*:.*"[^"]*"' feeder_client.cfg 2>/dev/null; then
        TOKEN=$(grep '"Token"' feeder_client.cfg | head -1)
        if echo "$TOKEN" | grep -q '02553680449A4224858500BD8DD2CFE5\|YOUR_TOKEN'; then
            echo "⚠ 警告: Token 仍为默认值，需要修改为你的真实 Token"
        else
            echo "✓ Token 已配置"
        fi
    fi
else
    echo "✗ 配置文件不存在: feeder_client.cfg"
fi
echo ""

echo "4. 检查日志文件"
echo "-------------------------------------------"
if [ -f "feeder_client.log" ]; then
    echo "✓ 日志文件存在"
    echo ""
    echo "最后 20 行日志:"
    echo "-------------------------------------------"
    tail -20 feeder_client.log
else
    echo "✗ 日志文件不存在: feeder_client.log"
fi
echo ""

echo "5. 检查 autoconf.json (自动配置)"
echo "-------------------------------------------"
if [ -f "autoconf.json" ]; then
    echo "✓ autoconf.json 存在"
    echo "配置服务查询成功"
else
    echo "✗ autoconf.json 不存在"
    echo "可能原因: 配置服务查询失败或 Token 无效"
fi
echo ""

echo "6. 测试 WebSocket 连接"
echo "-------------------------------------------"
if command -v curl &> /dev/null; then
    echo "测试连接到 ws://localhost:9020"
    # 注意: curl 不直接支持 WebSocket，这里只是测试端口
    if timeout 3 bash -c "cat < /dev/null > /dev/tcp/localhost/9020" 2>/dev/null; then
        echo "✓ 端口 9020 可以连接"
    else
        echo "✗ 端口 9020 无法连接"
    fi
else
    echo "curl 未安装，跳过连接测试"
fi
echo ""

echo "========================================="
echo "诊断完成"
echo "========================================="
echo ""
echo "常见问题解决:"
echo ""
echo "问题1: 端口被占用"
echo "  解决: 检查是否有其他程序占用了 9020 端口"
echo "  命令: lsof -i:9020"
echo ""
echo "问题2: Token 无效"
echo "  解决: 编辑 feeder_client.cfg，修改 Token 为你的真实 Token"
echo "  命令: vi feeder_client.cfg"
echo ""
echo "问题3: 配置服务连接失败"
echo "  解决: 检查网络连接，确保可以访问 mdl01.datayes.com:19000"
echo "  命令: telnet mdl01.datayes.com 19000"
echo ""
echo "问题4: 权限不足"
echo "  解决: 确保可执行文件有执行权限"
echo "  命令: chmod +x feeder_client"
echo ""
