#!/bin/bash

# 本地测试通联数据推送

echo "========================================"
echo "通联数据本地测试"
echo "========================================"
echo ""

# 检查二进制文件
if [ ! -f "./tonglian-test" ]; then
    echo "未找到 tonglian-test，正在编译..."
    go build -o tonglian-test cmd/tonglian-test/main.go
    if [ $? -ne 0 ]; then
        echo "✗ 编译失败"
        exit 1
    fi
    echo "✓ 编译成功"
fi

echo "连接到: ws://47.101.149.89:9020"
echo "数据格式: JSON"
echo "订阅内容:"
echo "  - 4.23.* (上交所债券)"
echo "  - 6.51.* (深交所债券)"
echo "  - 6.54.* (深交所债券)"
echo ""
echo "----------------------------------------"
echo "按 Ctrl+C 退出"
echo "----------------------------------------"
echo ""

# 运行测试程序
./tonglian-test "ws://47.101.149.89:9020"
