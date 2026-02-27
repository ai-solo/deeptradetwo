#!/bin/bash

# 通联 WebSocket 测试脚本

echo "========================================="
echo "通联 WebSocket 连接测试"
echo "========================================="
echo ""

# 检查 wscat 是否安装
if ! command -v wscat &> /dev/null; then
    echo "wscat 未安装，正在安装..."
    yum install -y nodejs > /dev/null 2>&1
    npm install -g wscat > /dev/null 2>&1

    if ! command -v wscat &> /dev/null; then
        echo "✗ wscat 安装失败"
        echo "请手动安装: npm install -g wscat"
        exit 1
    fi
    echo "✓ wscat 安装成功"
fi

echo "正在连接到 ws://localhost:9020 ..."
echo ""
echo "连接成功后，会自动发送订阅请求"
echo "订阅类别: 4.23.* (上交所债券快照)"
echo "订阅类别: 6.51.* (深交所债券快照)"
echo "订阅类别: 6.54.* (深交所债券其他)"
echo ""
echo "Token 权限: 债券市场数据"
echo "上交所 ServiceID: 4 (债券), Messages: 23"
echo "深交所 ServiceID: 6 (债券), Messages: 51, 54"
echo ""
echo "注意: 此 Token 没有股票数据权限"
echo "如需股票数据，请联系通联升级 Token"
echo ""
echo "按 Ctrl+C 退出"
echo "========================================="
echo ""

# 使用 wscat 连接并发送订阅
(
  sleep 2
  echo '{"format":"csv","subscribe":["4.23.*","6.51.*","6.54.*"]}'
  sleep 30
  echo "测试完成，退出..."
) | wscat -c ws://localhost:9020

echo ""
echo "========================================="
echo "测试完成"
echo "========================================="
