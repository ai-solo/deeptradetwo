#!/bin/bash

# 测试OSS上传功能脚本
# 处理1万条数据并上传到OSS

set -e

echo "=========================================="
echo "测试OSS上传功能"
echo "=========================================="

# 确保程序已编译
if [ ! -f "./data-converter" ]; then
    echo "[编译] 正在编译程序..."
    cd cmd/data-converter && go build -o ../../data-converter . && cd ../..
fi

# 设置OSS环境变量（请根据实际情况修改）
export OSS_ACCESS_KEY_ID="${OSS_ACCESS_KEY_ID:-your_access_key_id}"
export OSS_ACCESS_KEY_SECRET="${OSS_ACCESS_KEY_SECRET:-your_access_key_secret}"
export OSS_ENDPOINT="${OSS_ENDPOINT:-oss-cn-shanghai.aliyuncs.com}"
export OSS_BUCKET_NAME="${OSS_BUCKET_NAME:-stock-data}"

# 清理旧的输出文件（可选）
# rm -f output/20251201_*.parquet

# 运行数据处理程序
# 参数说明：
# -date: 交易日期
# -dir: 数据目录
# -output: 输出目录
# -optimize: 启用优化模式（Int32+Zstd压缩）
# -force-int32: 强制Int32模式
# -limit: 限制处理行数（测试用）
# -oss: 启用OSS上传
# -workers: 并发数

./data-converter \
  -date=20251201 \
  -dir=. \
  -output=./output \
  -optimize \
  -force-int32 \
  -limit=10000 \
  -oss \
  -workers=2 \
  -type=tick

echo ""
echo "=========================================="
echo "测试完成"
echo "=========================================="
