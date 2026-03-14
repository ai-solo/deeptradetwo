#!/bin/bash
set -e

echo "========================================"
echo "[启动] data-converter 容器"
echo "[配置] 年份: ${YEAR}, 月份: ${MONTH}"
echo "========================================"

# 必要参数检查
if [ -z "${YEAR}" ] || [ -z "${MONTH}" ]; then
    echo "[错误] 必须设置 YEAR 和 MONTH 环境变量"
    exit 1
fi

# 等待 aria2 就绪（最多60秒）
ARIA2_URL="${ARIA2_URL:-}"
if [ -z "${ARIA2_URL}" ]; then
    echo "[错误] 必须设置 ARIA2_URL 环境变量 (例如: http://192.168.1.100:6800/jsonrpc)"
    exit 1
fi
ARIA2_TOKEN="${ARIA2_TOKEN:-}"
echo "[等待] aria2 就绪: ${ARIA2_URL}"
for i in $(seq 1 30); do
    if curl -sf -X POST "${ARIA2_URL}" \
        -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":\"ping\",\"method\":\"aria2.getVersion\",\"params\":[\"token:${ARIA2_TOKEN}\"]}" \
        > /dev/null 2>&1; then
        echo "[就绪] aria2 已启动"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "[错误] aria2 启动超时"
        exit 1
    fi
    echo "[等待] aria2 未就绪，${i}/30..."
    sleep 2
done

# 构建 data-converter 参数
ARGS=(
    -auto-download
    -year "${YEAR}"
    -month "${MONTH}"
    -optimize
    -output /data/parquet
    -download-dir /data/raw
    -aria2-url "${ARIA2_URL}"
    -workers "${WORKERS:-0}"
    -password "${ZIP_PASSWORD:-DataYes}"
)

# OSS 参数
if [ -n "${OSS_ACCESS_KEY}" ]; then
    ARGS+=(
        -oss
        -oss-access-key "${OSS_ACCESS_KEY}"
        -oss-secret-key "${OSS_SECRET_KEY}"
        -oss-clean-local
    )
    [ -n "${OSS_ENDPOINT}" ] && ARGS+=(-oss-endpoint "${OSS_ENDPOINT}")
    [ -n "${OSS_BUCKET}" ]   && ARGS+=(-oss-bucket "${OSS_BUCKET}")
    [ -n "${OSS_PATH}" ]     && ARGS+=(-oss-path "${OSS_PATH}")
fi

# 文件服务器参数
[ -n "${FILE_SERVER_URL}" ]       && ARGS+=(-fileserver-url "${FILE_SERVER_URL}")
[ -n "${FILE_SERVER_AUTH}" ]      && ARGS+=(-fileserver-auth "${FILE_SERVER_AUTH}")
[ -n "${FILE_SERVER_USER}" ]      && ARGS+=(-fileserver-username "${FILE_SERVER_USER}")
[ -n "${FILE_SERVER_PASS_HASH}" ] && ARGS+=(-fileserver-password "${FILE_SERVER_PASS_HASH}")

# aria2 token
[ -n "${ARIA2_TOKEN}" ] && ARGS+=(-aria2-token "${ARIA2_TOKEN}")

# daily basic
[ "${DAILY_BASIC:-true}" = "true" ] && ARGS+=(-daily-basic)

echo "[执行] data-converter ${ARGS[*]}"
exec /usr/local/bin/data-converter "${ARGS[@]}"
