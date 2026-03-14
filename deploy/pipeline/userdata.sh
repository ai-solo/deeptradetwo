#!/bin/bash
set -e

# ============================================================
# ECS cloud-init UserData
# 由 pipeline 控制程序注入，以下占位符会被替换：
#   {{YEAR}}  {{MONTH}}  {{GIT_REPO}}  {{GIT_BRANCH}}
#   {{ARIA2_TOKEN}}  {{FILE_SERVER_URL}}  {{FILE_SERVER_AUTH}}
#   {{FILE_SERVER_USER}}  {{FILE_SERVER_PASS_HASH}}
#   {{OSS_ACCESS_KEY}}  {{OSS_SECRET_KEY}}
#   {{OSS_ENDPOINT}}  {{OSS_BUCKET}}  {{OSS_PATH}}
#   {{MYSQL_HOST}}  {{MYSQL_PORT}}  {{MYSQL_USER}}  {{MYSQL_PASSWORD}}  {{MYSQL_DATABASE}}
#   {{WORKERS}}  {{ECS_REGION}}
#   {{ACCESS_KEY_ID}}  {{ACCESS_KEY_SECRET}}
# ============================================================

YEAR="{{YEAR}}"
MONTH="{{MONTH}}"
GIT_REPO="{{GIT_REPO}}"
GIT_BRANCH="{{GIT_BRANCH}}"
ARIA2_TOKEN="{{ARIA2_TOKEN}}"
FILE_SERVER_URL="{{FILE_SERVER_URL}}"
FILE_SERVER_AUTH="{{FILE_SERVER_AUTH}}"
FILE_SERVER_USER="{{FILE_SERVER_USER}}"
FILE_SERVER_PASS_HASH="{{FILE_SERVER_PASS_HASH}}"
OSS_ACCESS_KEY="{{OSS_ACCESS_KEY}}"
OSS_SECRET_KEY="{{OSS_SECRET_KEY}}"
OSS_ENDPOINT="{{OSS_ENDPOINT}}"
OSS_BUCKET="{{OSS_BUCKET}}"
OSS_PATH="{{OSS_PATH}}"
MYSQL_HOST="{{MYSQL_HOST}}"
MYSQL_PORT="{{MYSQL_PORT}}"
MYSQL_USER="{{MYSQL_USER}}"
MYSQL_PASSWORD="{{MYSQL_PASSWORD}}"
MYSQL_DATABASE="{{MYSQL_DATABASE}}"
WORKERS="{{WORKERS}}"
ECS_REGION="{{ECS_REGION}}"
# 从阿里云元数据服务获取实例 ID
ECS_INSTANCE_ID=$(curl -sf http://100.100.100.200/latest/meta-data/instance-id || echo "")

# cloud-init 环境下 HOME 可能未定义
export HOME=${HOME:-/root}
export GOCACHE=${GOCACHE:-/root/.cache/go-build}

LOG=/var/log/data-pipeline.log
exec > >(tee -a $LOG) 2>&1

# ---- 日志工具函数 ----
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO ] $*"
}
log_ok() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [OK   ] $*"
}
log_err() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2
}
log_step() {
    echo ""
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ======== $* ========"
}
log_sys() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SYS  ] CPU: $(nproc) 核 | 内存: $(free -h | awk '/^Mem/{print $2}') 总 / $(free -h | awk '/^Mem/{print $7}') 可用 | 磁盘: $(df -h / | awk 'NR==2{print $4}') 可用"
}

# 记录每步耗时
STEP_START=$(date +%s)
log_elapsed() {
    local now=$(date +%s)
    local elapsed=$((now - STEP_START))
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [TIME ] 耗时: ${elapsed}s"
    STEP_START=$now
}

# 未处理错误时打印上下文
trap 'log_err "脚本异常退出 (line $LINENO, exit $?)"; log_sys' ERR

echo ""
echo "################################################################"
log_info "data-pipeline 启动"
log_info "实例 ID : ${ECS_INSTANCE_ID}"
log_info "Region  : ${ECS_REGION}"
log_info "任务    : ${YEAR}-$(printf '%02d' ${MONTH})"
log_info "Git     : ${GIT_REPO} @ ${GIT_BRANCH}"
log_info "文件服务: ${FILE_SERVER_URL}"
log_info "OSS     : ${OSS_BUCKET}/${OSS_PATH} (${OSS_ENDPOINT})"
log_sys
echo "################################################################"
echo ""

# ================================================================
log_step "1/7 安装系统依赖"
# ================================================================
log_info "apt 安装基础依赖"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git curl wget unzip ca-certificates sysstat aria2
timedatectl set-timezone Asia/Shanghai 2>/dev/null || ln -snf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
log_ok "依赖安装完成"
log_sys
log_elapsed

# ================================================================
log_step "2/7 安装 Go"
# ================================================================
if command -v go &>/dev/null; then
    log_info "Go 已存在: $(go version)"
else
    log_info "下载并安装 Go"
    GO_VER="1.23.5"
    GOARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
    GO_TAR="go${GO_VER}.linux-${GOARCH}.tar.gz"
    log_info "下载 ${GO_TAR}"
    wget -nv --timeout=60 -O /tmp/${GO_TAR} \
        "https://mirrors.aliyun.com/golang/${GO_TAR}" 2>&1
    log_info "解压 Go"
    tar -C /usr/local -xzf /tmp/${GO_TAR}
    rm -f /tmp/${GO_TAR}
    log_ok "Go 解压完成"
fi
export GOPATH=/root/go
export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin
export GOPROXY=https://mirrors.aliyun.com/goproxy/,direct
log_ok "Go 版本: $(go version)"
log_elapsed

# ================================================================
log_step "3/7 启动 aria2"
# ================================================================
mkdir -p /data/raw /data/parquet
log_info "启动 aria2c (RPC 端口 6800)"
ARIA2_ARGS=(
    --enable-rpc
    --rpc-allow-origin-all
    --rpc-listen-all
    --rpc-listen-port=6800
    --user-agent="pan.baidu.com"
    --max-concurrent-downloads=10
    --split=32
    --min-split-size=10M
    --max-connection-per-server=4
    --continue=true
    --dir=/data/raw
    --log=/var/log/aria2.log
    --log-level=notice
    -D
)
[ -n "${ARIA2_TOKEN}" ] && ARIA2_ARGS+=(--rpc-secret="${ARIA2_TOKEN}")
aria2c "${ARIA2_ARGS[@]}"
sleep 2
# 验证 aria2 是否正常响应
if curl -sf -X POST http://127.0.0.1:6800/jsonrpc \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":\"ping\",\"method\":\"aria2.getVersion\",\"params\":[$([ -n \"${ARIA2_TOKEN}\" ] && echo \"\\\"token:${ARIA2_TOKEN}\\\"\" || echo)]}" \
    > /dev/null; then
    log_ok "aria2 RPC 响应正常"
else
    log_err "aria2 RPC 无响应，查看日志:"
    tail -20 /var/log/aria2.log || true
    exit 1
fi
log_elapsed

# ================================================================
log_step "4/7 拉取代码并编译"
# ================================================================
log_info "克隆仓库: ${GIT_REPO} (分支: ${GIT_BRANCH})"
git clone --depth=1 --branch "${GIT_BRANCH}" "${GIT_REPO}" /src/deeptrade 2>&1
log_ok "克隆完成"
cd /src/deeptrade
export HOME=/root
export GOCACHE=/root/.cache/go-build
log_info "编译 data-converter (CGO_ENABLED=0)"
CGO_ENABLED=0 go build -v -ldflags='-s -w' -o /usr/local/bin/data-converter ./cmd/data-converter 2>&1
log_ok "编译完成: $(/usr/local/bin/data-converter --help 2>&1 | head -1 || echo 'binary ok')"
log_sys
log_elapsed

# ================================================================
log_step "5/7 生成 config.toml"
# ================================================================
mkdir -p /app/conf
log_info "写入 /app/conf/config.toml"
cat > /app/conf/config.toml << 'TOMLEOF'
[storage]
channel_buffer_size = 10
TOMLEOF

if [ -n "${MYSQL_HOST}" ]; then
    log_info "追加 MySQL 配置: ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}"
cat >> /app/conf/config.toml << TOMLEOF

[storage.mysql]
host = "${MYSQL_HOST}"
port = ${MYSQL_PORT}
user = "${MYSQL_USER}"
password = "${MYSQL_PASSWORD}"
database = "${MYSQL_DATABASE}"
max_open_conns = 5
max_idle_conns = 2
conn_max_life_time = 300
TOMLEOF
fi

log_ok "config.toml 内容:"
cat /app/conf/config.toml
# FREEDOM_PROJECT_CONFIG 指向 conf 目录（freedom 框架约定）
export FREEDOM_PROJECT_CONFIG=/app/conf
log_ok "FREEDOM_PROJECT_CONFIG=${FREEDOM_PROJECT_CONFIG}"
log_elapsed

# ================================================================
log_step "6/7 启动抢占回收监控"
# 配置 aliyun cli（用于动态提价）
aliyun configure set \
    --profile default \
    --mode AK \
    --region "${ECS_REGION}" \
    --access-key-id "{{ACCESS_KEY_ID}}" \
    --access-key-secret "{{ACCESS_KEY_SECRET}}" 2>/dev/null || true

# 出价档位：初始出价倍数，每次提价步进 20%，最高不超过按量价格
SPOT_PRICE_MULTIPLIER=1.0   # 当前倍数（相对市场价）
SPOT_PRICE_MAX_MULTIPLIER=3.0  # 最高出价倍数上限

MAIN_PID_FILE=/tmp/data-converter-main.pid
(
    while true; do
        # 1. 检查回收通知（5分钟内必回收，无法挽救，记录日志）
        TERMINATION=$(curl -sf --max-time 2 \
            http://100.100.100.200/latest/meta-data/instance/spot/termination-time 2>/dev/null || echo "")
        if [ -n "${TERMINATION}" ]; then
            log_err "========================================"
            log_err "!!! 抢占式实例即将被回收: ${TERMINATION} !!!"
            log_err "!!! 当前月份: ${YEAR}-${MONTH}, 实例: ${ECS_INSTANCE_ID} !!!"
            log_err "!!! 注意: 回收通知发出后无法通过提价取消，任务将中断 !!!"
            log_err "========================================"
            # 从文件读取主进程 PID
            if [ -f "${MAIN_PID_FILE}" ]; then
                kill -TERM $(cat ${MAIN_PID_FILE}) 2>/dev/null || true
            fi
            break
        fi

        # 2. 查询当前实例市场价，若市场价超过出价 70% 则主动提价
        INSTANCE_TYPE=$(curl -sf --max-time 2 \
            http://100.100.100.200/latest/meta-data/instance/instance-type 2>/dev/null || echo "")
        if [ -n "${INSTANCE_TYPE}" ] && [ -n "${ECS_INSTANCE_ID}" ]; then
            # 查当前竞价历史最新价格
            MARKET_PRICE=$(aliyun ecs DescribeSpotPriceHistory \
                --RegionId "${ECS_REGION}" \
                --InstanceType "${INSTANCE_TYPE}" \
                --NetworkType vpc \
                --output cols=SpotPrice rows=SpotPrices.SpotPriceType 2>/dev/null \
                | tail -1 | tr -d ' ' || echo "")

            # 查当前实例出价上限
            CURRENT_PRICE=$(aliyun ecs DescribeInstances \
                --RegionId "${ECS_REGION}" \
                --InstanceIds "[\"${ECS_INSTANCE_ID}\"]" \
                --output cols=SpotPriceLimit rows=Instances.Instance 2>/dev/null \
                | tail -1 | tr -d ' ' || echo "")

            if [ -n "${MARKET_PRICE}" ] && [ -n "${CURRENT_PRICE}" ] \
               && awk "BEGIN{exit !(${MARKET_PRICE}+0 > ${CURRENT_PRICE}+0 * 0.7)}" 2>/dev/null; then
                # 市场价超过当前出价 70%，提价 20%
                NEW_PRICE=$(awk "BEGIN{printf \"%.4f\", ${CURRENT_PRICE} * 1.2}")
                MAX_CHECK=$(awk "BEGIN{exit !(${NEW_PRICE}+0 > ${CURRENT_PRICE}+0 * ${SPOT_PRICE_MAX_MULTIPLIER})}" 2>/dev/null && echo "over" || echo "ok")
                if [ "${MAX_CHECK}" = "ok" ]; then
                    log_info "[抢占监控] 市场价 ${MARKET_PRICE} 接近出价 ${CURRENT_PRICE}，提价至 ${NEW_PRICE} 元/小时"
                    aliyun ecs ModifyInstanceSpec \
                        --RegionId "${ECS_REGION}" \
                        --InstanceId "${ECS_INSTANCE_ID}" \
                        --SpotStrategy SpotWithPriceLimit \
                        --SpotPriceLimit "${NEW_PRICE}" 2>/dev/null \
                        && log_ok "[抢占监控] 提价成功: ${NEW_PRICE} 元/小时" \
                        || log_err "[抢占监控] 提价失败，当前市场价: ${MARKET_PRICE}"
                else
                    log_err "[抢占监控] 市场价 ${MARKET_PRICE} 已超出最高出价上限，不再提价"
                fi
            else
                log_info "[抢占监控] 价格正常 - 市场价: ${MARKET_PRICE:-未知}, 当前出价: ${CURRENT_PRICE:-未知}"
            fi
        fi

        sleep 60
    done
) &
SPOT_MONITOR_PID=$!
log_ok "抢占回收监控已启动 (PID: ${SPOT_MONITOR_PID}, 每60秒检查一次，自动提价保护)"

log_step "7/7 运行数据清洗"
# ================================================================
log_info "构建启动参数"
ARGS=(
    -auto-download
    -year "${YEAR}"
    -month "${MONTH}"
    -optimize
    -daily-basic
    -market all
    -type all
    -output /data/parquet
    -download-dir /data/raw
    -aria2-url "http://127.0.0.1:6800/jsonrpc"
    -workers "${WORKERS:-0}"
)

[ -n "${ARIA2_TOKEN}" ]           && ARGS+=(-aria2-token "${ARIA2_TOKEN}")
[ -n "${FILE_SERVER_URL}" ]       && ARGS+=(-fileserver-url "${FILE_SERVER_URL}")
[ -n "${FILE_SERVER_AUTH}" ]      && ARGS+=(-fileserver-auth "${FILE_SERVER_AUTH}")
[ -n "${FILE_SERVER_USER}" ]      && ARGS+=(-fileserver-username "${FILE_SERVER_USER}")
[ -n "${FILE_SERVER_PASS_HASH}" ] && ARGS+=(-fileserver-password "${FILE_SERVER_PASS_HASH}")
[ -z "${MYSQL_HOST}" ]            && ARGS+=(-no-mysql)

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

log_info "启动命令: /usr/local/bin/data-converter ${ARGS[*]}"
log_sys

EXIT_CODE=0
/usr/local/bin/data-converter "${ARGS[@]}" &
MAIN_PID=$!
echo ${MAIN_PID} > ${MAIN_PID_FILE}
log_info "data-converter 已启动 (PID: ${MAIN_PID})"
wait ${MAIN_PID} || EXIT_CODE=$?

# 停止抢占监控进程
kill ${SPOT_MONITOR_PID} 2>/dev/null || true

log_elapsed
if [ ${EXIT_CODE} -eq 0 ]; then
    log_ok "数据清洗成功: ${YEAR}-$(printf '%02d' ${MONTH})"
else
    log_err "数据清洗失败 (exit ${EXIT_CODE}): ${YEAR}-$(printf '%02d' ${MONTH})"
    log_sys
    log_info "aria2 日志最后50行:"
    tail -50 /var/log/aria2.log || true
fi

# ================================================================
log_step "7/7 自动释放 ECS"
# ================================================================
log_info "实例 ID: ${ECS_INSTANCE_ID}"
if ! command -v aliyun &>/dev/null; then
    log_info "安装 aliyun cli"
    wget -q https://aliyuncli.alicdn.com/aliyun-cli-linux-latest-amd64.tgz -O /tmp/aliyun-cli.tgz \
        && tar -C /usr/local/bin -xzf /tmp/aliyun-cli.tgz aliyun \
        && rm -f /tmp/aliyun-cli.tgz
    log_ok "aliyun cli 安装完成: $(aliyun version)"
fi

aliyun configure set \
    --profile default \
    --mode AK \
    --region "${ECS_REGION}" \
    --access-key-id "{{ACCESS_KEY_ID}}" \
    --access-key-secret "{{ACCESS_KEY_SECRET}}"

if [ -n "${ECS_INSTANCE_ID}" ]; then
    log_info "发送 DeleteInstance 请求: ${ECS_INSTANCE_ID}"
    aliyun ecs DeleteInstance \
        --region "${ECS_REGION}" \
        --InstanceId "${ECS_INSTANCE_ID}" \
        --Force true \
        --TerminateSubscription true 2>&1 \
        && log_ok "释放指令已发送" \
        || log_err "自释放失败，请手动释放: ${ECS_INSTANCE_ID}"
else
    log_err "无法获取实例 ID，跳过自释放（请手动释放）"
fi

echo ""
echo "################################################################"
log_info "全部流程结束: ${YEAR}-$(printf '%02d' ${MONTH})"
log_sys
echo "日志文件: ${LOG}"
echo "################################################################"
