# ---- 编译阶段 ----
FROM golang:1.24-bookworm AS builder

WORKDIR /src

# 先拷贝 go.mod/go.sum，利用缓存层
COPY go.mod go.sum ./
RUN go mod download

# 拷贝源码并编译
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags='-s -w' -o /data-converter ./cmd/data-converter

# ---- 运行阶段 ----
FROM ubuntu:22.04

# 安装运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 时区设置
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 拷贝二进制和配置
COPY --from=builder /data-converter /usr/local/bin/data-converter
COPY conf/config.toml /app/conf/config.toml
COPY deploy/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# 工作目录
WORKDIR /app

# 数据目录
RUN mkdir -p /data/raw /data/parquet

ENTRYPOINT ["/app/entrypoint.sh"]
