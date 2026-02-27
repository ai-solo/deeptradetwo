#!/bin/bash

# 通联数据采集系统 - 订阅管理脚本

set -e

# 数据库连接信息（从配置文件读取）
DB_HOST=$(grep -A 10 '\[storage.mysql\]' conf/config.toml | grep 'host' | awk '{print $2}' | tr -d '"')
DB_PORT=$(grep -A 10 '\[storage.mysql\]' conf/config.toml | grep 'port' | awk '{print $2}')
DB_USER=$(grep -A 10 '\[storage.mysql\]' conf/config.toml | grep 'user' | awk '{print $2}' | tr -d '"')
DB_PASS=$(grep -A 10 '\[storage.mysql\]' conf/config.toml | grep 'password' | awk '{print $2}' | tr -d '"')
DB_NAME=$(grep -A 10 '\[storage.mysql\]' conf/config.toml | grep 'database' | awk '{print $2}' | tr -d '"')

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 显示使用帮助
show_help() {
    echo "通联数据采集系统 - 订阅管理"
    echo ""
    echo "用法: $0 [命令] [参数]"
    echo ""
    echo "命令:"
    echo "  list                          查看所有订阅"
    echo "  add <category> <security>     添加订阅"
    echo "  remove <category>             删除订阅"
    echo "  import <file>                 从文件批量导入订阅"
    echo "  export <file>                 导出订阅到文件"
    echo ""
    echo "参数说明:"
    echo "  category  订阅类别，格式: <ServiceID>.<MessageID>.<SecurityCode>"
    echo "            ServiceID: 3=上交所, 5=深交所"
    echo "            MessageID: 8=股票快照"
    echo "            例如: 3.8.600000 (上交所浦发银行)"
    echo ""
    echo "示例:"
    echo "  $0 list                                    # 查看所有订阅"
    echo "  $0 add 3.8.600000 浦发银行                 # 添加订阅"
    echo "  $0 remove 3.8.600000                       # 删除订阅"
    echo "  $0 import subscriptions.txt                # 批量导入"
    echo ""
}

# 列出所有订阅
list_subscriptions() {
    echo "========================================="
    echo "当前订阅列表"
    echo "========================================="
    echo ""

    mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "
        SELECT
            CONCAT('[', SUBSTRING_INDEX(category_id, '.', 3), ']') as Category,
            CASE SUBSTRING_INDEX(category_id, '.', 1)
                WHEN '3' THEN '上交所'
                WHEN '5' THEN '深交所'
                ELSE '未知'
            END as Exchange,
            security_id as SecurityCode,
            security_name as SecurityName,
            sid as ServiceID,
            mid as MessageID,
            is_active as Active
        FROM tonglian_subscriptions
        ORDER BY is_active DESC, category_id;
    " 2>/dev/null

    echo ""
    echo "总数:"
    mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "
        SELECT
            COUNT(*) as Total,
            SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END) as Active
        FROM tonglian_subscriptions;
    " 2>/dev/null
}

# 添加订阅
add_subscription() {
    local category="$1"
    local security_name="$2"

    if [ -z "$category" ]; then
        echo -e "${RED}错误: 请提供订阅类别${NC}"
        exit 1
    fi

    # 解析类别
    IFS='.' read -ra PARTS <<< "$category"
    local service_id="${PARTS[0]}"
    local message_id="${PARTS[1]}"
    local security_id="${PARTS[2]}"

    if [ -z "$service_id" ] || [ -z "$message_id" ] || [ -z "$security_id" ]; then
        echo -e "${RED}错误: 订阅类别格式不正确${NC}"
        echo "正确格式: <ServiceID>.<MessageID>.<SecurityCode>"
        echo "示例: 3.8.600000"
        exit 1
    fi

    # 如果没有提供证券名称，使用证券代码
    if [ -z "$security_name" ]; then
        security_name="$security_id"
    fi

    echo "添加订阅: $category ($security_name)"

    # 插入数据库
    mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "
        INSERT INTO tonglian_subscriptions (category_id, security_id, security_name, sid, mid, is_active, created_at, updated_at)
        VALUES ('$category', '$security_id', '$security_name', $service_id, $message_id, 1, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
            is_active = 1,
            security_name = '$security_name',
            updated_at = NOW();
    " 2>/dev/null

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ 订阅添加成功${NC}"
        echo "  注意: 需要重启通联数据采集服务以生效"
    else
        echo -e "${RED}✗ 订阅添加失败${NC}"
        exit 1
    fi
}

# 删除订阅
remove_subscription() {
    local category="$1"

    if [ -z "$category" ]; then
        echo -e "${RED}错误: 请提供订阅类别${NC}"
        exit 1
    fi

    echo "删除订阅: $category"

    # 更新数据库（设置为不活跃）
    mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "
        UPDATE tonglian_subscriptions
        SET is_active = 0, updated_at = NOW()
        WHERE category_id = '$category';
    " 2>/dev/null

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ 订阅删除成功${NC}"
        echo "  注意: 需要重启通联数据采集服务以生效"
    else
        echo -e "${RED}✗ 订阅删除失败${NC}"
        exit 1
    fi
}

# 批量导入订阅
import_subscriptions() {
    local file="$1"

    if [ -z "$file" ]; then
        echo -e "${RED}错误: 请提供导入文件${NC}"
        exit 1
    fi

    if [ ! -f "$file" ]; then
        echo -e "${RED}错误: 文件不存在: $file${NC}"
        exit 1
    fi

    echo "从文件导入订阅: $file"

    local count=0
    while IFS=$',' read -r category security_name; do
        # 跳过空行和注释
        [[ -z "$category" || "$category" == \#* ]] && continue

        add_subscription "$category" "$security_name"
        ((count++))
    done < "$file"

    echo ""
    echo -e "${GREEN}✓ 成功导入 $count 条订阅${NC}"
}

# 导出订阅到文件
export_subscriptions() {
    local file="$1"

    if [ -z "$file" ]; then
        echo -e "${RED}错误: 请提供输出文件${NC}"
        exit 1
    fi

    echo "导出订阅到文件: $file"

    mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASS" "$DB_NAME" -e "
        SELECT category_id, security_name
        FROM tonglian_subscriptions
        WHERE is_active = 1
        ORDER BY category_id
    " 2>/dev/null | tail -n +2 > "$file"

    local count=$(wc -l < "$file")
    echo -e "${GREEN}✓ 成功导出 $count 条订阅${NC}"
}

# 主函数
main() {
    if [ $# -eq 0 ]; then
        show_help
        exit 0
    fi

    local command="$1"
    shift

    case "$command" in
        list)
            list_subscriptions
            ;;
        add)
            add_subscription "$@"
            ;;
        remove)
            remove_subscription "$@"
            ;;
        import)
            import_subscriptions "$@"
            ;;
        export)
            export_subscriptions "$@"
            ;;
        help|-h|--help)
            show_help
            ;;
        *)
            echo -e "${RED}错误: 未知命令 '$command'${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

cd "$(dirname "$0")/.."
main "$@"
