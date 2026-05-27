#!/usr/bin/bash

# 系统参数验证脚本（仅验证，不修改）
# 用法：sudo ./check_sysctl_params.sh   （读取部分参数可能需要 root 权限）

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# 验证单个 sysctl 参数
# 参数: $1=参数路径, $2=期望值（字符串）
verify_param() {
    local param=$1
    local expected=$2
    local current
    current=$(cat "/proc/sys/$param" 2>/dev/null | tr -s '\t' ' ')

    # 如果期望值为空，跳过验证
    if [ -z "$expected" ]; then
        return
    fi

    # 如果无法读取当前值
    if [ -z "$current" ]; then
        log "Error: Cannot read $param"
        return
    fi

    read -r -a exp_arr <<< "$expected"
    read -r -a cur_arr <<< "$current"

    # 三元参数（tcp_rmem, tcp_wmem, ip_local_port_range）
    if [ ${#exp_arr[@]} -eq 3 ] && [ ${#cur_arr[@]} -eq 3 ]; then
        if [ "${cur_arr[0]}" = "${exp_arr[0]}" ] && [ "${cur_arr[2]}" = "${exp_arr[2]}" ]; then
            if [ "${cur_arr[1]}" = "${exp_arr[1]}" ]; then
                log "Verified $param = $current"
            else
                log "Notice: $param default=${cur_arr[1]} (expected ${exp_arr[1]}), kernel adjusted it"
            fi
        else
            log "Warning: $param = $current (expected: $expected)"
        fi
        return
    fi

    # 四元参数（kernel/printk）：仅比较第一个值（console_loglevel）
    if [ "$param" = "kernel/printk" ] && [ ${#cur_arr[@]} -ge 1 ]; then
        if [ "${cur_arr[0]}" = "$expected" ]; then
            log "Verified $param console_loglevel=${cur_arr[0]}"
        else
            log "Warning: $param console_loglevel=${cur_arr[0]} (expected: $expected)"
        fi
        return
    fi

    # 默认情况：严格比较
    if [ "$current" = "$expected" ]; then
        log "Verified $param = $current"
    else
        log "Warning: $param = $current (expected: $expected)"
    fi
}

# 开始系统级别验证
log "Starting system parameters verification..."

# 定义需要验证的参数列表（参数名:期望值）
declare -A params=(
    ["fs/file-max"]="1048576"
    ["fs/epoll/max_user_watches"]="1048576"
    ["net/core/somaxconn"]="65535"
    ["net/ipv4/ip_local_port_range"]="1024 65535"
    ["net/ipv4/tcp_tw_reuse"]="1"
    ["net/ipv4/tcp_fin_timeout"]="15"
    ["net/ipv4/tcp_rmem"]="4096 87380 8388608"
    ["net/ipv4/tcp_wmem"]="4096 65536 8388608"
    ["net/ipv4/tcp_max_syn_backlog"]="8192"
    ["net/core/netdev_max_backlog"]="10000"
    ["kernel/threads-max"]="65536"
    ["kernel/pid_max"]="65536"
    ["kernel/printk"]="4"
)

# 执行验证
for param in "${!params[@]}"; do
    verify_param "$param" "${params[$param]}"
done

# 验证 ulimit -n（进程级别文件描述符软限制）
expected_ulimit=65536
current_ulimit=$(ulimit -n)
if [ "$current_ulimit" -eq "$expected_ulimit" ]; then
    log "Verified ulimit -n = $current_ulimit"
else
    log "Warning: ulimit -n = $current_ulimit (expected: $expected_ulimit)"
fi

log "Verification completed."
