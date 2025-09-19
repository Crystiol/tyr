#!/usr/bin/env bash

# 临时调整系统参数以测试 epoll_server
# 注意：这些更改在系统重启后会失效
# 执行前需以 root 权限运行：sudo ./tune_epoll_server.sh

if [ "$EUID" -ne 0 ]; then
    echo "Error: Please run this script as root (use sudo)"
    exit 1
fi

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"; }

# 安全应用 sysctl 参数
apply_sysctl() {
    local param=$1
    local value=$2
    if [ -f "/proc/sys/${param}" ] || [[ $param =~ .*\..* ]]; then
        sysctl -w "$param=$value" >/dev/null 2>&1
        local current=$(sysctl -n "$param" 2>/dev/null)
        if [ "$current" = "$value" ]; then
            log "Set $param = $value"
        else
            log "Warning: $param expected $value, got $current"
        fi
    else
        log "Warning: /proc/sys/$param not exists, skipped"
    fi
}

log "Starting temporary system tuning for epoll server testing..."

# 文件描述符限制
apply_sysctl "fs.file-max" "1048576"
apply_sysctl "fs.epoll.max_user_watches" "1048576"  # 可能不存在

# 进程级 ulimit（仅当前 shell）
ulimit -n 65536
log "Set ulimit -n = $(ulimit -n)"

# TCP 网络参数
apply_sysctl "net.core.somaxconn" "65535"
apply_sysctl "net.ipv4.ip_local_port_range" "1024 65535"
apply_sysctl "net.ipv4.tcp_tw_reuse" "1"
apply_sysctl "net.ipv4.tcp_fin_timeout" "15"
apply_sysctl "net.ipv4.tcp_rmem" "4096 87380 8388608"
apply_sysctl "net.ipv4.tcp_wmem" "4096 65536 8388608"
apply_sysctl "net.ipv4.tcp_max_syn_backlog" "8192"
apply_sysctl "net.core.netdev_max_backlog" "10000"

# 内存管理
apply_sysctl "vm.overcommit_memory" "2"
apply_sysctl "vm.nr_hugepages" "1024"

# 线程和进程限制
apply_sysctl "kernel.threads-max" "65536"
apply_sysctl "kernel.pid_max" "65536"

# 内核日志级别
apply_sysctl "kernel.printk" "4"

log "Temporary system tuning completed."
log "Note: These changes will revert on reboot."
