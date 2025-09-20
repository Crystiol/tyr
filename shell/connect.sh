#!/usr/bin/bash

# 临时调整系统参数以测试 et_server
# 注意：这些更改在系统重启后会失效
# 执行前需以 root 权限运行：sudo ./netopt.sh

pkill client

# 检查是否以 root 权限运行
if [ "$EUID" -ne 0 ]; then
    echo "Error: Please run this script as root (use sudo)"
    exit 1
fi

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# 应用 sysctl 参数
apply_sysctl() {
    local param=$1
    local value=$2
    echo "$value" > "/proc/sys/$param" 2>/dev/null
    if [ $? -eq 0 ]; then
        log "Set $param = $value"
    else
        log "Error: Failed to write $param"
    fi
}

log "Starting temporary system tuning for epoll server testing..."

# -------------------------
# 1. 文件描述符限制
# -------------------------
apply_sysctl "fs/file-max" "1048576"
apply_sysctl "fs/epoll/max_user_watches" "1048576"
ulimit -n 65536
current_ulimit=$(ulimit -n)
log "Set ulimit -n = $current_ulimit"

# -------------------------
# 2. TCP 网络参数
# -------------------------
apply_sysctl "net/core/somaxconn" "65535"
apply_sysctl "net/ipv4/ip_local_port_range" "1024 65535"
apply_sysctl "net/ipv4/tcp_tw_reuse" "1"
apply_sysctl "net/ipv4/tcp_fin_timeout" "15"
apply_sysctl "net/ipv4/tcp_rmem" "4096 87380 8388608"
apply_sysctl "net/ipv4/tcp_wmem" "4096 65536 8388608"
apply_sysctl "net/ipv4/tcp_max_syn_backlog" "8192"
apply_sysctl "net/core/netdev_max_backlog" "10000"

# -------------------------
# 3. 内存管理（默认不启用）
# -------------------------
# apply_sysctl "vm/overcommit_memory" "2"
# apply_sysctl "vm/nr_hugepages" "1024"

# -------------------------
# 4. 线程和进程限制
# -------------------------
apply_sysctl "kernel/threads-max" "65536"
apply_sysctl "kernel/pid_max" "65536"

# -------------------------
# 5. 内核日志级别
# -------------------------
apply_sysctl "kernel/printk" "4"

# -------------------------
# 验证参数
# -------------------------
verify_param() {
    local param=$1
    local expected=$2
    local current
    current=$(cat "/proc/sys/$param" 2>/dev/null | tr -s '\t' ' ')

    # 如果 expected 为空，表示没有设置过，跳过
    if [ -z "$expected" ]; then
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

    # 四元参数（kernel/printk）
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

log "Verifying applied configurations..."
verify_param "fs/file-max" "1048576"
verify_param "fs/epoll/max_user_watches" "1048576"
verify_param "net/core/somaxconn" "65535"
verify_param "net/ipv4/ip_local_port_range" "1024 65535"
verify_param "net/ipv4/tcp_tw_reuse" "1"
verify_param "net/ipv4/tcp_fin_timeout" "15"
verify_param "net/ipv4/tcp_rmem" "4096 87380 8388608"
verify_param "net/ipv4/tcp_wmem" "4096 65536 8388608"
verify_param "net/ipv4/tcp_max_syn_backlog" "8192"
verify_param "net/core/netdev_max_backlog" "10000"
verify_param "vm/overcommit_memory" ""   # 跳过
verify_param "vm/nr_hugepages" ""        # 跳过
verify_param "kernel/threads-max" "65536"
verify_param "kernel/pid_max" "65536"
verify_param "kernel/printk" "4"

log "Verified ulimit -n = $current_ulimit"

log "Temporary system tuning completed. Ready for epoll server testing."
log "Note: These changes are temporary and will revert on reboot."

#exec ./et_server/build/Desktop_Qt_5_15_2_GCC_64bit-Debug/et_server
#exec ./pingpong_client ./pingpong_client -p 9000 -t 1 -d 60 -c 20000 -H 127.0.0.2

ip addr add 127.0.0.2/8 dev lo
ip addr add 127.0.0.3/8 dev lo
ip addr add 127.0.0.4/8 dev lo
ip addr add 127.0.0.5/8 dev lo
ip addr show dev lo

log "Starting multiple pingpong_client instances..."

for i in $(seq 1 5); do
    ip="127.0.0.$i"
    log "Launching pingpong_client with IP $ip"

    # 每个客户端分 8 批发送，每批 2000 个连接
    for batch in $(seq 1 8); do
        start_conn=$(( (batch - 1) * 2000 + 1 ))
        log "Client $ip: batch $batch starting connections $start_conn to $((start_conn + 1999))"
        
        ./client "$ip" 127.0.0.1 9000 2000 &
        sleep 1   # 每批间隔 1 秒，可根据需要调整
    done
done

log "All pingpong_client instances started."
#sleep 10000

exit 0

