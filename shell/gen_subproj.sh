#!/bin/bash
# generate_cmakelists.sh
# 用法: ./generate_cmakelists.sh <target_directory>

set -e

# 检查参数
if [ $# -ne 1 ]; then
    echo "Usage: $0 <target_directory>"
    exit 1
fi

TARGET_DIR="$1"

# 检查目录是否存在
if [ ! -d "$TARGET_DIR" ]; then
    echo "Error: directory '$TARGET_DIR' does not exist."
    exit 1
fi

# 用目录名作为 project 名
PROJECT_NAME=$(basename "$TARGET_DIR")

OUTPUT="$TARGET_DIR/CMakeLists.txt"

# 写入文件头部
echo "cmake_minimum_required(VERSION 3.15)" > "$OUTPUT"
echo "project(${PROJECT_NAME} C)" >> "$OUTPUT"
echo "" >> "$OUTPUT"

# 遍历指定目录下所有 .c 文件
for src in "$TARGET_DIR"/*.c; do
    # 如果目录下没有 .c 文件，跳过
    [ -e "$src" ] || continue

    exe_name=$(basename "$src" .c)
    echo "add_executable(${exe_name} ${exe_name}.c)" >> "$OUTPUT"
    echo "target_link_libraries(${exe_name} PRIVATE tlpi)" >> "$OUTPUT"
    echo "" >> "$OUTPUT"
done

echo "Generated $OUTPUT"

