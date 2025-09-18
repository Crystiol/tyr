#!/bin/bash
set -e

if [ $# -lt 1 ]; then
    echo "用法: $0 <目录>"
    exit 1
fi

DIR=$(realpath "$1")
PROJECT_NAME=$(basename "$DIR")
OUT_FILE="$DIR/CMakeLists.txt"

{
    echo "cmake_minimum_required(VERSION 3.10)"
    echo "project(${PROJECT_NAME})"
    echo ""

    # 枚举一级子目录，按名字排序
    for d in $(find "$DIR" -mindepth 1 -maxdepth 1 -type d | sort); do
        SUBDIR=$(basename "$d")
        echo "add_subdirectory(${SUBDIR})"
    done
} > "$OUT_FILE"

echo "生成完成: $OUT_FILE"

