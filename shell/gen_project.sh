#!/bin/bash

# 生成子工程CMakeLists.txt：gen_project.sh tlpi-dist
# 检查是否传入了目录参数
if [ -z "$1" ]; then
    echo "Usage: $0 <root_directory>"
    exit 1
fi

ROOT_DIR="$1"

# 枚举 root 目录下的所有一级子目录
for dir in "$ROOT_DIR"/*/; do
    # 去掉末尾的斜杠
    dir=${dir%/}

    # 调用 gen_project.sh，传入目录名作为参数
    ./gen_subproj.sh "$dir"
done

