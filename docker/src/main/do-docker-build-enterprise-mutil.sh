#!/bin/bash

# 通过 macOS 构建多平台Docker镜像脚本
# 用法: ./do-docker-build-enterprise-mutil.sh <version> <user> [dockerfile]

set -e  # 遇到错误立即退出

# 显示用法信息
show_usage() {
    echo "用法: $0 <version> <user> [dockerfile]"
    echo "示例: $0 2.0.6.3 xuan.wang Dockerfile-1.0.0-standalone-mutil-platform"
    echo "参数说明:"
    echo "  version  - 镜像版本号 (必需)"
    echo "  user     - 维护者名称 (必需)" 
    echo "  dockerfile - Dockerfile文件名 [可选，默认为Dockerfile-1.0.0-standalone-mutil-platform]"
}

# 参数校验
if [ $# -lt 2 ]; then
    echo "错误: 缺少必要参数"
    show_usage
    exit 1
fi

# 设置变量
VERSION="$1"
USER="$2"
DOCKERFILE="${3:-Dockerfile-1.0.0-standalone-mutil-platform}"
TARGET_FILE="target/timechodb-${VERSION}-bin.zip"
IMAGE_NAME="nexus.infra.timecho.com:8243/timecho/timechodb:${VERSION}-standalone"

# 检查目标文件是否存在
echo "检查目标文件是否存在..."
if [ ! -f "$TARGET_FILE" ]; then
    echo "错误: 目标文件 $TARGET_FILE 不存在!"
    echo "请确保版本号正确且文件已生成。"
    exit 1
fi

echo "✅ 目标文件校验通过: $TARGET_FILE"

# 获取Git提交ID和当前时间
COMMIT_ID=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE=$(date -Iseconds)

# 显示构建信息
echo "=========================================="
echo "Docker 多平台镜像构建"
echo "版本: $VERSION"
echo "维护者: $USER"
echo "Dockerfile: $DOCKERFILE"
echo "提交ID: $COMMIT_ID"
echo "构建时间: $BUILD_DATE"
echo "目标平台: linux/amd64,linux/arm64,linux/arm/v7,linux/arm/v6"
echo "=========================================="

# 确认是否继续
read -p "是否继续构建? (y/N): " confirm
if [[ ! $confirm =~ ^[Yy]$ ]]; then
    echo "构建已取消。"
    exit 0
fi

# 执行Docker构建命令
echo "开始构建Docker镜像..."
docker buildx build \
  --platform linux/amd64,linux/arm64,linux/arm/v7,linux/arm/v6 \
  -f "$DOCKERFILE" \
  --build-arg version="$VERSION" \
  --build-arg target="timechodb-${VERSION}-bin" \
  --label build_date="$BUILD_DATE" \
  --label maintainer="$USER" \
  --label commit_id="$COMMIT_ID" \
  --no-cache \
  -t "$IMAGE_NAME" . \
  --push

# 检查构建结果
if [ $? -eq 0 ]; then
    echo "✅ Docker镜像构建并推送成功!"
    echo "镜像地址: $IMAGE_NAME"
else
    echo "❌ Docker镜像构建失败!"
    exit 1
fi