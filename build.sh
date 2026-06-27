#!/bin/bash
# imapdownloader 优化版 - 树莓派5 编译脚本

set -e

cd "$(dirname "$0")"
echo "=== 编译 imapdownloader（树莓派5 优化版）==="
go version

# 先试压缩版
echo ">>> 尝试压缩版编译..."
if go mod tidy 2>/dev/null && go build -ldflags="-s -w" -o imapdownloader . 2>/dev/null; then
    echo "✅ 压缩版编译成功（IMAP COMPRESS + 并行下载）"
else
    echo ">>> 压缩版不兼容，切换到无压缩版（保留并行下载）..."
    cp downloader_no_compress.go downloader.go
    cat > go.mod << 'GOMOD'
module github.com/weibaohui/imapdownloader

go 1.19

require (
	github.com/emersion/go-imap v1.2.1
	github.com/emersion/go-message v0.18.1
	github.com/sirupsen/logrus v1.9.3
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/emersion/go-sasl v0.0.0-20200509203442-7bfe0ed36a21 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)
GOMOD
    go mod tidy
    go build -ldflags="-s -w" -o imapdownloader .
    echo "✅ 并行下载版编译成功"
fi

echo ""
echo "下一步："
echo "  1. nano config.yaml   # 配置邮箱"
echo "  2. ./imapdownloader   # 运行"
