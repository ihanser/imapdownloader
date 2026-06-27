# imapdownloader 优化版（树莓派5 加速版）

基于 [weibaohui/imapdownloader](https://github.com/weibaohui/imapdownloader) 优化。

## 🚀 两项加速

### 1️⃣ IMAP COMPRESS（DEFLATE 压缩）
在 IMAP 协议层开启压缩，纯文本邮件**传输量减少 70-80%**。

### 2️⃣ 并行下载
同时建立多个 IMAP 连接，每个下载一个文件夹，**速度 ×N**。

### 综合效果
| 场景 | 加速比 |
|---|---|
| 单文件夹（压缩） | **3-5×** |
| 4 文件夹（压缩 + 并行） | **12-20×** |

---

## 📦 方式一：GitHub Actions 在线编译（推荐）

### 1. 创建 GitHub 仓库

```bash
# 在树莓派上创建本地仓库并推送到 GitHub
cd ~/imapdownloader
git init
git add .
git commit -m "init: imapdownloader RPi5 optimized"
git remote add origin https://github.com/YOUR_USER/imapdownloader.git
git push -u origin main
```

### 2. 触发编译

两种方式：
- **自动**：推送代码后自动触发
- **手动**：在 GitHub 页面 → Actions → **编译 imapdownloader（RPi5 优化版）** → Run workflow

### 3. 下载二进制

编译完成后，进入 Actions 页面 → 点击完成的工作流 → **Artifacts** → 下载 `imapdownloader_linux_arm64`

### 4. 放到树莓派运行

```bash
# 在 RPi5 上
chmod +x imapdownloader_linux_arm64
mv imapdownloader_linux_arm64 imapdownloader
nano config.yaml   # 配置邮箱
./imapdownloader
```

---

## ⚙️ 方式二：树莓派本地编译

```bash
sudo apt install golang-go -y
chmod +x build.sh
./build.sh
```

---

## ⚙️ 配置

编辑 `config.yaml`：

```yaml
dir: backup
host: imap.qq.com:993
username: your@email.com
password: 授权码（QQ邮箱需填授权码）
prefixes:
  - 收件箱
  - 已发送
parallel: 4  # 并行连接数
```

---

## 🔗 完整管线

```bash
# 1. 下载邮件
./imapdownloader

# 2. EML → TXT
python3 eml_to_txt.py backup -o txt_output -j 4

# 3. 清洗
python3 clean_emails.py txt_output -o emails_cleaned.txt -j 4
```
