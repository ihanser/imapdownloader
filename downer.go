package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/emersion/go-message/charset"

	"database/sql"
	_ "modernc.org/sqlite"
)

type Downloader struct {
	Client     *client.Client
	Options    *Options
	DB         *sql.DB
	Skipped    uint64
	Downloaded uint64
}

func NewDownloader(opts *Options) (d *Downloader, err error) {
	d = &Downloader{}
	d.Options = opts
	imap.CharsetReader = charset.Reader
	cli, err := client.DialTLS(d.Options.Host, nil)
	if err != nil {
		return
	}
	d.Client = cli
	log.Info("已连接到服务器:", d.Options.Host)

	if err = d.Client.Login(d.Options.Username, d.Options.Password); err != nil {
		return
	}
	log.Info("已登录:", d.Options.Username)

	// SQLite 数据库：跟踪已下载邮件（全局去重）
	dbPath := filepath.Join(d.Options.absDir, ".imap_downloaded.db")
	d.DB, err = sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("打开数据库失败: %w", err)
	}

	_, err = d.DB.Exec(`
		CREATE TABLE IF NOT EXISTS downloaded_emails (
			msg_id TEXT PRIMARY KEY,          -- Message-ID（全局唯一标识）
			uid INTEGER NOT NULL,
			mailbox TEXT NOT NULL,
			file_path TEXT,
			subject TEXT,
			downloaded_at TEXT NOT NULL DEFAULT (datetime('now'))
		)
	`)
	if err != nil {
		return nil, fmt.Errorf("初始化数据库失败: %w", err)
	}

	var count int
	d.DB.QueryRow("SELECT COUNT(*) FROM downloaded_emails").Scan(&count)
	log.Infof("已下载数据库: %s (%d 条记录)", dbPath, count)

	log.Info("✅ 连接成功")
	return
}

// isMsgIDDownloaded 检查 Message-ID 是否已下载（跨文件夹去重）
func (d *Downloader) isMsgIDDownloaded(msgID string) bool {
	if msgID == "" {
		return false
	}
	var count int
	err := d.DB.QueryRow("SELECT COUNT(*) FROM downloaded_emails WHERE msg_id=?", msgID).Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
}

// markMsgIDDownloaded 记录已下载的 Message-ID
func (d *Downloader) markMsgIDDownloaded(msgID string, uid uint32, mailbox, filePath, subject string) error {
	if msgID == "" {
		return nil
	}
	_, err := d.DB.Exec(
		"INSERT OR IGNORE INTO downloaded_emails (msg_id, uid, mailbox, file_path, subject) VALUES (?, ?, ?, ?, ?)",
		msgID, uid, mailbox, filePath, subject,
	)
	return err
}

// fallbackKey 当 Message-ID 为空时，用 subject+日期 生成哈希作为降级主键
func fallbackKey(subject, dateStr string) string {
	h := sha256.Sum256([]byte(subject + "|" + dateStr))
	return fmt.Sprintf("FALLBACK-%x", h[:16])
}

func (d *Downloader) downloadAccountMailbox(ctx context.Context, mailbox string) (err error) {
	_, err = d.Client.Select(mailbox, true)
	if err != nil {
		return
	}
	log.Infof("当前邮箱文件夹 %s", mailbox)

	status, err := d.Client.Status(mailbox, []imap.StatusItem{imap.StatusMessages})
	if err != nil {
		status2, err2 := d.Client.Select(mailbox, true)
		if err2 != nil {
			return err
		}
		status = status2
	}

	total := status.Messages
	log.Infof("总数: %d", total)
	if total == 0 {
		return
	}

	dir := filepath.Join(d.Options.absDir, mailbox)
	log.Infof("存放位置: %s", dir)
	t1 := time.Now()

	atomic.StoreUint64(&d.Skipped, 0)
	atomic.StoreUint64(&d.Downloaded, 0)

	batchSize := uint32(100)
	for start := uint32(1); start <= total; start += batchSize {
		end := start + batchSize - 1
		if end > total {
			end = total
		}
		err = d.downloadMailsByRange(ctx, start, end, mailbox)
		if err != nil {
			return
		}
	}

	t2 := time.Since(t1)
	skipped := atomic.LoadUint64(&d.Skipped)
	downloaded := atomic.LoadUint64(&d.Downloaded)
	log.Infof("%s 处理完成: 跳过 %d 封, 下载 %d 封, 耗时 %0.0f 分钟",
		mailbox, skipped, downloaded, t2.Minutes())
	return
}

func (d *Downloader) getPrefixMatchedMailBoxes(ctx context.Context) (mailboxes []string, err error) {
	chBoxes := make(chan *imap.MailboxInfo)
	done := make(chan error, 1)
	go func() {
		done <- d.Client.List("", "*", chBoxes)
	}()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err = <-done:
			return
		case box := <-chBoxes:
			if box == nil {
				continue
			}
			for _, prefix := range d.Options.Prefixes {
				if strings.HasPrefix(box.Name, prefix) {
					mailboxes = append(mailboxes, box.Name)
					break
				}
			}
		}
	}
}

func (d *Downloader) downloadMailsByRange(ctx context.Context, start, end uint32, mailbox string) (err error) {
	uidInfos, err := d.scanUIDs(ctx, start, end, mailbox)
	if err != nil {
		return
	}
	if len(uidInfos) == 0 {
		return
	}
	err = d.downloadByUIDs(ctx, uidInfos, mailbox)
	if err != nil {
		return
	}
	return
}

// uidInfo 待下载邮件信息
type uidInfo struct {
	UID     uint32
	MsgID   string
	Subject string
}

// scanUIDs 扫描序号范围，返回尚未下载的 UID 列表
// 双重检查：① Message-ID 数据库去重（跨文件夹） ② 文件路径存在检查
func (d *Downloader) scanUIDs(ctx context.Context, start, end uint32, mailbox string) (infos []uidInfo, err error) {
	seq := &imap.SeqSet{}
	seq.AddRange(start, end)

	// 只抓 UID + Envelope（含 Message-ID），不下载正文
	messages := make(chan *imap.Message, 100)
	done := make(chan error, 1)
	go func() {
		done <- d.Client.Fetch(seq, []imap.FetchItem{imap.FetchUid, imap.FetchEnvelope}, messages)
	}()

	var skipped uint64
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err = <-done:
			if err != nil {
				return nil, err
			}
			if skipped > 0 || len(infos) > 0 {
				if skipped > 0 {
					log.Infof("[%d~%d] 📋 跳过 %d 封（已存在），待下载 %d 封", start, end, skipped, len(infos))
				} else {
					log.Infof("[%d~%d] 🔽 待下载 %d 封", start, end, len(infos))
				}
			} else {
				log.Infof("[%d~%d] ✅ 全部跳过，无新增邮件", start, end)
			}
			atomic.AddUint64(&d.Skipped, skipped)
			return infos, nil
		case msg := <-messages:
			if msg == nil {
				continue
			}

			// 获取 Message-ID（全局去重主键）
			msgID := ""
			subject := ""
			if msg.Envelope != nil {
				msgID = msg.Envelope.MessageId
				subject = msg.Envelope.Subject
			}

			// 检查 1: Message-ID 数据库去重（跨文件夹）
			if msgID != "" && d.isMsgIDDownloaded(msgID) {
				skipped++
				continue
			}

			// 检查 2: 文件路径存在检查（兼容旧文件）
			storePath := d.getMailStorePath(msg, mailbox)
			if exists, _ := PathExists(storePath); exists {
				// 回填数据库
				if msgID != "" {
					_ = d.markMsgIDDownloaded(msgID, msg.Uid, mailbox, storePath, subject)
				}
				skipped++
				continue
			}

			// 检查 3: 如果没有 Message-ID，用 subject+日期 哈希做降级去重
			if msgID == "" {
				fk := fallbackKey(subject, msg.Envelope.Date)
				if d.isMsgIDDownloaded(fk) {
					skipped++
					continue
				}
				msgID = fk // 用 fallback key 代替空 Message-ID
			}

			infos = append(infos, uidInfo{
				UID:     msg.Uid,
				MsgID:   msgID,
				Subject: subject,
			})
		}
	}
}

// downloadByUIDs 按 UID 列表下载邮件
func (d *Downloader) downloadByUIDs(ctx context.Context, uidInfos []uidInfo, mailbox string) (err error) {
	items := []imap.FetchItem{
		imap.FetchFlags,
		imap.FetchEnvelope,
		imap.FetchInternalDate,
		imap.FetchRFC822Size,
		imap.FetchUid,
		imap.FetchBodyStructure,
		"BODY[]",
	}

	// 提取 UID 列表
	uids := make([]uint32, len(uidInfos))
	msgIDMap := make(map[uint32]uidInfo)
	for i, info := range uidInfos {
		uids[i] = info.UID
		msgIDMap[info.UID] = info
	}

	seq := &imap.SeqSet{}
	seq.AddNum(uids...)

	messages := make(chan *imap.Message, 100)
	done := make(chan error, 1)
	go func() {
		done <- d.Client.UidFetch(seq, items, messages)
	}()

	log.Infof("开始下载 %d 封邮件...", len(uids))
	count := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-done:
			if err != nil {
				log.Errorf("FETCH 出错: %s", err)
				return
			}
			log.Infof("✅ 下载完成，共 %d 封", count)
			atomic.AddUint64(&d.Downloaded, uint64(count))
			return
		case msg := <-messages:
			if msg == nil {
				continue
			}
			err = d.saveMail(ctx, msg, mailbox, msgIDMap[msg.Uid])
			if err != nil {
				log.Errorf("❌ 保存邮件失败: %s", err)
				continue
			}
			count++
			if count%50 == 0 {
				log.Infof("  已下载 %d 封...", count)
			}
		}
	}
}

func (d *Downloader) saveMail(ctx context.Context, msg *imap.Message, mailbox string, info uidInfo) (err error) {
	storePath := d.getMailStorePath(msg, mailbox)
	dir := filepath.Dir(storePath)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return
	}

	for _, literal := range msg.Body {
		if body, err := io.ReadAll(literal); err == nil && len(body) > 0 {
			if err = os.WriteFile(storePath, body, 0644); err != nil {
				return err
			}
			// 保存成功后写入数据库
			if err := d.markMsgIDDownloaded(info.MsgID, msg.Uid, mailbox, storePath, info.Subject); err != nil {
				log.Warnf("记录数据库失败: %s", err)
			}
			log.Infof("  💾 已保存: %s", filepath.Base(storePath))
			return nil
		}
	}

	log.Warnf("邮件 UID %d 无有效内容，跳过", msg.Uid)
	return nil
}

func (d *Downloader) getMailStorePath(msg *imap.Message, mailbox string) string {
	t := msg.InternalDate
	year := t.Format("2006")
	month := t.Format("01")
	subject := "无主题"
	if msg.Envelope != nil && msg.Envelope.Subject != "" {
		subject = msg.Envelope.Subject
	}
	fileName := fmt.Sprintf("%s-%d.eml", subject, t.UnixMilli())
	fileName = sanitizeFileName(fileName)
	dir := filepath.Join(d.Options.absDir, mailbox, year, month)
	return filepath.Join(dir, fileName)
}

func sanitizeFileName(name string) string {
	replacer := strings.NewReplacer(
		"\\", "_",
		"/", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
	)
	return replacer.Replace(name)
}
