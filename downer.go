package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
			msg_id TEXT PRIMARY KEY,
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

func fallbackKey(subject, dateStr string) string {
	h := sha256.Sum256([]byte(subject + "|" + dateStr))
	return fmt.Sprintf("FALLBACK-%x", h[:16])
}

//===========================================================================
// 核心：文件夹内并行下载 — 先扫描全部 UID，再并行拉取正文
//===========================================================================

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

	// Phase 1：快速扫描全部序号范围，收集待下载 UID（只拉 Envelope，不拉正文）
	log.Info("🔄 第1阶段：扫描邮件列表...")
	allInfos := make([]uidInfo, 0)
	batchSize := uint32(100)
	for start := uint32(1); start <= total; start += batchSize {
		end := start + batchSize - 1
		if end > total {
			end = total
		}
		infos, err := d.scanUIDs(ctx, start, end, mailbox)
		if err != nil {
			return err
		}
		allInfos = append(allInfos, infos...)
	}

	// Phase 2：并行下载正文
	if len(allInfos) > 0 {
		log.Infof("🔄 第2阶段：并行下载 %d 封邮件（%d 线程）...", len(allInfos), d.Options.Threads)
		err = d.downloadAllParallel(ctx, allInfos, mailbox)
	} else {
		log.Infof("✅ 无新增邮件")
	}

	t2 := time.Since(t1)
	skipped := atomic.LoadUint64(&d.Skipped)
	downloaded := atomic.LoadUint64(&d.Downloaded)
	log.Infof("%s 处理完成: 跳过 %d 封, 下载 %d 封, 耗时 %0.0f 分钟",
		mailbox, skipped, downloaded, t2.Minutes())
	return
}

// downloadAllParallel 将待下载 UID 平均分块，每块一个 goroutine 并行下载
func (d *Downloader) downloadAllParallel(ctx context.Context, allInfos []uidInfo, mailbox string) error {
	numWorkers := d.Options.Threads
	if numWorkers < 1 {
		numWorkers = 1
	}
	if numWorkers > len(allInfos) {
		numWorkers = len(allInfos)
	}

	// 分块
	chunkSize := (len(allInfos) + numWorkers - 1) / numWorkers
	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers)

	for w := 0; w < numWorkers; w++ {
		start := w * chunkSize
		end := start + chunkSize
		if start >= len(allInfos) {
			break
		}
		if end > len(allInfos) {
			end = len(allInfos)
		}
		chunk := allInfos[start:end]

		wg.Add(1)
		go func(workerId int, uids []uidInfo) {
			defer wg.Done()

			// 每个线程独立 IMAP 连接
			subD, err := NewDownloader(d.Options)
			if err != nil {
				log.Errorf("[线程%d] 创建连接失败: %s", workerId, err)
				errCh <- err
				return
			}
			defer func() {
				if subD.DB != nil {
					subD.DB.Close()
				}
				_ = subD.Client.Logout()
			}()

			// 选择相同文件夹
			_, err = subD.Client.Select(mailbox, true)
			if err != nil {
				log.Errorf("[线程%d] 选择文件夹失败: %s", workerId, err)
				errCh <- err
				return
			}

			log.Infof("[线程%d] 下载 %d 封...", workerId, len(uids))
			if err := d.downloadByUIDsWithConn(ctx, subD, uids, mailbox); err != nil {
				log.Errorf("[线程%d] 下载出错: %s", workerId, err)
				errCh <- err
			}
		}(w, chunk)
	}

	wg.Wait()
	close(errCh)

	for e := range errCh {
		if e != nil {
			return e
		}
	}
	return nil
}

// downloadByUIDsWithConn 用指定连接下载一组 UID
func (d *Downloader) downloadByUIDsWithConn(ctx context.Context, subD *Downloader, uidInfos []uidInfo, mailbox string) error {
	items := []imap.FetchItem{
		imap.FetchFlags,
		imap.FetchEnvelope,
		imap.FetchInternalDate,
		imap.FetchRFC822Size,
		imap.FetchUid,
		imap.FetchBodyStructure,
		"BODY[]",
	}

	uids := make([]uint32, len(uidInfos))
	msgIDMap := make(map[uint32]uidInfo, len(uidInfos))
	for i, info := range uidInfos {
		uids[i] = info.UID
		msgIDMap[info.UID] = info
	}

	seq := &imap.SeqSet{}
	seq.AddNum(uids...)

	messages := make(chan *imap.Message, 100)
	done := make(chan error, 1)
	go func() {
		done <- subD.Client.UidFetch(seq, items, messages)
	}()

	count := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-done:
			if err != nil {
				return err
			}
			atomic.AddUint64(&d.Downloaded, uint64(count))
			return nil
		case msg := <-messages:
			if msg == nil {
				continue
			}
			info, ok := msgIDMap[msg.Uid]
			if !ok {
				continue
			}

			storePath := d.getMailStorePath(msg, mailbox)
			if err := d.saveMailWithPath(msg, mailbox, info, storePath); err != nil {
				log.Errorf("❌ 保存邮件失败: %s", err)
				continue
			}
			count++
			if count%50 == 0 {
				log.Infof("[线程] 已下载 %d 封...", count)
			}
		}
	}
}

//===========================================================================
// 以下函数保持不变
//===========================================================================

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

// uidInfo 待下载邮件信息
type uidInfo struct {
	UID     uint32
	MsgID   string
	Subject string
}

// scanUIDs 扫描序号范围，返回尚未下载的 UID 列表
func (d *Downloader) scanUIDs(ctx context.Context, start, end uint32, mailbox string) (infos []uidInfo, err error) {
	seq := &imap.SeqSet{}
	seq.AddRange(start, end)

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
					log.Infof("[%d~%d] 📋 跳过 %d 封，待下载 %d 封", start, end, skipped, len(infos))
				} else {
					log.Infof("[%d~%d] 🔽 待下载 %d 封", start, end, len(infos))
				}
			} else {
				log.Infof("[%d~%d] ✅ 全部跳过", start, end)
			}
			atomic.AddUint64(&d.Skipped, skipped)
			return infos, nil
		case msg := <-messages:
			if msg == nil {
				continue
			}

			msgID := ""
			subject := ""
			if msg.Envelope != nil {
				msgID = msg.Envelope.MessageId
				subject = msg.Envelope.Subject
			}

			if msgID != "" && d.isMsgIDDownloaded(msgID) {
				skipped++
				continue
			}

			storePath := d.getMailStorePath(msg, mailbox)
			if exists, _ := PathExists(storePath); exists {
				if msgID != "" {
					_ = d.markMsgIDDownloaded(msgID, msg.Uid, mailbox, storePath, subject)
				}
				skipped++
				continue
			}

			if msgID == "" {
				fk := fallbackKey(subject, msg.Envelope.Date.Format(time.RFC3339))
				if d.isMsgIDDownloaded(fk) {
					skipped++
					continue
				}
				msgID = fk
			}

			infos = append(infos, uidInfo{
				UID:     msg.Uid,
				MsgID:   msgID,
				Subject: subject,
			})
		}
	}
}

func (d *Downloader) saveMail(ctx context.Context, msg *imap.Message, mailbox string, info uidInfo) (err error) {
	storePath := d.getMailStorePath(msg, mailbox)
	return d.saveMailWithPath(msg, mailbox, info, storePath)
}

// saveMailWithPath 写 EML 文件并记录数据库
func (d *Downloader) saveMailWithPath(msg *imap.Message, mailbox string, info uidInfo, storePath string) (err error) {
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
	subject := "无主题"
	msgID := ""
	if msg.Envelope != nil {
		if msg.Envelope.Subject != "" {
			subject = msg.Envelope.Subject
		}
		msgID = msg.Envelope.MessageId
	}

	var fileName string
	if msgID != "" {
		h := sha256.Sum256([]byte(msgID))
		idPart := fmt.Sprintf("%x", h[:16])
		fileName = fmt.Sprintf("%s-%s.eml", subject, idPart)
	} else {
		t := msg.InternalDate
		fileName = fmt.Sprintf("%s-%d.eml", subject, t.UnixMilli())
	}
	fileName = sanitizeFileName(fileName)

	t := msg.InternalDate
	year := t.Format("2006")
	month := t.Format("01")
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
