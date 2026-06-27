package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/emersion/go-message/charset"
)

type Downloader struct {
	Client  *client.Client
	Options *Options
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

	log.Info("✅ 连接成功")
	return
}

func (d *Downloader) downloadAccountMailbox(ctx context.Context, mailbox string) (err error) {
	_, err = d.Client.Select(mailbox, true)
	if err != nil {
		return
	}
	log.Infof("当前邮箱文件夹 %s", mailbox)

	// 获取邮件总数
	status, err := d.Client.Status(mailbox, []imap.StatusItem{imap.StatusMessages})
	if err != nil {
		// 如果 Status 不支持，尝试 Select 获取总数
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

	batchSize := uint32(100)
	for start := uint32(1); start <= total; start += batchSize {
		end := start + batchSize - 1
		if end > total {
			end = total
		}
		log.Debugf("处理第 %d~%d 批", start, end)
		err = d.downloadMailsByRange(ctx, start, end, mailbox)
		if err != nil {
			return
		}
	}

	t2 := time.Since(t1)
	log.Infof("%s 下载耗时: %0.0f 分钟", mailbox, t2.Minutes())
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
			log.Infof("枚举邮箱文件夹结束")
			return
		case box := <-chBoxes:
			if box == nil {
				continue
			}
			log.Infof("发现邮箱文件夹: %s", box.Name)
			for _, prefix := range d.Options.Prefixes {
				if strings.HasPrefix(box.Name, prefix) {
					log.Infof("符合前缀条件: %s", box.Name)
					mailboxes = append(mailboxes, box.Name)
					break
				}
			}
		}
	}
}

func (d *Downloader) downloadMailsByRange(ctx context.Context, start, end uint32, mailbox string) (err error) {
	uids, err := d.getNewUIDs(ctx, start, end, mailbox)
	if err != nil {
		return
	}
	if len(uids) == 0 {
		return
	}
	err = d.downloadByUIDs(ctx, uids, mailbox)
	if err != nil {
		return
	}
	return
}

// getNewUIDs 获取该范围内尚未下载的邮件 UID
func (d *Downloader) getNewUIDs(ctx context.Context, start, end uint32, mailbox string) (uids []uint32, err error) {
	seq := &imap.SeqSet{}
	seq.AddRange(start, end)

	messages := make(chan *imap.Message, 100)
	done := make(chan error, 1)
	go func() {
		done <- d.Client.Fetch(seq, []imap.FetchItem{imap.FetchUid}, messages)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err = <-done:
			if err != nil {
				return nil, err
			}
			if len(uids) == 0 {
				log.Infof("[%d~%d] 本批次无新增邮件", start, end)
			}
			return uids, nil
		case msg := <-messages:
			if msg == nil {
				continue
			}
			storePath := d.getMailStorePath(msg, mailbox)
			exists, _ := PathExists(storePath)
			if exists {
				continue
			}
			uids = append(uids, msg.Uid)
		}
	}
}

// downloadByUIDs 按 UID 列表下载邮件
func (d *Downloader) downloadByUIDs(ctx context.Context, uids []uint32, mailbox string) (err error) {
	items := []imap.FetchItem{
		imap.FetchFlags,
		imap.FetchEnvelope,
		imap.FetchInternalDate,
		imap.FetchRFC822Size,
		imap.FetchUid,
		imap.FetchBodyStructure,
		"BODY[]",
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
			log.Infof("下载完成，共 %d 封", count)
			return
		case msg := <-messages:
			if msg == nil {
				continue
			}
			err = d.saveMail(ctx, msg, mailbox)
			if err != nil {
				log.Errorf("保存邮件失败: %s", err)
				continue
			}
			count++
			if count%50 == 0 {
				log.Infof("已下载 %d 封...", count)
			}
		}
	}
}

func (d *Downloader) saveMail(ctx context.Context, msg *imap.Message, mailbox string) (err error) {
	storePath := d.getMailStorePath(msg, mailbox)
	exists, _ := PathExists(storePath)
	if exists {
		return
	}
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
			log.Debugf("已保存: %s", storePath)
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
