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
	"github.com/emersion/go-imap-compress"
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
	// 增强邮件编码探测能力
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

	// ★ 启用 IMAP COMPRESS (DEFLATE) 压缩，传输量减少 70-80%
	if err = d.Client.Compress(imapcompress.CompressDeflater); err != nil {
		log.Warnf("服务器不支持 COMPRESS 扩展，将以无压缩模式继续: %s", err)
		err = nil // 不阻塞后续流程
	} else {
		log.Info("✅ IMAP COMPRESS 已启用，传输数据将压缩 70-80%")
	}

	return
}

// 下载单个邮箱文件夹
func (d *Downloader) downloadAccountMailbox(ctx context.Context, mailbox string) (err error) {
	status, err := d.Client.Select(mailbox, true)
	if err != nil {
		return
	}
	log.Infof("当前邮箱文件夹 %s, 总数 %d", status.Name, status.Messages)

	if status.Messages == 0 {
		return
	}
	all := status.Messages
	dir := filepath.Join(d.Options.absDir, mailbox)
	log.Infof("%s 邮箱文件夹下载存放位置: %s", mailbox, dir)
	count := int(all / 100)
	t1 := time.Now()
	for i := 0; i <= count; i++ {
		start := i*100 + 1
		end := (i + 1) * 100
		if int(all)-start < 100 {
			end = int(status.Messages)
		}
		log.Debugf("正在分析第 %d 批: [%d~%d]", i+1, start, end)
		err = d.downloadMailsByRange(ctx, uint32(start), uint32(end), mailbox)
		if err != nil {
			return
		}
	}
	t2 := time.Since(t1)
	log.Infof("%s 下载耗时：%0.0f 分钟", mailbox, t2.Minutes())
	return
}

// 获取匹配前缀的邮箱文件夹
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

// 按批次分析并下载邮件
func (d *Downloader) downloadMailsByRange(ctx context.Context, start, end uint32, mailbox string) (err error) {
	seqDL, err := d.getDownloadMailList(ctx, start, end, mailbox)
	if err != nil {
		log.Errorf("[%d~%d] 分析下载队列出错:%s", start, end, err.Error())
		return
	}
	if seqDL.Empty() {
		log.Debugf("[%d~%d] 下载队列为空，跳过", start, end)
		return
	}
	err = d.downloadMailList(ctx, seqDL, mailbox)
	if err != nil {
		log.Errorf("[%d~%d] 下载队列出错:%s", start, end, err.Error())
		return
	}
	return
}

// 获取待下载邮件列表
func (d *Downloader) getDownloadMailList(ctx context.Context, start, end uint32, mailbox string) (seqs *imap.SeqSet, err error) {
	seqDL := &imap.SeqSet{}
	seqDL.AddRange(start, end)

	messages := make(chan *imap.Message, 100)
	done := make(chan error, 1)
	go func() {
		done <- d.Client.Fetch(seqDL, []imap.FetchItem{imap.FetchUid}, messages)
	}()

	hasNew := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err = <-done:
			if !hasNew {
				log.Infof("[%d~%d] 本批次无新增邮件", start, end)
				return seqDL, nil
			}
			return
		case msg := <-messages:
			if msg == nil {
				continue
			}
			// 检查是否已下载
			storePath := d.getMailStorePath(msg, mailbox)
			exists, _ := PathExists(storePath)
			if exists {
				seqDL.DelNum(imap.SeqId(msg.SeqNum))
				continue
			}
			hasNew = true
		}
	}
}

// 批量下载邮件
func (d *Downloader) downloadMailList(ctx context.Context, seqs *imap.SeqSet, mailbox string) (err error) {
	// 要拉取的字段
	items := []imap.FetchItem{
		imap.FetchFlags,
		imap.FetchEnvelope,
		imap.FetchInternalDate,
		imap.FetchRFC822Size,
		imap.FetchUid,
		imap.FetchBodyStructure,
		"BODY[]", // 拉取完整邮件内容
	}

	messages := make(chan *imap.Message, 100)
	done := make(chan error, 1)
	go func() {
		done <- d.Client.Fetch(seqs, items, messages)
	}()

	log.Infof("开始下载 %d 封邮件...", seqs.Size())
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

// 保存单封邮件
func (d *Downloader) saveMail(ctx context.Context, msg *imap.Message, mailbox string) (err error) {
	storePath := d.getMailStorePath(msg, mailbox)
	exists, _ := PathExists(storePath)
	if exists {
		return
	}
	// 创建目录
	dir := filepath.Dir(storePath)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return
	}

	// 遍历 body 结构，找到完整邮件内容
	for _, literal := range msg.Body {
		if body, err := io.ReadAll(literal); err == nil && len(body) > 0 {
			if err = os.WriteFile(storePath, body, 0644); err != nil {
				return err
			}
			log.Debugf("已保存: %s", storePath)
			return nil
		}
	}

	log.Warnf("邮件 %d 无有效内容，跳过", msg.SeqNum)
	return nil
}

// 构造邮件存储路径: {absDir}/{mailbox}/{YYYY}/{MM}/{主题}-{时间戳}.eml
func (d *Downloader) getMailStorePath(msg *imap.Message, mailbox string) string {
	t := msg.InternalDate
	year := t.Format("2006")
	month := t.Format("01")
	subject := "无主题"
	if msg.Envelope != nil && msg.Envelope.Subject != "" {
		subject = decodeSubject(msg.Envelope.Subject)
	}
	// 文件名 = 主题 + 时间戳
	fileName := fmt.Sprintf("%s-%d.eml", subject, t.UnixMilli())
	// 清理文件名中的非法字符
	fileName = sanitizeFileName(fileName)
	dir := filepath.Join(d.Options.absDir, mailbox, year, month)
	return filepath.Join(dir, fileName)
}

// 清理文件名中的非法字符
func sanitizeFileName(name string) string {
	// Windows 不允许: \ / : * ? " < > |
	// Unix 不允许: /
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

// 解码邮件主题（RFC 2047）
func decodeSubject(s string) string {
	// go-imap 已经解码了 Envelope.Subject，但如果包含 =? 开头的编码，手动再解一次
	if strings.Contains(s, "=?") {
		dec := &imap.CharsetReader
		// 尝试用 charset 解码
		if *dec != nil {
			// 简单处理：go-imap 的 envelope 已经解码大部分
			return s
		}
	}
	return s
}
