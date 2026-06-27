package main

import (
	"context"
	"os"
	"sync"

	"github.com/emersion/go-imap/client"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

func init() {
	log.SetOutput(os.Stdout)
	file, err := os.OpenFile("logrus.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		log.SetOutput(file)
	} else {
		log.Info("Failed to log to file, using default stderr")
	}
	log.SetLevel(log.InfoLevel)
}

func main() {
	opts := &Options{}
	buf, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("读取配置文件出错:%s\n", err.Error())
	}
	err = yaml.Unmarshal(buf, opts)
	if err != nil {
		log.Fatalf("转换配置文件出错:%s\n", err.Error())
	}
	opts.setAbsDir()
	opts.print()
	ctx := context.Background()
	if err = DownloadByAccount(ctx, opts); err != nil {
		log.Printf("下载报错：%s\n", err.Error())
	}
}

// DownloadByAccount 按邮箱账户进行下载（并行加速版）
func DownloadByAccount(ctx context.Context, opts *Options) (err error) {
	// 主连接只用来列举文件夹
	d, err := NewDownloader(opts)
	if err != nil {
		return
	}
	defer func(Client *client.Client) {
		_ = Client.Logout()
	}(d.Client)

	mailboxes, err := d.getPrefixMatchedMailBoxes(ctx)
	if err != nil {
		return
	}

	if len(mailboxes) == 0 {
		log.Warn("没有匹配的邮箱文件夹")
		return nil
	}

	numWorkers := opts.Parallel
	if numWorkers < 1 {
		numWorkers = 1
	}
	if numWorkers > len(mailboxes) {
		numWorkers = len(mailboxes)
	}

	log.Infof("===== 并行下载 %d 个文件夹，同时 %d 个连接 =====", len(mailboxes), numWorkers)

	type job struct {
		mailbox string
	}
	jobs := make(chan job, len(mailboxes))
	for _, mb := range mailboxes {
		jobs <- job{mailbox: mb}
	}
	close(jobs)

	var wg sync.WaitGroup
	errCh := make(chan error, len(mailboxes))

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for j := range jobs {
				log.Infof("[Worker %d] 开始下载文件夹: %s", workerId, j.mailbox)

				// 每个文件夹创建独立连接（IMAP 协议要求一个连接只能 SELECT 一个文件夹）
				subD, subErr := NewDownloader(opts)
				if subErr != nil {
					log.Errorf("[Worker %d] 创建连接失败: %s", workerId, subErr)
					errCh <- subErr
					continue
				}
				func() {
					defer func(Client *client.Client) {
						_ = Client.Logout()
					}(subD.Client)

					if subErr = subD.downloadAccountMailbox(ctx, j.mailbox); subErr != nil {
						log.Errorf("[Worker %d] 下载文件夹 %s 失败: %s", workerId, j.mailbox, subErr)
						errCh <- subErr
					}
				}()
				log.Infof("[Worker %d] 完成文件夹: %s", workerId, j.mailbox)
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	for e := range errCh {
		if err == nil {
			err = e
		}
	}

	return
}
