package main

import (
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

func init() {
	log.SetLevel(log.InfoLevel)
	log.SetOutput(io.Discard)

	file, err := os.OpenFile("logrus.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.SetOutput(os.Stdout)
		log.Info("日志文件打开失败，仅输出到控制台")
		return
	}

	mw := io.MultiWriter(os.Stdout, file)
	log.SetOutput(mw)
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
	// 设置默认值
	if opts.Parallel < 1 {
		opts.Parallel = 4
	}
	if opts.Threads < 1 {
		opts.Threads = opts.Parallel // 默认等于 parallel
	}
	opts.setAbsDir()
	opts.print()
	ctx := context.Background()
	if err = DownloadByAccount(ctx, opts); err != nil {
		log.Printf("下载报错：%s\n", err.Error())
	}
}

// DownloadByAccount 并行下载
func DownloadByAccount(ctx context.Context, opts *Options) (err error) {
	d, err := NewDownloader(opts)
	if err != nil {
		return
	}
	defer func() {
		if d.DB != nil {
			d.DB.Close()
		}
		_ = d.Client.Logout()
	}()

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
	mu := &sync.Mutex{}
	totalSkipped := uint64(0)
	totalDownloaded := uint64(0)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for j := range jobs {
				log.Infof("[Worker %d] 开始下载文件夹: %s", workerId, j.mailbox)

				subD, subErr := NewDownloader(opts)
				if subErr != nil {
					log.Errorf("[Worker %d] 创建连接失败: %s", workerId, subErr)
					errCh <- subErr
					continue
				}
				func() {
					defer func() {
						if subD.DB != nil {
							subD.DB.Close()
						}
						_ = subD.Client.Logout()
					}()

					if subErr = subD.downloadAccountMailbox(ctx, j.mailbox); subErr != nil {
						log.Errorf("[Worker %d] 下载文件夹 %s 失败: %s", workerId, j.mailbox, subErr)
						errCh <- subErr
					}

					skipped := atomic.LoadUint64(&subD.Skipped)
					downloaded := atomic.LoadUint64(&subD.Downloaded)
					mu.Lock()
					totalSkipped += skipped
					totalDownloaded += downloaded
					mu.Unlock()

					log.Infof("[Worker %d] %s 统计: 跳过 %d 封，下载 %d 封",
						workerId, j.mailbox, skipped, downloaded)
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

	log.Infof("===== 全部完成：总跳过 %d 封，总下载 %d 封 =====", totalSkipped, totalDownloaded)
	return
}
