package main

import (
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

type Options struct {
	Dir      string   `yaml:"dir"`
	Host     string   `yaml:"host"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	Prefixes []string `yaml:"prefixes"`
	Parallel int      `yaml:"parallel"` // 并行下载文件夹数
	Threads  int      `yaml:"threads"`  // 单个文件夹内下载线程数
	absDir   string   // 绝对路径（内部使用）
}

func (o *Options) print() {
	log.Infof("====== 配置信息 ======")
	log.Infof("用户名：%s", o.Username)
	log.Infof("服务器地址：%s", o.Host)
	log.Infof("并行文件夹数：%d", o.Parallel)
	log.Infof("文件夹内线程数：%d", o.Threads)
	for _, prefix := range o.Prefixes {
		log.Infof("邮箱文件夹：%s", prefix)
	}
	log.Infof("存储路径：%s", o.absDir)
	log.Infof("======================")
}

func (o *Options) setAbsDir() {
	o.absDir = filepath.Join(GetCurrentDirectory(), o.Dir)
}

func GetCurrentDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	return dir
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
