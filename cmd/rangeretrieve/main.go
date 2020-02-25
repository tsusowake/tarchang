package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/kelseyhightower/envconfig"
	"github.com/tsusowake/tarchang"
	"github.com/tsusowake/tarchang/s3"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Batch struct {
	downloader s3.IS3Downloader
}

func (d *Batch) Retrieve(bucket, objectKey string) {

	retrieveFiles := []struct {
		Offset int64
		Byte   int64
		Name   string
	}{
		{Offset: 512, Byte: 1893411, Name: "cat-1.jpg"},
		{Offset: 1894912, Byte: 150460, Name: "cat-2.jpg"},
		{Offset: 2045952, Byte: 332314, Name: "cat-3.jpg"},
		{Offset: 2379264, Byte: 1705696, Name: "lion-1.jpg"},
	}

	for _, ff := range retrieveFiles {
		buf, err := d.downloader.DownloadWithRange(bucket, objectKey, &s3.Range{
			Offset: ff.Offset,
			Byte:   ff.Byte,
		})
		if err != nil {
			panic(err)
		}

		downloadPath := filepath.Join("./tmp", ff.Name)
		downloadDir := filepath.Dir(downloadPath)
		if err := os.MkdirAll(downloadDir, 0777); err != nil {
			panic(err)
		}

		if err := ioutil.WriteFile(downloadPath, buf.Bytes(), 0644); err != nil {
			panic(err)
		}

		println(fmt.Sprintf("hash: %s", tarchang.BufToSha256String(buf.Bytes())))
	}
}

func main() {
	load()
	conf := GetConfig()

	awsConfig := aws.NewConfig().
		WithRegion("ap-northeast-1").
		WithMaxRetries(5)
	sess := session.Must(session.NewSession(awsConfig))

	b := &Batch{downloader: s3.NewS3Downloader(sess)}
	b.Retrieve(conf.Bucket, conf.ObjectKey)
}

type Config struct {
	Bucket    string `envconfig:"bucket" required:"true"`
	ObjectKey string `envconfig:"object_key" required:"true"`
}

var conf *Config

func load() {
	if conf != nil {
		return
	}
	c := &Config{}
	envconfig.MustProcess("", c)

	conf = c
}

// GetConfig コピーを返す
func GetConfig() Config {
	return *conf
}
