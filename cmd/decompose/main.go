package main

import (
	"archive/tar"
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/kelseyhightower/envconfig"
	"github.com/tsusowake/tarchang/s3"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)


type Batch struct {
	Downloader s3.IS3Downloader
}

func (b *Batch) Decompose(bucket, objectKey string) {

	buf, err := b.Downloader.Download(bucket, objectKey)
	if err != nil {
		panic(err)
	}

	reader := tar.NewReader(bytes.NewReader(buf.Bytes()))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		downloadPath := filepath.Join("./tmp", header.Name)
		downloadDir := filepath.Dir(downloadPath)
		if err := os.MkdirAll(downloadDir, 0777); err != nil {
			panic(err)
		}

		bout := bytes.NewBuffer([]byte{})
		if _, err := io.Copy(bout, reader); err != nil {
			panic(err)
		}

		if err := ioutil.WriteFile(downloadPath, bout.Bytes(), 0644); err != nil {
			panic(err)
		}
	}

	return
}




func main() {
	load()
	conf := GetConfig()

	awsConfig := aws.NewConfig().
		WithRegion("ap-northeast-1").
		WithMaxRetries(5)
	sess := session.Must(session.NewSession(awsConfig))

	b := &Batch{Downloader:s3.NewS3Downloader(sess)}
	b.Decompose(conf.Bucket, conf.ObjectKey)
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
