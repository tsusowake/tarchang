package main

import (
	"archive/tar"
	"context"
	"fmt"
	"github.com/tsusowake/tarchang"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/kelseyhightower/envconfig"
	"github.com/tsusowake/tarchang/s3"
)

type Batch struct {
	Uploader s3.IS3Uploader
}

func (b *Batch) Compose(filePaths []string, bucket, objectKey string) {

	tarFilePath := b.makeTar(filePaths, objectKey)
	bbb, err := ioutil.ReadFile(tarFilePath)
	out, err := b.Uploader.UploadBufferWithContext(context.Background(), aws.NewWriteAtBuffer(bbb), bucket, objectKey)
	if err != nil {
		panic(err)
	}

	fmt.Println("out: ", out)
}

func (b *Batch) makeTar(filePaths []string, objectKey string) string {
	tarFilePath := filepath.Join("./tmp", objectKey)
	tarFileDir := filepath.Dir(tarFilePath)
	if err := os.MkdirAll(tarFileDir, 0777); err != nil {
		panic(err)
	}

	f, err := os.Create(tarFilePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	tw := tar.NewWriter(f)
	offset := int64(0)
	for _, fp := range filePaths {

		bb, err := ioutil.ReadFile(fp)
		if err != nil {
			panic(err)
		}

		hdr := &tar.Header{
			Name: filepath.Base(fp),
			Size: int64(len(bb)),
			Mode: 0600,
		}

		if err := tw.WriteHeader(hdr); err != nil {
			panic(err)
		}
		offset += 512 // tar header

		writtenBytes, err := tw.Write(bb)
		if err != nil {
			panic(err)
		}

		println(fmt.Sprintf("file name: %s, offset: %d - byte: %d", hdr.Name, offset, int64(len(bb))))
		println(fmt.Sprintf("hash: %s", tarchang.BufToSha256String(bb)))

		writtenBlocks := writtenBytes / 512
		if writtenBytes%512 > 0 {
			writtenBlocks += 1
		}
		offset += int64(writtenBlocks * 512)
	}

	if err := tw.Close(); err != nil {
		panic(err)
	}

	return tarFilePath
}

func main() {
	load()
	conf := GetConfig()

	awsConfig := aws.NewConfig().
		WithRegion("ap-northeast-1").
		WithMaxRetries(5)
	sess := session.Must(session.NewSession(awsConfig))

	files := []string{
		"../../img/cat-1.jpg",
		"../../img/cat-2.jpg",
		"../../img/cat-3.jpg",
		"../../img/lion-1.jpg",
	}

	b := &Batch{Uploader: s3.NewS3Uploader(sess)}
	b.Compose(files, conf.UploadBucket, conf.UploadObjectKey)
}

type Config struct {
	UploadBucket    string `envconfig:"upload_bucket" required:"true"`
	UploadObjectKey string `envconfig:"upload_object_key" required:"true"`
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
