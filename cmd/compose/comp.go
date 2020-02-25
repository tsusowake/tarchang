package main

import (
	"archive/tar"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/tsusowake/tarchang/s3"
)

type Composer struct {
	Uploader s3.IS3Uploader
}

func NewComposer(uploader s3.IS3Uploader) *Composer {
	return &Composer{
		Uploader: uploader,
	}
}

func (c *Composer) Compose(filePaths []string, bucket, objectKey string) {

	tarFilePath := c.makeTar(filePaths, bucket, objectKey)
	bbb, err := ioutil.ReadFile(tarFilePath)
	out, err := c.Uploader.UploadBufferWithContext(context.Background(), aws.NewWriteAtBuffer(bbb), bucket, objectKey)
	if err != nil {
		panic(err)
	}

	fmt.Println("out: ", out)
	return
}

func (c *Composer)makeTar(filePaths []string, bucket, objectKey string) string {
	tarFilePath := filepath.Join("./tmp", bucket, objectKey)
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
	for _, fp := range filePaths {

		fmt.Println("filepath: ", fp)

		bb, err := ioutil.ReadFile(fp)
		if err != nil {
			panic(err)
		}

		hdr := &tar.Header{
			Name: fp,
			Size: int64(len(bb)),
			Mode: 0600,
		}

		if err := tw.WriteHeader(hdr); err != nil {
			panic(err)
		}

		if _, err := tw.Write(bb); err != nil {
			panic(err)
		}
	}

	return tarFilePath
}