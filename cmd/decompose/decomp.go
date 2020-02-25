package main

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/tsusowake/tarchang/s3"
)

type Decomposer struct {
	Downloader s3.IS3Downloader
}

func NewDecomposer(downloader s3.IS3Downloader) *Decomposer {
	return &Decomposer{
		Downloader: downloader,
	}
}

func (d *Decomposer) Decompose(bucket, objectKey string) {

	buf, err := d.Downloader.DownloadAtBufferWithContext(context.Background(), bucket, objectKey)
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

		downloadPath := filepath.Join("./tmp", bucket, header.Name)
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
