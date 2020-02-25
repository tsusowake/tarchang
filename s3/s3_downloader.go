package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	perr "github.com/pkg/errors"
)

type IS3Downloader interface {
	Download(bucket, key string) (*aws.WriteAtBuffer, error)
	DownloadWithRange(bucket, key string, rangeByte *Range) (*aws.WriteAtBuffer, error)
}

type Range struct {
	Offset int64
	Byte   int64
}

type s3Downloader struct {
	downloader *s3manager.Downloader
}

func NewS3Downloader(sess *session.Session) IS3Downloader {
	return &s3Downloader{
		downloader: s3manager.NewDownloader(sess),
	}
}

func (s *s3Downloader) Download(bucket, key string) (*aws.WriteAtBuffer, error) {
	s3Input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := s.downloader.DownloadWithContext(context.Background(), buf, s3Input)
	if err != nil {
		return nil, perr.WithStack(err)
	}
	return buf, nil
}

func (s *s3Downloader) DownloadWithRange(bucket, key string, rangeByte *Range) (*aws.WriteAtBuffer, error) {
	contentRange := fmt.Sprintf("bytes=%d-%d",
		rangeByte.Offset,
		rangeByte.Offset+rangeByte.Byte-1,
	)

	s3Input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(contentRange),
	}

	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := s.downloader.DownloadWithContext(context.Background(), buf, s3Input)
	if err != nil {
		return nil, perr.WithStack(err)
	}
	return buf, nil
}
