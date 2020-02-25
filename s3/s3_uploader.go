package s3

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	perr "github.com/pkg/errors"
)

type IS3Uploader interface {
	UploadBufferWithContext(ctx context.Context, buf *aws.WriteAtBuffer, bucket, key string) (*s3manager.UploadOutput, error)
}

type s3Uploader struct {
	Uploader *s3manager.Uploader
}

func NewS3Uploader(sess *session.Session) IS3Uploader {
	return &s3Uploader{
		Uploader:       s3manager.NewUploader(sess),
	}
}

func (s *s3Uploader) UploadBufferWithContext(ctx context.Context, buf *aws.WriteAtBuffer, bucket, key string) (*s3manager.UploadOutput, error) {
	out, err := s.Uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Body:         bytes.NewReader(buf.Bytes()),
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		StorageClass: aws.String(s3.ObjectStorageClassStandard),
	})
	if err != nil {
		return nil, perr.WithStack(err)
	}
	return out, nil
}
