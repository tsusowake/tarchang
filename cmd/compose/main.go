package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/kelseyhightower/envconfig"
	"github.com/tsusowake/tarchang/s3"
)

func main() {
	load()
	conf := GetConfig()

	awsConfig := aws.NewConfig().
		WithRegion("ap-northeast-1").
		WithMaxRetries(5)
	sess := session.Must(session.NewSession(awsConfig))

	files := []string{
		fmt.Sprintf("../decompose/tmp/%s/cat-1.jpg", conf.UploadBucket),
		fmt.Sprintf("../decompose/tmp/%s/cat-2.jpg", conf.UploadBucket),
		fmt.Sprintf("../decompose/tmp/%s/cat-3.jpg", conf.UploadBucket),
		fmt.Sprintf("../decompose/tmp/%s/lion-1.jpg", conf.UploadBucket),
	}

	NewComposer(s3.NewS3Uploader(sess)).Compose(files, conf.UploadBucket, conf.UploadObjectKey)
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
