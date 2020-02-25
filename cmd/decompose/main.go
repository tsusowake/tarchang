package main

import (
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

	NewDecomposer(
		s3.NewS3Downloader(sess),
	).Decompose(conf.Bucket, conf.ObjectKey)
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
