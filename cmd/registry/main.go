package main

import (
	_ "net/http/pprof"

	"github.com/docker/distribution/registry"
	_ "github.com/docker/distribution/registry/auth/htpasswd"
	_ "github.com/docker/distribution/registry/auth/silly"
	_ "github.com/docker/distribution/registry/auth/token"
	_ "github.com/docker/distribution/registry/proxy"
	_ "github.com/docker/distribution/registry/storage/driver/azure"
	_ "github.com/docker/distribution/registry/storage/driver/filesystem"
	_ "github.com/docker/distribution/registry/storage/driver/gcs"
	_ "github.com/docker/distribution/registry/storage/driver/inmemory"
	_ "github.com/docker/distribution/registry/storage/driver/middleware/cloudfront"
	_ "github.com/docker/distribution/registry/storage/driver/oss"
	_ "github.com/docker/distribution/registry/storage/driver/s3-aws"
	_ "github.com/docker/distribution/registry/storage/driver/s3-goamz"
	_ "github.com/docker/distribution/registry/storage/driver/swift"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/basictracer-go/examples/dapperish"
	"github.com/opentracing/opentracing-go"
)

func main() {
	tracerImpl := basictracer.New(dapperish.NewTrivialRecorder("registry")) // do a better name
	opentracing.InitGlobalTracer(tracerImpl)
	registry.RootCmd.Execute()
}
