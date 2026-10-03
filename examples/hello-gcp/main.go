// Command hello-gcp is the smallest useful Strata stack: a public Cloud Run
// "hello" service that gets its own least-privilege service account and
// read access to a private bucket. It deploys in about a minute and costs
// nothing while idle, which makes it the demo of choice for real GCP.
//
//	go run ./examples/hello-gcp -out strata.out
//	./scripts/demo-gcp.sh            # full guided demo against your project
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/udaykishore-resu/strata/pkg/cdk"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/run"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/storage"
	"github.com/udaykishore-resu/strata/pkg/template"
)

// Build defines the stack; tests synthesize it without writing files.
func Build() *cdk.App {
	app := cdk.NewApp()
	stack := cdk.NewStack(app, "hello", &cdk.StackProps{
		Description: "Minimal Strata demo: a public Cloud Run service with a private bucket",
	})

	// Parameter so the demo can deploy a broken image and watch the rollback.
	image := stack.AddParameter("Image", template.Parameter{
		Type:        "string",
		Default:     "us-docker.pkg.dev/cloudrun/container/hello",
		Description: "Container image for the web service.",
	})

	assets := storage.NewBucket(stack, "Assets", &storage.BucketProps{
		Versioned:    true, // flipped by hand in the drift demo
		ForceDestroy: true, // `strata destroy` empties the bucket first
		Labels:       map[string]any{"app": "hello"},
	})

	web := run.NewService(stack, "Web", &run.ServiceProps{
		Image:        image,
		Env:          map[string]any{"ASSETS_BUCKET": assets.BucketName()},
		MaxInstances: 2,
		Public:       true,
		Labels:       map[string]any{"app": "hello"},
	})

	// Intent, not IAM plumbing: the service's own identity may read the bucket.
	assets.GrantRead(web.ServiceAccount())

	stack.AddOutput("Url", web.URL(), "Public URL of the hello service")
	stack.AddOutput("Bucket", assets.BucketName(), "Private assets bucket")
	return app
}

func main() {
	out := flag.String("out", "strata.out", "output directory")
	flag.Parse()
	paths, err := Build().Synth(*out)
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range paths {
		fmt.Println("synthesized", p)
	}
}
