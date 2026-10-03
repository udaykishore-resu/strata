// Command orders-platform is an example Strata CDK app: an orders API on
// Cloud Run with an uploads bucket, an event topic with a dead-letter
// queue, a worker subscription and a database password secret.
//
//	go run ./examples/orders-platform -out strata.out
//	strata deploy -s orders -f strata.out/orders.template.json -p Env=prod
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/udaykishore-resu/strata/pkg/cdk"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/iam"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/pubsub"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/run"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/secretmanager"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/storage"
	"github.com/udaykishore-resu/strata/pkg/template"
)

// Build defines the app; it is separate from main so tests can synthesize it.
func Build() *cdk.App {
	app := cdk.NewApp()
	stack := cdk.NewStack(app, "orders", &cdk.StackProps{Description: "Orders platform: API, uploads, events"})

	env := stack.AddParameter("Env", template.Parameter{
		Type: "string", Default: "dev", Allowed: []any{"dev", "prod"},
		Description: "Deployment environment label.",
	})
	image := stack.AddParameter("ApiImage", template.Parameter{
		Type: "string", Default: "us-docker.pkg.dev/cloudrun/container/hello",
		Description: "Container image for the orders API.",
	})
	labels := map[string]any{"app": "orders", "env": env}

	uploads := storage.NewBucket(stack, "Uploads", &storage.BucketProps{
		Versioned: true, LifecycleDeleteAfterDays: 90, Labels: labels,
	})

	events := pubsub.NewTopic(stack, "OrderEvents", &pubsub.TopicProps{Labels: labels})
	deadLetters := pubsub.NewTopic(stack, "OrderEventsDlq", &pubsub.TopicProps{
		MessageRetention: 7 * 24 * time.Hour, Labels: labels,
	})
	fulfillment := events.AddSubscription("Fulfillment", &pubsub.SubscriptionProps{
		AckDeadline: 30 * time.Second, DeadLetterTopic: deadLetters, MaxDeliveryAttempts: 5, Labels: labels,
	})

	// The value is added out of band: gcloud secrets versions add <id> --data-file=-
	dbPassword := secretmanager.NewSecret(stack, "DbPassword", &secretmanager.SecretProps{Labels: labels})

	api := run.NewService(stack, "Api", &run.ServiceProps{
		Image:        image,
		MaxInstances: 20,
		Env: map[string]any{
			"ENV":           env,
			"UPLOAD_BUCKET": uploads.BucketName(),
			"EVENTS_TOPIC":  events.TopicName(),
		},
		Labels: labels,
		Public: true,
	})
	// Least privilege, expressed as intent rather than IAM bindings.
	uploads.GrantReadWrite(api.ServiceAccount())
	events.GrantPublish(api.ServiceAccount())
	dbPassword.GrantAccess(api.ServiceAccount())

	worker := iam.NewServiceAccount(stack, "FulfillmentWorker", &iam.ServiceAccountProps{DisplayName: "orders fulfillment worker"})
	fulfillment.GrantConsume(worker)
	uploads.GrantRead(worker)

	stack.AddOutput("ApiUrl", api.URL(), "Public URL of the orders API")
	stack.AddOutput("UploadBucket", uploads.URL(), "Uploads bucket")
	stack.AddOutput("EventsTopic", events.ResourceName(), "Order events topic")
	stack.AddOutput("WorkerServiceAccount", worker.Email(), "Identity for the fulfillment worker")

	// Policy as code: fail synth if anything other than the API is public.
	stack.AddValidation(func(t *template.Template) error {
		for id, r := range t.Resources {
			if r.Properties["member"] == "allUsers" && r.Type != "gcp:run:ServiceIamMember" {
				return fmt.Errorf("resource %s grants public access", id)
			}
		}
		return nil
	})
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
