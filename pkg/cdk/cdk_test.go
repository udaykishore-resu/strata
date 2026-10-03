package cdk_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/udaykishore-resu/strata/pkg/cdk"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/iam"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/pubsub"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/run"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/secretmanager"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/storage"
	"github.com/udaykishore-resu/strata/pkg/template"
)

func TestSynthWiresGrantsAndDependencies(t *testing.T) {
	app := cdk.NewApp()
	st := cdk.NewStack(app, "orders", nil)
	b := storage.NewBucket(st, "Uploads", &storage.BucketProps{Versioned: true})
	sec := secretmanager.NewSecret(st, "DbPassword", nil)
	api := run.NewService(st, "Api", &run.ServiceProps{
		Image: "gcr.io/p/api:1", Env: map[string]any{"BUCKET": b.BucketName()},
		Secrets: map[string]*secretmanager.Secret{"DB_PASSWORD": sec}, Public: true,
	})
	b.GrantReadWrite(api.ServiceAccount())
	b.GrantRead(cdk.Principal("group:auditors@example.com"))
	st.AddOutput("Url", api.URL(), "")

	tmpl, err := st.Template()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(tmpl.Resources))
	for id := range tmpl.Resources {
		ids = append(ids, id)
	}
	byType := map[string]int{}
	for _, r := range tmpl.Resources {
		byType[r.Type]++
	}
	want := map[string]int{
		"gcp:serviceusage:Service": 4, // storage, secretmanager, run, iam
		"gcp:storage:Bucket":       1, "gcp:secretmanager:Secret": 1, "gcp:run:Service": 1,
		"gcp:iam:ServiceAccount": 1, "gcp:storage:BucketIamMember": 2,
		"gcp:secretmanager:SecretIamMember": 1, "gcp:run:ServiceIamMember": 1,
	}
	for typ, n := range want {
		if byType[typ] != n {
			t.Errorf("%s: got %d, want %d (ids %v)", typ, byType[typ], n, ids)
		}
	}

	svc := tmpl.Resources["Api"]
	if !contains(svc.DependsOn, "ApiRun") {
		t.Errorf("service must depend on the run API: %v", svc.DependsOn)
	}
	var grantID string
	for id, r := range tmpl.Resources {
		if r.Type == "gcp:secretmanager:SecretIamMember" {
			grantID = id
		}
	}
	if !contains(svc.DependsOn, grantID) {
		t.Errorf("service must wait for its secret grant %s: %v", grantID, svc.DependsOn)
	}
	if sa, ok := svc.Properties["serviceAccount"].(map[string]any); !ok || sa["getAtt"] == nil {
		t.Errorf("service account not wired: %v", svc.Properties["serviceAccount"])
	}
	if !strings.HasPrefix(grantID, "DbPassword") || len(grantID) <= len("DbPasswordSecretAccessorGrant") {
		t.Errorf("nested logical IDs should be path + hash, got %s", grantID)
	}
}

func TestDuplicateIDsPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	st := cdk.NewStack(cdk.NewApp(), "s", nil)
	pubsub.NewTopic(st, "Events", nil)
	pubsub.NewTopic(st, "Events", nil)
}

func TestLogicalIDsAreStable(t *testing.T) {
	build := func() map[string]bool {
		st := cdk.NewStack(cdk.NewApp(), "s", nil)
		t := pubsub.NewTopic(st, "Events", nil)
		sa := iam.NewServiceAccount(st, "Sa", nil)
		t.GrantPublish(sa)
		t.AddSubscription("Sub", nil).GrantConsume(sa)
		tmpl, _ := st.Template()
		out := map[string]bool{}
		for id := range tmpl.Resources {
			out[id] = true
		}
		return out
	}
	a, b := build(), build()
	for id := range a {
		if !b[id] {
			t.Fatalf("logical id %s not stable across synths", id)
		}
	}
}

func TestValidationHook(t *testing.T) {
	st := cdk.NewStack(cdk.NewApp(), "s", nil)
	storage.NewBucket(st, "Site", nil).GrantRead(cdk.AllUsers)
	st.AddValidation(func(tm *template.Template) error {
		for id, r := range tm.Resources {
			if r.Properties["member"] == "allUsers" {
				return fmt.Errorf("%s is public", id)
			}
		}
		return nil
	})
	if _, err := st.Template(); err == nil || !strings.Contains(err.Error(), "is public") {
		t.Fatalf("expected policy violation, got %v", err)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
