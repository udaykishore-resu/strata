// Package run provides Cloud Run constructs.
package run

import (
	"fmt"
	"time"

	"github.com/udaykishore-resu/strata/pkg/cdk"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/iam"
	"github.com/udaykishore-resu/strata/pkg/cdk/gcp/secretmanager"
)

// ServiceProps configures a Cloud Run service.
type ServiceProps struct {
	Image any // required: image reference, or a token such as a parameter

	Name   string // generated when empty
	Region string // defaults to the engine region
	Port   int

	Env     map[string]any                   // values may be tokens
	Secrets map[string]*secretmanager.Secret // env var -> secret (latest version)

	// ServiceAccount runs the service. When nil a dedicated, permissionless
	// service account is created (least privilege by default).
	ServiceAccount *iam.ServiceAccount

	CPU          string
	Memory       string
	MinInstances int
	MaxInstances int
	Concurrency  int
	Timeout      time.Duration
	Ingress      string // INGRESS_TRAFFIC_ALL | INGRESS_TRAFFIC_INTERNAL_ONLY | INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER

	// Public allows unauthenticated invocations (roles/run.invoker to allUsers).
	Public bool
	Labels map[string]any
}

// Service is a Cloud Run service.
type Service struct {
	*cdk.Resource
	sa     *iam.ServiceAccount
	region string
}

// NewService creates a Cloud Run service.
func NewService(scope cdk.Construct, id string, props *ServiceProps) *Service {
	if props == nil || props.Image == nil || props.Image == "" {
		panic("run: ServiceProps.Image is required")
	}
	p := map[string]any{"image": props.Image}
	set := func(k string, v any, ok bool) {
		if ok {
			p[k] = v
		}
	}
	set("name", props.Name, props.Name != "")
	set("region", props.Region, props.Region != "")
	set("port", props.Port, props.Port > 0)
	set("cpu", props.CPU, props.CPU != "")
	set("memory", props.Memory, props.Memory != "")
	set("minInstances", props.MinInstances, props.MinInstances > 0)
	set("maxInstances", props.MaxInstances, props.MaxInstances > 0)
	set("concurrency", props.Concurrency, props.Concurrency > 0)
	set("timeoutSeconds", int(props.Timeout.Seconds()), props.Timeout > 0)
	set("ingress", props.Ingress, props.Ingress != "")
	set("labels", props.Labels, len(props.Labels) > 0)
	set("env", props.Env, len(props.Env) > 0)

	r := cdk.NewResource(scope, id, "gcp:run:Service", p)
	r.AddDependency(cdk.StackOf(scope).RequireService("run.googleapis.com"))
	s := &Service{Resource: r, region: props.Region}

	s.sa = props.ServiceAccount
	if s.sa == nil {
		s.sa = iam.NewServiceAccount(r, "ServiceAccount", &iam.ServiceAccountProps{
			DisplayName: fmt.Sprintf("Cloud Run service %s", r.Node().Path()),
		})
	}
	r.Set("serviceAccount", s.sa.Email())

	if len(props.Secrets) > 0 {
		secretEnv := map[string]any{}
		for env, sec := range props.Secrets {
			secretEnv[env] = sec.Ref()
			// The revision cannot start until the runtime identity can read the secret.
			r.AddDependency(sec.GrantAccess(s.sa))
		}
		r.Set("secretEnv", secretEnv)
	}
	if props.Public {
		s.GrantInvoke(cdk.AllUsers)
	}
	return s
}

// URL returns a token for the service's HTTPS URL.
func (s *Service) URL() any { return s.Attr("uri") }

// ServiceName returns a token for the service ID.
func (s *Service) ServiceName() any { return s.Ref() }

// ServiceAccount returns the runtime identity (grant it access to things).
func (s *Service) ServiceAccount() *iam.ServiceAccount { return s.sa }

// GrantInvoke allows g to call the service.
func (s *Service) GrantInvoke(g cdk.Grantee) *cdk.Resource {
	p := map[string]any{"service": s.Ref(), "role": "roles/run.invoker", "member": g.GrantMember()}
	if s.region != "" {
		p["region"] = s.region
	}
	return cdk.NewResource(s, cdk.GrantID(s, "roles/run.invoker"), "gcp:run:ServiceIamMember", p)
}
