// Package secretmanager provides Secret Manager constructs.
package secretmanager

import "github.com/udaykishore-resu/strata/pkg/cdk"

// SecretProps configures a secret container. Secret values are never part
// of a template: add versions with `gcloud secrets versions add`.
type SecretProps struct {
	SecretID  string // generated when empty
	Locations []string
	Labels    map[string]any
}

// Secret is a Secret Manager secret.
type Secret struct {
	*cdk.Resource
}

// NewSecret creates a secret.
func NewSecret(scope cdk.Construct, id string, props *SecretProps) *Secret {
	if props == nil {
		props = &SecretProps{}
	}
	p := map[string]any{}
	if props.SecretID != "" {
		p["secretId"] = props.SecretID
	}
	if len(props.Locations) > 0 {
		locs := make([]any, len(props.Locations))
		for i, l := range props.Locations {
			locs[i] = l
		}
		p["replicationLocations"] = locs
	}
	if len(props.Labels) > 0 {
		p["labels"] = props.Labels
	}
	r := cdk.NewResource(scope, id, "gcp:secretmanager:Secret", p)
	r.AddDependency(cdk.StackOf(scope).RequireService("secretmanager.googleapis.com"))
	return &Secret{Resource: r}
}

// SecretID returns a token for the secret ID.
func (s *Secret) SecretID() any { return s.Ref() }

// ResourceName returns a token for projects/P/secrets/ID.
func (s *Secret) ResourceName() any { return s.Attr("id") }

// GrantAccess allows g to read secret versions.
func (s *Secret) GrantAccess(g cdk.Grantee) *cdk.Resource {
	return cdk.NewResource(s, cdk.GrantID(s, "roles/secretmanager.secretAccessor"), "gcp:secretmanager:SecretIamMember", map[string]any{
		"secret": s.Ref(), "role": "roles/secretmanager.secretAccessor", "member": g.GrantMember(),
	})
}
