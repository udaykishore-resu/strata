// Package storage provides Cloud Storage constructs.
package storage

import "github.com/udaykishore-resu/strata/pkg/cdk"

// BucketProps configures a bucket. The zero value is a private, uniform
// access bucket in the US multi-region with a generated name.
type BucketProps struct {
	Name                     string // generated when empty (recommended: allows replacement)
	Location                 string
	StorageClass             string
	Versioned                bool
	LifecycleDeleteAfterDays int
	ForceDestroy             bool // delete objects when the bucket is deleted
	AllowPublicAccess        bool // sets publicAccessPrevention=inherited
	Labels                   map[string]any
	Retain                   bool // keep the bucket when removed from the stack
}

// Bucket is a Cloud Storage bucket.
type Bucket struct {
	*cdk.Resource
}

// NewBucket creates a bucket.
func NewBucket(scope cdk.Construct, id string, props *BucketProps) *Bucket {
	if props == nil {
		props = &BucketProps{}
	}
	p := map[string]any{"versioning": props.Versioned}
	if props.Name != "" {
		p["name"] = props.Name
	}
	if props.Location != "" {
		p["location"] = props.Location
	}
	if props.StorageClass != "" {
		p["storageClass"] = props.StorageClass
	}
	if props.LifecycleDeleteAfterDays > 0 {
		p["lifecycleDeleteAfterDays"] = props.LifecycleDeleteAfterDays
	}
	if props.ForceDestroy {
		p["forceDestroy"] = true
	}
	if props.AllowPublicAccess {
		p["publicAccessPrevention"] = "inherited"
	}
	if len(props.Labels) > 0 {
		p["labels"] = props.Labels
	}
	r := cdk.NewResource(scope, id, "gcp:storage:Bucket", p)
	r.AddDependency(cdk.StackOf(scope).RequireService("storage.googleapis.com"))
	if props.Retain {
		r.Retain()
	}
	return &Bucket{Resource: r}
}

// BucketName returns a token for the bucket name.
func (b *Bucket) BucketName() any { return b.Ref() }

// URL returns a token for the gs:// URL.
func (b *Bucket) URL() any { return b.Attr("url") }

// Grant binds role on the bucket to g.
func (b *Bucket) Grant(g cdk.Grantee, role string) *cdk.Resource {
	return cdk.NewResource(b, cdk.GrantID(b, role), "gcp:storage:BucketIamMember", map[string]any{
		"bucket": b.Ref(), "role": role, "member": g.GrantMember(),
	})
}

// GrantRead allows reading objects.
func (b *Bucket) GrantRead(g cdk.Grantee) *cdk.Resource {
	return b.Grant(g, "roles/storage.objectViewer")
}

// GrantWrite allows creating objects.
func (b *Bucket) GrantWrite(g cdk.Grantee) *cdk.Resource {
	return b.Grant(g, "roles/storage.objectCreator")
}

// GrantReadWrite allows full object access.
func (b *Bucket) GrantReadWrite(g cdk.Grantee) *cdk.Resource {
	return b.Grant(g, "roles/storage.objectAdmin")
}
