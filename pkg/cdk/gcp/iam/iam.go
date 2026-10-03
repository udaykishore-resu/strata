// Package iam provides IAM constructs.
package iam

import "github.com/udaykishore-resu/strata/pkg/cdk"

// ServiceAccountProps configures a service account.
type ServiceAccountProps struct {
	AccountID   string // generated when empty
	DisplayName string
	Description string
}

// ServiceAccount is an IAM service account. It is a Grantee.
type ServiceAccount struct {
	*cdk.Resource
}

// NewServiceAccount creates a service account.
func NewServiceAccount(scope cdk.Construct, id string, props *ServiceAccountProps) *ServiceAccount {
	if props == nil {
		props = &ServiceAccountProps{}
	}
	p := map[string]any{}
	if props.AccountID != "" {
		p["accountId"] = props.AccountID
	}
	if props.DisplayName != "" {
		p["displayName"] = props.DisplayName
	}
	if props.Description != "" {
		p["description"] = props.Description
	}
	r := cdk.NewResource(scope, id, "gcp:iam:ServiceAccount", p)
	r.AddDependency(cdk.StackOf(scope).RequireService("iam.googleapis.com"))
	return &ServiceAccount{Resource: r}
}

// Email returns a token for the account email.
func (s *ServiceAccount) Email() any { return s.Attr("email") }

// Member returns a token for "serviceAccount:EMAIL".
func (s *ServiceAccount) Member() any { return s.Attr("member") }

// GrantMember implements cdk.Grantee.
func (s *ServiceAccount) GrantMember() any { return s.Member() }

// GrantProjectRole binds role on the project to g.
func GrantProjectRole(scope cdk.Construct, id string, g cdk.Grantee, role string) *cdk.Resource {
	return cdk.NewResource(scope, id, "gcp:projects:IamMember", map[string]any{
		"role": role, "member": g.GrantMember(), "project": cdk.ProjectID,
	})
}
