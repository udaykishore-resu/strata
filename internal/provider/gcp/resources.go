package gcp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/udaykishore-resu/strata/internal/provider"
	"github.com/udaykishore-resu/strata/pkg/template"
)

// Providers returns every GCP resource provider bound to c.
func Providers(c *Client) []provider.Provider {
	ps := []provider.Provider{
		&serviceUsage{c}, &bucket{c}, &serviceAccount{c}, &topic{c},
		&subscription{c}, &runService{c}, &secret{c},
	}
	return append(ps, iamProviders(c)...)
}

// Schemas returns the schemas of all GCP providers.
func Schemas() []provider.Schema {
	var out []provider.Schema
	for _, p := range Providers(nil) {
		out = append(out, p.Schema())
	}
	return out
}

func labelsBody(props map[string]any) map[string]string {
	return provider.StringMap(props, "labels")
}

// labelPatch returns new labels plus nulls for removed keys (PATCH semantics).
func labelPatch(oldProps, newProps map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range provider.StringMap(newProps, "labels") {
		out[k] = v
	}
	for k := range provider.StringMap(oldProps, "labels") {
		if _, ok := out[k]; !ok {
			out[k] = nil
		}
	}
	return out
}

func changed(oldProps, newProps map[string]any, keys ...string) []string {
	var out []string
	for _, k := range keys {
		if !template.Equal(oldProps[k], newProps[k]) {
			out = append(out, k)
		}
	}
	return out
}

func observedLabels(m map[string]string) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ------------------------------------------------------------ serviceusage

type serviceUsage struct{ c *Client }

func (s *serviceUsage) Schema() provider.Schema {
	return provider.Schema{
		Type:        "gcp:serviceusage:Service",
		Description: "Enables a Google API on the project (e.g. run.googleapis.com).",
		Properties: map[string]provider.PropertySpec{
			"service":         {Type: provider.TypeString, Required: true, ForceNew: true, Description: "API service name."},
			"disableOnDelete": {Type: provider.TypeBool, Default: false, Description: "Disable the API when the resource is deleted."},
		},
		Attributes:   map[string]string{"name": "Service name.", "state": "ENABLED or DISABLED."},
		NameProperty: "service",
		Name:         provider.NameConstraints{MaxLen: 255},
	}
}

func (s *serviceUsage) path(env provider.Env, svc string) string {
	return fmt.Sprintf("%s/v1/projects/%s/services/%s", s.c.endpoint("serviceusage"), env.Project, svc)
}

func (s *serviceUsage) Create(ctx context.Context, req provider.Request) (provider.Result, error) {
	svc := provider.String(req.Properties, "service")
	var op operation
	if err := s.c.Do(ctx, http.MethodPost, s.path(req.Env, svc)+":enable", map[string]any{}, &op); err != nil {
		return provider.Result{}, err
	}
	if _, err := s.c.WaitOperation(ctx, "serviceusage", "/v1/", op); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: svc, Attributes: map[string]any{"name": svc, "state": "ENABLED"}}, nil
}

func (s *serviceUsage) Read(ctx context.Context, req provider.Request) (provider.Result, error) {
	var out struct {
		State string `json:"state"`
	}
	if err := s.c.Do(ctx, http.MethodGet, s.path(req.Env, req.PhysicalID), nil, &out); err != nil {
		return provider.Result{}, err
	}
	if out.State != "ENABLED" {
		return provider.Result{}, fmt.Errorf("service %s is %s: %w", req.PhysicalID, out.State, provider.ErrNotFound)
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: map[string]any{"name": req.PhysicalID, "state": out.State},
		Observed: map[string]any{"service": req.PhysicalID}}, nil
}

func (s *serviceUsage) Update(ctx context.Context, req provider.Request) (provider.Result, error) {
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: map[string]any{"name": req.PhysicalID, "state": "ENABLED"}}, nil
}

func (s *serviceUsage) Delete(ctx context.Context, req provider.Request) error {
	if !provider.Bool(req.Properties, "disableOnDelete") {
		return nil // APIs are shared project-wide; leave them on by default
	}
	var op operation
	if err := s.c.Do(ctx, http.MethodPost, s.path(req.Env, req.PhysicalID)+":disable", map[string]any{"disableDependentServices": false}, &op); err != nil {
		return err
	}
	_, err := s.c.WaitOperation(ctx, "serviceusage", "/v1/", op)
	return err
}

// ------------------------------------------------------------------ bucket

type bucket struct{ c *Client }

func (b *bucket) Schema() provider.Schema {
	return provider.Schema{
		Type:        "gcp:storage:Bucket",
		Description: "A Cloud Storage bucket with secure defaults (uniform access, public access prevention).",
		Properties: map[string]provider.PropertySpec{
			"name":                     {Type: provider.TypeString, ForceNew: true, Description: "Globally unique bucket name; generated when omitted."},
			"location":                 {Type: provider.TypeString, ForceNew: true, Default: "US", Description: "Region, dual-region or multi-region."},
			"storageClass":             {Type: provider.TypeString, Default: "STANDARD"},
			"versioning":               {Type: provider.TypeBool, Default: false},
			"uniformBucketLevelAccess": {Type: provider.TypeBool, Default: true},
			"publicAccessPrevention":   {Type: provider.TypeString, Default: "enforced", Description: "enforced or inherited."},
			"lifecycleDeleteAfterDays": {Type: provider.TypeInt, Description: "Delete objects older than N days."},
			"forceDestroy":             {Type: provider.TypeBool, Default: false, Description: "Delete all objects when the bucket is deleted."},
			"labels":                   {Type: provider.TypeMap},
		},
		Attributes:   map[string]string{"name": "Bucket name.", "url": "gs:// URL.", "selfLink": "API URL."},
		NameProperty: "name",
		Name:         provider.NameConstraints{MinLen: 3, MaxLen: 63},
		Labels:       true,
	}
}

func (b *bucket) url(name string) string {
	return b.c.endpoint("storage") + "/storage/v1/b/" + url.PathEscape(name)
}

func (b *bucket) body(props map[string]any) map[string]any {
	body := map[string]any{
		"storageClass": provider.String(props, "storageClass"),
		"versioning":   map[string]any{"enabled": provider.Bool(props, "versioning")},
		"iamConfiguration": map[string]any{
			"uniformBucketLevelAccess": map[string]any{"enabled": provider.Bool(props, "uniformBucketLevelAccess")},
			"publicAccessPrevention":   provider.String(props, "publicAccessPrevention"),
		},
	}
	if days := provider.Int(props, "lifecycleDeleteAfterDays"); days > 0 {
		body["lifecycle"] = map[string]any{"rule": []any{map[string]any{
			"action": map[string]any{"type": "Delete"}, "condition": map[string]any{"age": days},
		}}}
	}
	return body
}

type bucketResource struct {
	Name             string                 `json:"name"`
	Location         string                 `json:"location"`
	StorageClass     string                 `json:"storageClass"`
	SelfLink         string                 `json:"selfLink"`
	Labels           map[string]string      `json:"labels"`
	Versioning       struct{ Enabled bool } `json:"versioning"`
	IAMConfiguration struct {
		UniformBucketLevelAccess struct{ Enabled bool } `json:"uniformBucketLevelAccess"`
		PublicAccessPrevention   string                 `json:"publicAccessPrevention"`
	} `json:"iamConfiguration"`
	Lifecycle struct {
		Rule []struct {
			Action    struct{ Type string } `json:"action"`
			Condition struct{ Age int }     `json:"condition"`
		} `json:"rule"`
	} `json:"lifecycle"`
}

func (b *bucket) attrs(r bucketResource) map[string]any {
	return map[string]any{"name": r.Name, "url": "gs://" + r.Name, "selfLink": r.SelfLink}
}

func (b *bucket) Create(ctx context.Context, req provider.Request) (provider.Result, error) {
	name := provider.String(req.Properties, "name")
	body := b.body(req.Properties)
	body["name"] = name
	body["location"] = provider.String(req.Properties, "location")
	body["labels"] = labelsBody(req.Properties)
	var out bucketResource
	u := b.c.endpoint("storage") + "/storage/v1/b?project=" + url.QueryEscape(req.Env.Project)
	if err := b.c.Do(ctx, http.MethodPost, u, body, &out); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: name, Attributes: b.attrs(out)}, nil
}

func (b *bucket) Read(ctx context.Context, req provider.Request) (provider.Result, error) {
	var out bucketResource
	if err := b.c.Do(ctx, http.MethodGet, b.url(req.PhysicalID), nil, &out); err != nil {
		return provider.Result{}, err
	}
	loc := out.Location
	if want := provider.String(req.Properties, "location"); strings.EqualFold(want, loc) {
		loc = want // the API upper-cases locations
	}
	days := 0
	for _, r := range out.Lifecycle.Rule {
		if r.Action.Type == "Delete" && r.Condition.Age > 0 {
			days = r.Condition.Age
		}
	}
	observed := map[string]any{
		"location":                 loc,
		"storageClass":             out.StorageClass,
		"versioning":               out.Versioning.Enabled,
		"uniformBucketLevelAccess": out.IAMConfiguration.UniformBucketLevelAccess.Enabled,
		"publicAccessPrevention":   out.IAMConfiguration.PublicAccessPrevention,
		"labels":                   observedLabels(out.Labels),
	}
	if days > 0 || req.Properties["lifecycleDeleteAfterDays"] != nil {
		observed["lifecycleDeleteAfterDays"] = float64(days)
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: b.attrs(out), Observed: observed}, nil
}

func (b *bucket) Update(ctx context.Context, req provider.Request) (provider.Result, error) {
	body := b.body(req.Properties)
	body["labels"] = labelPatch(req.OldProperties, req.Properties)
	if _, ok := body["lifecycle"]; !ok && provider.Int(req.OldProperties, "lifecycleDeleteAfterDays") > 0 {
		body["lifecycle"] = nil // clear the rule
	}
	var out bucketResource
	if err := b.c.Do(ctx, http.MethodPatch, b.url(req.PhysicalID), body, &out); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: b.attrs(out)}, nil
}

func (b *bucket) Delete(ctx context.Context, req provider.Request) error {
	if provider.Bool(req.Properties, "forceDestroy") {
		if err := b.empty(ctx, req.PhysicalID); err != nil {
			return err
		}
	}
	return b.c.Do(ctx, http.MethodDelete, b.url(req.PhysicalID), nil, nil)
}

// empty deletes every object version in the bucket.
func (b *bucket) empty(ctx context.Context, name string) error {
	page := ""
	for {
		u := b.url(name) + "/o?versions=true&maxResults=1000&fields=items(name,generation),nextPageToken"
		if page != "" {
			u += "&pageToken=" + url.QueryEscape(page)
		}
		var out struct {
			Items []struct {
				Name       string `json:"name"`
				Generation string `json:"generation"`
			} `json:"items"`
			NextPageToken string `json:"nextPageToken"`
		}
		if err := b.c.Do(ctx, http.MethodGet, u, nil, &out); err != nil {
			return err
		}
		for _, it := range out.Items {
			du := b.url(name) + "/o/" + url.PathEscape(it.Name) + "?generation=" + url.QueryEscape(it.Generation)
			if err := b.c.Do(ctx, http.MethodDelete, du, nil, nil); err != nil && !isNotFound(err) {
				return err
			}
		}
		if out.NextPageToken == "" {
			return nil
		}
		page = out.NextPageToken
	}
}

func isNotFound(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Code == http.StatusNotFound
}

// --------------------------------------------------------- service account

type serviceAccount struct{ c *Client }

func (s *serviceAccount) Schema() provider.Schema {
	return provider.Schema{
		Type:        "gcp:iam:ServiceAccount",
		Description: "An IAM service account.",
		Properties: map[string]provider.PropertySpec{
			"accountId":   {Type: provider.TypeString, ForceNew: true, Description: "6-30 chars; generated when omitted."},
			"displayName": {Type: provider.TypeString},
			"description": {Type: provider.TypeString},
		},
		Attributes: map[string]string{
			"email": "Service account email.", "uniqueId": "Numeric ID.",
			"name": "Resource name.", "member": "IAM member string (serviceAccount:EMAIL).",
		},
		NameProperty: "accountId",
		Name:         provider.NameConstraints{MinLen: 6, MaxLen: 30},
	}
}

type saResource struct {
	Name        string `json:"name"`
	Email       string `json:"email"`
	UniqueID    string `json:"uniqueId"`
	DisplayName string `json:"displayName"`
	Description string `json:"description"`
}

func saAttrs(r saResource) map[string]any {
	return map[string]any{"email": r.Email, "uniqueId": r.UniqueID, "name": r.Name, "member": "serviceAccount:" + r.Email}
}

func (s *serviceAccount) url(env provider.Env, accountID string) string {
	email := accountID
	if !strings.Contains(email, "@") {
		email = fmt.Sprintf("%s@%s.iam.gserviceaccount.com", accountID, env.Project)
	}
	return fmt.Sprintf("%s/v1/projects/%s/serviceAccounts/%s", s.c.endpoint("iam"), env.Project, url.PathEscape(email))
}

func (s *serviceAccount) Create(ctx context.Context, req provider.Request) (provider.Result, error) {
	id := provider.String(req.Properties, "accountId")
	body := map[string]any{
		"accountId": id,
		"serviceAccount": map[string]any{
			"displayName": provider.String(req.Properties, "displayName"),
			"description": provider.String(req.Properties, "description"),
		},
	}
	var out saResource
	u := fmt.Sprintf("%s/v1/projects/%s/serviceAccounts", s.c.endpoint("iam"), req.Env.Project)
	if err := s.c.Do(ctx, http.MethodPost, u, body, &out); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: id, Attributes: saAttrs(out)}, nil
}

func (s *serviceAccount) Read(ctx context.Context, req provider.Request) (provider.Result, error) {
	var out saResource
	if err := s.c.Do(ctx, http.MethodGet, s.url(req.Env, req.PhysicalID), nil, &out); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: saAttrs(out),
		Observed: map[string]any{"displayName": out.DisplayName, "description": out.Description}}, nil
}

func (s *serviceAccount) Update(ctx context.Context, req provider.Request) (provider.Result, error) {
	body := map[string]any{
		"serviceAccount": map[string]any{
			"displayName": provider.String(req.Properties, "displayName"),
			"description": provider.String(req.Properties, "description"),
		},
		"updateMask": "displayName,description",
	}
	var out saResource
	if err := s.c.Do(ctx, http.MethodPatch, s.url(req.Env, req.PhysicalID), body, &out); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: saAttrs(out)}, nil
}

func (s *serviceAccount) Delete(ctx context.Context, req provider.Request) error {
	return s.c.Do(ctx, http.MethodDelete, s.url(req.Env, req.PhysicalID), nil, nil)
}

// ------------------------------------------------------------------- topic

type topic struct{ c *Client }

func (t *topic) Schema() provider.Schema {
	return provider.Schema{
		Type:        "gcp:pubsub:Topic",
		Description: "A Pub/Sub topic.",
		Properties: map[string]provider.PropertySpec{
			"name":                     {Type: provider.TypeString, ForceNew: true},
			"labels":                   {Type: provider.TypeMap},
			"messageRetentionDuration": {Type: provider.TypeString, Description: `e.g. "86400s"`},
			"kmsKeyName":               {Type: provider.TypeString},
		},
		Attributes:   map[string]string{"name": "Topic ID.", "id": "Full resource name."},
		NameProperty: "name",
		Name:         provider.NameConstraints{MinLen: 3, MaxLen: 255},
		Labels:       true,
	}
}

func (t *topic) url(env provider.Env, name string) string {
	return fmt.Sprintf("%s/v1/projects/%s/topics/%s", t.c.endpoint("pubsub"), env.Project, url.PathEscape(name))
}

func (t *topic) body(props map[string]any) map[string]any {
	body := map[string]any{"labels": labelsBody(props)}
	for _, k := range []string{"messageRetentionDuration", "kmsKeyName"} {
		if v := provider.String(props, k); v != "" {
			body[k] = v
		}
	}
	return body
}

func (t *topic) attrs(env provider.Env, name string) map[string]any {
	return map[string]any{"name": name, "id": fmt.Sprintf("projects/%s/topics/%s", env.Project, name)}
}

func (t *topic) Create(ctx context.Context, req provider.Request) (provider.Result, error) {
	name := provider.String(req.Properties, "name")
	if err := t.c.Do(ctx, http.MethodPut, t.url(req.Env, name), t.body(req.Properties), nil); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: name, Attributes: t.attrs(req.Env, name)}, nil
}

func (t *topic) Read(ctx context.Context, req provider.Request) (provider.Result, error) {
	var out struct {
		Labels                   map[string]string `json:"labels"`
		MessageRetentionDuration string            `json:"messageRetentionDuration"`
		KmsKeyName               string            `json:"kmsKeyName"`
	}
	if err := t.c.Do(ctx, http.MethodGet, t.url(req.Env, req.PhysicalID), nil, &out); err != nil {
		return provider.Result{}, err
	}
	obs := map[string]any{"labels": observedLabels(out.Labels)}
	if out.MessageRetentionDuration != "" || req.Properties["messageRetentionDuration"] != nil {
		obs["messageRetentionDuration"] = out.MessageRetentionDuration
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: t.attrs(req.Env, req.PhysicalID), Observed: obs}, nil
}

func (t *topic) Update(ctx context.Context, req provider.Request) (provider.Result, error) {
	mask := changed(req.OldProperties, req.Properties, "labels", "messageRetentionDuration", "kmsKeyName")
	if len(mask) > 0 {
		body := map[string]any{"topic": t.body(req.Properties), "updateMask": strings.Join(mask, ",")}
		if err := t.c.Do(ctx, http.MethodPatch, t.url(req.Env, req.PhysicalID), body, nil); err != nil {
			return provider.Result{}, err
		}
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: t.attrs(req.Env, req.PhysicalID)}, nil
}

func (t *topic) Delete(ctx context.Context, req provider.Request) error {
	return t.c.Do(ctx, http.MethodDelete, t.url(req.Env, req.PhysicalID), nil, nil)
}

// ------------------------------------------------------------ subscription

type subscription struct{ c *Client }

func (s *subscription) Schema() provider.Schema {
	return provider.Schema{
		Type:        "gcp:pubsub:Subscription",
		Description: "A Pub/Sub subscription (pull, or push when pushEndpoint is set).",
		Properties: map[string]provider.PropertySpec{
			"name":                     {Type: provider.TypeString, ForceNew: true},
			"topic":                    {Type: provider.TypeString, Required: true, ForceNew: true, Description: "Topic ID or full name."},
			"ackDeadlineSeconds":       {Type: provider.TypeInt, Default: 10},
			"messageRetentionDuration": {Type: provider.TypeString},
			"retainAckedMessages":      {Type: provider.TypeBool, Default: false},
			"deadLetterTopic":          {Type: provider.TypeString, Description: "Topic ID or full name for undeliverable messages."},
			"maxDeliveryAttempts":      {Type: provider.TypeInt, Default: 5},
			"pushEndpoint":             {Type: provider.TypeString},
			"filter":                   {Type: provider.TypeString, ForceNew: true},
			"enableMessageOrdering":    {Type: provider.TypeBool, ForceNew: true, Default: false},
			"labels":                   {Type: provider.TypeMap},
		},
		Attributes:   map[string]string{"name": "Subscription ID.", "id": "Full resource name."},
		NameProperty: "name",
		Name:         provider.NameConstraints{MinLen: 3, MaxLen: 255},
		Labels:       true,
	}
}

func (s *subscription) url(env provider.Env, name string) string {
	return fmt.Sprintf("%s/v1/projects/%s/subscriptions/%s", s.c.endpoint("pubsub"), env.Project, url.PathEscape(name))
}

func (s *subscription) body(env provider.Env, props map[string]any, create bool) map[string]any {
	body := map[string]any{
		"ackDeadlineSeconds":  provider.Int(props, "ackDeadlineSeconds"),
		"retainAckedMessages": provider.Bool(props, "retainAckedMessages"),
		"labels":              labelsBody(props),
		"pushConfig":          map[string]any{},
	}
	if v := provider.String(props, "messageRetentionDuration"); v != "" {
		body["messageRetentionDuration"] = v
	}
	if ep := provider.String(props, "pushEndpoint"); ep != "" {
		body["pushConfig"] = map[string]any{"pushEndpoint": ep}
	}
	if dl := provider.String(props, "deadLetterTopic"); dl != "" {
		body["deadLetterPolicy"] = map[string]any{
			"deadLetterTopic":     fullName(dl, "topics", env),
			"maxDeliveryAttempts": provider.Int(props, "maxDeliveryAttempts"),
		}
	}
	if create {
		body["topic"] = fullName(provider.String(props, "topic"), "topics", env)
		body["enableMessageOrdering"] = provider.Bool(props, "enableMessageOrdering")
		if f := provider.String(props, "filter"); f != "" {
			body["filter"] = f
		}
	}
	return body
}

func (s *subscription) attrs(env provider.Env, name string) map[string]any {
	return map[string]any{"name": name, "id": fmt.Sprintf("projects/%s/subscriptions/%s", env.Project, name)}
}

func (s *subscription) Create(ctx context.Context, req provider.Request) (provider.Result, error) {
	name := provider.String(req.Properties, "name")
	if err := s.c.Do(ctx, http.MethodPut, s.url(req.Env, name), s.body(req.Env, req.Properties, true), nil); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: name, Attributes: s.attrs(req.Env, name)}, nil
}

func (s *subscription) Read(ctx context.Context, req provider.Request) (provider.Result, error) {
	var out struct {
		AckDeadlineSeconds  int               `json:"ackDeadlineSeconds"`
		RetainAckedMessages bool              `json:"retainAckedMessages"`
		Labels              map[string]string `json:"labels"`
	}
	if err := s.c.Do(ctx, http.MethodGet, s.url(req.Env, req.PhysicalID), nil, &out); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: s.attrs(req.Env, req.PhysicalID), Observed: map[string]any{
		"ackDeadlineSeconds":  float64(out.AckDeadlineSeconds),
		"retainAckedMessages": out.RetainAckedMessages,
		"labels":              observedLabels(out.Labels),
	}}, nil
}

func (s *subscription) Update(ctx context.Context, req provider.Request) (provider.Result, error) {
	mask := changed(req.OldProperties, req.Properties, "ackDeadlineSeconds", "retainAckedMessages", "labels", "messageRetentionDuration")
	if len(changed(req.OldProperties, req.Properties, "deadLetterTopic", "maxDeliveryAttempts")) > 0 {
		mask = append(mask, "deadLetterPolicy")
	}
	if len(changed(req.OldProperties, req.Properties, "pushEndpoint")) > 0 {
		mask = append(mask, "pushConfig")
	}
	if len(mask) > 0 {
		body := map[string]any{"subscription": s.body(req.Env, req.Properties, false), "updateMask": strings.Join(mask, ",")}
		if err := s.c.Do(ctx, http.MethodPatch, s.url(req.Env, req.PhysicalID), body, nil); err != nil {
			return provider.Result{}, err
		}
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: s.attrs(req.Env, req.PhysicalID)}, nil
}

func (s *subscription) Delete(ctx context.Context, req provider.Request) error {
	return s.c.Do(ctx, http.MethodDelete, s.url(req.Env, req.PhysicalID), nil, nil)
}

// ------------------------------------------------------------- run service

type runService struct{ c *Client }

func (r *runService) Schema() provider.Schema {
	return provider.Schema{
		Type:        "gcp:run:Service",
		Description: "A Cloud Run (v2) service.",
		Properties: map[string]provider.PropertySpec{
			"name":           {Type: provider.TypeString, ForceNew: true},
			"region":         {Type: provider.TypeString, ForceNew: true, Description: "Defaults to the engine region."},
			"image":          {Type: provider.TypeString, Required: true},
			"port":           {Type: provider.TypeInt, Default: 8080},
			"env":            {Type: provider.TypeMap, Description: "Plain environment variables."},
			"secretEnv":      {Type: provider.TypeMap, Description: `ENV_NAME -> "secret" or "secret:version" (default latest).`},
			"serviceAccount": {Type: provider.TypeString, Description: "Runtime service account email."},
			"cpu":            {Type: provider.TypeString, Default: "1"},
			"memory":         {Type: provider.TypeString, Default: "512Mi"},
			"minInstances":   {Type: provider.TypeInt, Default: 0},
			"maxInstances":   {Type: provider.TypeInt, Default: 10},
			"concurrency":    {Type: provider.TypeInt, Default: 80},
			"timeoutSeconds": {Type: provider.TypeInt, Default: 300},
			"ingress":        {Type: provider.TypeString, Default: "INGRESS_TRAFFIC_ALL"},
			"labels":         {Type: provider.TypeMap},
		},
		Attributes: map[string]string{
			"name": "Service ID.", "id": "Full resource name.", "uri": "HTTPS URL.",
			"latestReadyRevision": "Latest ready revision name.",
		},
		NameProperty: "name",
		Name:         provider.NameConstraints{MaxLen: 49},
		Labels:       true,
	}
}

func (r *runService) region(env provider.Env, props map[string]any) string {
	if v := provider.String(props, "region"); v != "" {
		return v
	}
	return env.Region
}

func (r *runService) path(env provider.Env, props map[string]any, name string) string {
	return fmt.Sprintf("projects/%s/locations/%s/services/%s", env.Project, r.region(env, props), name)
}

func (r *runService) body(props map[string]any) map[string]any {
	var envList []any
	plain := provider.StringMap(props, "env")
	secrets := provider.StringMap(props, "secretEnv")
	keys := make([]string, 0, len(plain)+len(secrets))
	for k := range plain {
		keys = append(keys, k)
	}
	for k := range secrets {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if ref, ok := secrets[k]; ok {
			name, version, found := strings.Cut(ref, ":")
			if !found || strings.HasPrefix(ref, "projects/") {
				name, version = ref, "latest"
			}
			envList = append(envList, map[string]any{"name": k, "valueSource": map[string]any{
				"secretKeyRef": map[string]any{"secret": name, "version": version},
			}})
			continue
		}
		envList = append(envList, map[string]any{"name": k, "value": plain[k]})
	}
	container := map[string]any{
		"image": provider.String(props, "image"),
		"ports": []any{map[string]any{"containerPort": provider.Int(props, "port")}},
		"resources": map[string]any{"limits": map[string]any{
			"cpu": provider.String(props, "cpu"), "memory": provider.String(props, "memory"),
		}},
	}
	if len(envList) > 0 {
		container["env"] = envList
	}
	tmpl := map[string]any{
		"containers": []any{container},
		"scaling": map[string]any{
			"minInstanceCount": provider.Int(props, "minInstances"),
			"maxInstanceCount": provider.Int(props, "maxInstances"),
		},
		"maxInstanceRequestConcurrency": provider.Int(props, "concurrency"),
		"timeout":                       fmt.Sprintf("%ds", provider.Int(props, "timeoutSeconds")),
	}
	if sa := provider.String(props, "serviceAccount"); sa != "" {
		tmpl["serviceAccount"] = sa
	}
	return map[string]any{
		"template": tmpl,
		"ingress":  provider.String(props, "ingress"),
		"labels":   labelsBody(props),
	}
}

type runResource struct {
	Name                string            `json:"name"`
	URI                 string            `json:"uri"`
	LatestReadyRevision string            `json:"latestReadyRevision"`
	Ingress             string            `json:"ingress"`
	Labels              map[string]string `json:"labels"`
	Template            struct {
		ServiceAccount string `json:"serviceAccount"`
		Scaling        struct {
			MinInstanceCount int `json:"minInstanceCount"`
			MaxInstanceCount int `json:"maxInstanceCount"`
		} `json:"scaling"`
		Containers []struct {
			Image string `json:"image"`
		} `json:"containers"`
	} `json:"template"`
}

func (r *runService) attrs(rr runResource) map[string]any {
	short := rr.Name[strings.LastIndex(rr.Name, "/")+1:]
	return map[string]any{"name": short, "id": rr.Name, "uri": rr.URI, "latestReadyRevision": rr.LatestReadyRevision}
}

func (r *runService) get(ctx context.Context, path string) (runResource, error) {
	var out runResource
	err := r.c.Do(ctx, http.MethodGet, r.c.endpoint("run")+"/v2/"+path, nil, &out)
	return out, err
}

func (r *runService) Create(ctx context.Context, req provider.Request) (provider.Result, error) {
	name := provider.String(req.Properties, "name")
	parent := fmt.Sprintf("projects/%s/locations/%s", req.Env.Project, r.region(req.Env, req.Properties))
	var op operation
	u := r.c.endpoint("run") + "/v2/" + parent + "/services?serviceId=" + url.QueryEscape(name)
	if err := r.c.Do(ctx, http.MethodPost, u, r.body(req.Properties), &op); err != nil {
		return provider.Result{}, err
	}
	if _, err := r.c.WaitOperation(ctx, "run", "/v2/", op); err != nil {
		return provider.Result{}, err
	}
	rr, err := r.get(ctx, r.path(req.Env, req.Properties, name))
	if err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: name, Attributes: r.attrs(rr)}, nil
}

func (r *runService) Read(ctx context.Context, req provider.Request) (provider.Result, error) {
	rr, err := r.get(ctx, r.path(req.Env, req.Properties, req.PhysicalID))
	if err != nil {
		return provider.Result{}, err
	}
	obs := map[string]any{
		"ingress":        rr.Ingress,
		"labels":         observedLabels(rr.Labels),
		"serviceAccount": rr.Template.ServiceAccount,
		"minInstances":   float64(rr.Template.Scaling.MinInstanceCount),
		"maxInstances":   float64(rr.Template.Scaling.MaxInstanceCount),
	}
	if provider.String(req.Properties, "serviceAccount") == "" {
		delete(obs, "serviceAccount") // the default compute account is filled in by the API
	}
	if len(rr.Template.Containers) > 0 {
		obs["image"] = rr.Template.Containers[0].Image
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: r.attrs(rr), Observed: obs}, nil
}

func (r *runService) Update(ctx context.Context, req provider.Request) (provider.Result, error) {
	path := r.path(req.Env, req.Properties, req.PhysicalID)
	var op operation
	if err := r.c.Do(ctx, http.MethodPatch, r.c.endpoint("run")+"/v2/"+path, r.body(req.Properties), &op); err != nil {
		return provider.Result{}, err
	}
	if _, err := r.c.WaitOperation(ctx, "run", "/v2/", op); err != nil {
		return provider.Result{}, err
	}
	rr, err := r.get(ctx, path)
	if err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: r.attrs(rr)}, nil
}

func (r *runService) Delete(ctx context.Context, req provider.Request) error {
	var op operation
	if err := r.c.Do(ctx, http.MethodDelete, r.c.endpoint("run")+"/v2/"+r.path(req.Env, req.Properties, req.PhysicalID), nil, &op); err != nil {
		return err
	}
	_, err := r.c.WaitOperation(ctx, "run", "/v2/", op)
	return err
}

// ------------------------------------------------------------------ secret

type secret struct{ c *Client }

func (s *secret) Schema() provider.Schema {
	return provider.Schema{
		Type:        "gcp:secretmanager:Secret",
		Description: "A Secret Manager secret container. Versions (values) are added out of band so they never appear in templates or state.",
		Properties: map[string]provider.PropertySpec{
			"secretId":             {Type: provider.TypeString, ForceNew: true},
			"replicationLocations": {Type: provider.TypeList, ForceNew: true, Description: "User-managed replica regions; automatic when empty."},
			"labels":               {Type: provider.TypeMap},
		},
		Attributes:   map[string]string{"name": "Secret ID.", "id": "Full resource name."},
		NameProperty: "secretId",
		Name:         provider.NameConstraints{MaxLen: 255},
		Labels:       true,
	}
}

func (s *secret) url(env provider.Env, id string) string {
	return fmt.Sprintf("%s/v1/projects/%s/secrets/%s", s.c.endpoint("secretmanager"), env.Project, url.PathEscape(id))
}

func (s *secret) attrs(env provider.Env, id string) map[string]any {
	return map[string]any{"name": id, "id": fmt.Sprintf("projects/%s/secrets/%s", env.Project, id)}
}

func (s *secret) Create(ctx context.Context, req provider.Request) (provider.Result, error) {
	id := provider.String(req.Properties, "secretId")
	replication := map[string]any{"automatic": map[string]any{}}
	if locs := provider.StringList(req.Properties, "replicationLocations"); len(locs) > 0 {
		var replicas []any
		for _, l := range locs {
			replicas = append(replicas, map[string]any{"location": l})
		}
		replication = map[string]any{"userManaged": map[string]any{"replicas": replicas}}
	}
	body := map[string]any{"replication": replication, "labels": labelsBody(req.Properties)}
	u := fmt.Sprintf("%s/v1/projects/%s/secrets?secretId=%s", s.c.endpoint("secretmanager"), req.Env.Project, url.QueryEscape(id))
	if err := s.c.Do(ctx, http.MethodPost, u, body, nil); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: id, Attributes: s.attrs(req.Env, id)}, nil
}

func (s *secret) Read(ctx context.Context, req provider.Request) (provider.Result, error) {
	var out struct {
		Labels map[string]string `json:"labels"`
	}
	if err := s.c.Do(ctx, http.MethodGet, s.url(req.Env, req.PhysicalID), nil, &out); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: s.attrs(req.Env, req.PhysicalID),
		Observed: map[string]any{"labels": observedLabels(out.Labels)}}, nil
}

func (s *secret) Update(ctx context.Context, req provider.Request) (provider.Result, error) {
	if len(changed(req.OldProperties, req.Properties, "labels")) > 0 {
		body := map[string]any{"labels": labelsBody(req.Properties)}
		if err := s.c.Do(ctx, http.MethodPatch, s.url(req.Env, req.PhysicalID)+"?updateMask=labels", body, nil); err != nil {
			return provider.Result{}, err
		}
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: s.attrs(req.Env, req.PhysicalID)}, nil
}

func (s *secret) Delete(ctx context.Context, req provider.Request) error {
	return s.c.Do(ctx, http.MethodDelete, s.url(req.Env, req.PhysicalID), nil, nil)
}
