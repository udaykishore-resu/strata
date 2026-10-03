// Package pubsub provides Pub/Sub constructs.
package pubsub

import (
	"fmt"
	"time"

	"github.com/udaykishore-resu/strata/pkg/cdk"
)

// TopicProps configures a topic.
type TopicProps struct {
	Name             string // generated when empty
	MessageRetention time.Duration
	Labels           map[string]any
}

// Topic is a Pub/Sub topic.
type Topic struct {
	*cdk.Resource
	scope cdk.Construct
}

// NewTopic creates a topic.
func NewTopic(scope cdk.Construct, id string, props *TopicProps) *Topic {
	if props == nil {
		props = &TopicProps{}
	}
	p := map[string]any{}
	if props.Name != "" {
		p["name"] = props.Name
	}
	if props.MessageRetention > 0 {
		p["messageRetentionDuration"] = fmt.Sprintf("%ds", int(props.MessageRetention.Seconds()))
	}
	if len(props.Labels) > 0 {
		p["labels"] = props.Labels
	}
	r := cdk.NewResource(scope, id, "gcp:pubsub:Topic", p)
	r.AddDependency(cdk.StackOf(scope).RequireService("pubsub.googleapis.com"))
	return &Topic{Resource: r, scope: scope}
}

// TopicName returns a token for the topic ID.
func (t *Topic) TopicName() any { return t.Ref() }

// ResourceName returns a token for projects/P/topics/ID.
func (t *Topic) ResourceName() any { return t.Attr("id") }

// GrantPublish allows g to publish.
func (t *Topic) GrantPublish(g cdk.Grantee) *cdk.Resource {
	return cdk.NewResource(t, cdk.GrantID(t, "roles/pubsub.publisher"), "gcp:pubsub:TopicIamMember", map[string]any{
		"topic": t.Ref(), "role": "roles/pubsub.publisher", "member": g.GrantMember(),
	})
}

// AddSubscription creates a subscription to this topic in the topic's scope.
func (t *Topic) AddSubscription(id string, props *SubscriptionProps) *Subscription {
	if props == nil {
		props = &SubscriptionProps{}
	}
	cp := *props
	cp.Topic = t
	return NewSubscription(t.scope, id, &cp)
}

// SubscriptionProps configures a subscription.
type SubscriptionProps struct {
	Topic               *Topic
	Name                string
	AckDeadline         time.Duration
	MessageRetention    time.Duration
	RetainAckedMessages bool
	DeadLetterTopic     *Topic
	MaxDeliveryAttempts int
	PushEndpoint        any // literal URL or token, e.g. a run.Service URL
	Filter              string
	EnableOrdering      bool
	Labels              map[string]any
}

// Subscription is a Pub/Sub subscription.
type Subscription struct {
	*cdk.Resource
}

// NewSubscription creates a subscription.
func NewSubscription(scope cdk.Construct, id string, props *SubscriptionProps) *Subscription {
	if props == nil || props.Topic == nil {
		panic("pubsub: SubscriptionProps.Topic is required")
	}
	p := map[string]any{"topic": props.Topic.Ref()}
	if props.Name != "" {
		p["name"] = props.Name
	}
	if props.AckDeadline > 0 {
		p["ackDeadlineSeconds"] = int(props.AckDeadline.Seconds())
	}
	if props.MessageRetention > 0 {
		p["messageRetentionDuration"] = fmt.Sprintf("%ds", int(props.MessageRetention.Seconds()))
	}
	if props.RetainAckedMessages {
		p["retainAckedMessages"] = true
	}
	if props.DeadLetterTopic != nil {
		p["deadLetterTopic"] = props.DeadLetterTopic.Ref()
		if props.MaxDeliveryAttempts > 0 {
			p["maxDeliveryAttempts"] = props.MaxDeliveryAttempts
		}
	}
	if props.PushEndpoint != nil {
		p["pushEndpoint"] = props.PushEndpoint
	}
	if props.Filter != "" {
		p["filter"] = props.Filter
	}
	if props.EnableOrdering {
		p["enableMessageOrdering"] = true
	}
	if len(props.Labels) > 0 {
		p["labels"] = props.Labels
	}
	r := cdk.NewResource(scope, id, "gcp:pubsub:Subscription", p)
	return &Subscription{Resource: r}
}

// SubscriptionName returns a token for the subscription ID.
func (s *Subscription) SubscriptionName() any { return s.Ref() }

// GrantConsume allows g to pull and acknowledge messages.
func (s *Subscription) GrantConsume(g cdk.Grantee) *cdk.Resource {
	return cdk.NewResource(s, cdk.GrantID(s, "roles/pubsub.subscriber"), "gcp:pubsub:SubscriptionIamMember", map[string]any{
		"subscription": s.Ref(), "role": "roles/pubsub.subscriber", "member": g.GrantMember(),
	})
}
