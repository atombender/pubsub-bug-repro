package bug_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestBug(t *testing.T) {
	ctx := context.Background()

	srv := pstest.NewServer()
	t.Cleanup(func() {
		srv.Close()
	})

	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	projectID := nuid.Next()
	client, err := pubsub.NewClientWithConfig(ctx, projectID, &pubsub.ClientConfig{},
		option.WithGRPCConn(conn))
	require.NoError(t, err)

	topicID := nuid.Next()
	topic, err := srv.GServer.CreateTopic(context.Background(), &pubsubpb.Topic{
		Name: fmt.Sprintf("projects/%s/topics/%s", projectID, topicID),
	})
	require.NoError(t, err)

	subID := nuid.Next()
	_, err = srv.GServer.CreateSubscription(context.Background(), &pubsubpb.Subscription{
		Name:               fmt.Sprintf("projects/%s/subscriptions/%s", projectID, subID),
		Topic:              topic.Name,
		AckDeadlineSeconds: 10,
		RetryPolicy: &pubsubpb.RetryPolicy{
			MinimumBackoff: &durationpb.Duration{Seconds: 1},
			MaximumBackoff: &durationpb.Duration{Seconds: 1},
		},
	})
	require.NoError(t, err)

	srv.Publish(topic.Name, []byte("{}"), map[string]string{})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			cancel()
		}
	}()

	retryCount := 0
	sub := client.Subscription(subID)
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		t.Logf("received message")
		if retryCount < 2 {
			retryCount++
			m.Nack()
			t.Logf("nacking")
		} else {
			t.Logf("acking, done")
			m.Ack()
			go cancel()
		}
	})
	require.NoError(t, err)
	require.Equal(t, 2, retryCount)
}
