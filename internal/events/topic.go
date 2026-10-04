package events

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CreateTopic creates the topic with its fixed partition count. It is a no-op if the topic exists with that count,
// and an error if it exists with any other: per-account order depends on the count never changing.
func CreateTopic(ctx context.Context, cl *kgo.Client, replicationFactor int16) error {
	adm := kadm.NewClient(cl)
	resp, err := adm.CreateTopic(ctx, Partitions, replicationFactor, nil, Topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %s: %w", Topic, err)
	}
	details, err := adm.ListTopics(ctx, Topic)
	if err != nil {
		return fmt.Errorf("describe topic %s: %w", Topic, err)
	}
	if n := len(details[Topic].Partitions); n != Partitions {
		return fmt.Errorf("topic %s has %d partitions, want %d", Topic, n, Partitions)
	}
	return nil
}
