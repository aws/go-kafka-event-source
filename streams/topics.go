// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package streams

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/aws/go-kafka-event-source/streams/sak"
	"github.com/google/btree"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TopicPartition struct {
	Partition int32
	Topic     string
}

// ntp == 'New Topic Partition'. Essentially a macro for TopicPartition{Parition: p, Topic: t} which is quite verbose
func ntp(p int32, t string) TopicPartition {
	return TopicPartition{Partition: p, Topic: t}
}

var tpSetFreeList = btree.NewFreeListG[TopicPartition](128)

// A convenience data structure. It is what the name implies, a Set of TopicPartitions.
// This data structure is not thread-safe. You will need to providde your own locking mechanism.
type TopicPartitionSet struct {
	*btree.BTreeG[TopicPartition]
}

// Comparator for TopicPartitions
func topicPartitionLess(a, b TopicPartition) bool {
	res := a.Partition - b.Partition
	if res != 0 {
		return res < 0
	}
	return a.Topic < b.Topic
}

// Returns a new, empty TopicPartitionSet.
func NewTopicPartitionSet() TopicPartitionSet {
	return TopicPartitionSet{btree.NewWithFreeListG(16, topicPartitionLess, tpSetFreeList)}
}

// Insert the TopicPartition. Returns true if the item was inserted, false if the item was aready present
func (tps TopicPartitionSet) Insert(tp TopicPartition) bool {
	_, ok := tps.ReplaceOrInsert(tp)
	return !ok
}

// Tertuens true if the tp is currently a member of TopicPartitionSet
func (tps TopicPartitionSet) Contains(tp TopicPartition) bool {
	_, ok := tps.Get(tp)
	return ok
}

// Removes tp from the TopicPartitionSet. Rerurns true is the item was present.
func (tps TopicPartitionSet) Remove(tp TopicPartition) bool {
	_, ok := tps.Delete(tp)
	return ok
}

// Converts the set to a newly allocate slice of TopicPartitions.
func (tps TopicPartitionSet) Items() []TopicPartition {
	slice := make([]TopicPartition, 0, tps.Len())
	tps.Ascend(func(tp TopicPartition) bool {
		slice = append(slice, tp)
		return true
	})
	return slice
}

type Source struct {
	// The group id for the underlying Kafka consumer group.
	GroupId string
	// The Kafka Topic to consume
	Topic string
	// The desired number of partitions for Topic.
	NumPartitions int
	// The desired replication factor for Topic. Defaults to 1.
	ReplicationFactor int
	// The desired min-insync-replicas for Topic. Defaults to 1.
	MinInSync int
	// The number of Kafka partitions to use for the applications commit log. Defaults to 5 if unset.
	CommitLogPartitions int
	// The Kafka cluster on which Topic resides.
	SourceCluster Cluster
	// StateCluster is the Kafka cluster on which the commit log and the StateStore topic resides. If left unset (recommended), defaults to SourceCluster.
	StateCluster Cluster
	// The consumer rebalance strategies to use for the underlying Kafka consumer group.
	BalanceStrategies []BalanceStrategy
	/*
		CommitOffsets should be set to true
		if you are migrating from a traditional consumer group.
		This will ensure that the offsets are commited to the consumer group
		when in a mixed fleet scenario (migrating into an EventSource from a standard consumer).
		If the deploytment fails, the original non-EventSource application can then
		resume consuming from the commited offsets. Once the EventSource application is well-established,
		this setting should be switched to false as offsets are managed by another topic.
		In a EventSource application, committing offsets via the standard mechanism only
		consumes resources and provides no benefit.
	*/
	CommitOffsets bool
	/*
		The config used for the eos producer pool. If empty, [DefaultEosConfig] is used. If an EventSource is initialized with an invalid
		[EosConfig], the application will panic.
	*/
	EosConfig EosConfig
}

// An interface for implementing a resusable Kafka client configuration.
// TODO: document reserved options
type Cluster interface {
	// Returns the list of kgo.Opt(s) that will be used whenever a connection is made to this cluster.
	// At minimum, it should return the kgo.SeedBrokers() option.
	Config() ([]kgo.Opt, error)
}

// A [Cluster] implementation useful for local development/testing. Establishes a plain text connection to a Kafka cluster.
// For a more advanced example, see [github.com/aws/go-kafka-event-source/msk].
//
//	cluster := streams.SimpleCluster([]string{"127.0.0.1:9092"})
type SimpleCluster []string

// Returns []kgo.Opt{kgo.SeedBrokers(sc...)}
func (sc SimpleCluster) Config() ([]kgo.Opt, error) {
	return []kgo.Opt{kgo.SeedBrokers(sc...)}, nil
}

// NewClient creates a kgo.Client from the options retuned from the provided [Cluster] and addtional `options`.
// Used internally and exposed for convenience.
func NewClient(cluster Cluster, options ...kgo.Opt) (*kgo.Client, error) {
	configOptions := []kgo.Opt{kgo.WithLogger(kgoLogger), kgo.ProducerBatchCompression(kgo.NoCompression())}
	clusterOpts, err := cluster.Config()
	if err != nil {
		return nil, err
	}
	configOptions = append(configOptions, clusterOpts...)
	configOptions = append(configOptions, options...)
	return kgo.NewClient(configOptions...)
}

// Returns the formatted topic name usewd for the commit log of Source
func (s Source) CommitLogTopicNameForGroupId() string {
	return fmt.Sprintf("gkes_commit_log_%s", s.GroupId)
}

// Returns the formatted topic name used for the change log ([StateStore]) of Source
func (s Source) ChangeLogTopicName() string {
	return fmt.Sprintf("gkes_change_log_%s_%s", s.Topic, s.GroupId)
}

// Returns Source.StateCluster if defined, otherwise Source.Cluster
func (s Source) stateCluster() Cluster {
	if s.StateCluster == nil {
		return s.SourceCluster
	}
	return s.StateCluster
}

func minInSyncConfig(source Source) string {
	factor := replicationFactorConfig(source)
	if factor <= 1 {
		return "1"
	}
	if source.MinInSync >= int(factor) {
		return fmt.Sprintf("%d", source.ReplicationFactor-1)
	}
	return fmt.Sprintf("%d", source.MinInSync)
}

func replicationFactorConfig(source Source) int16 {
	if source.ReplicationFactor <= 0 {
		return 1
	}
	return int16(source.ReplicationFactor)
}

func commitLogPartitionsConfig(source Source) int32 {
	if source.CommitLogPartitions <= 0 {
		return int32(5)
	}
	return int32(source.CommitLogPartitions)
}

func createTopic(adminClient *kadm.Client, numPartitions int32, replicationFactor int16, config map[string]*string, topic ...string) error {
	res, err := adminClient.CreateTopics(context.Background(), numPartitions, replicationFactor, config, topic...)
	log.Infof("createTopic res: %+v, err: %v", res, err)
	return err
}

func createDestination(destination Destination) (Destination, error) {
	client, err := NewClient(destination.Cluster)
	adminClient := kadm.NewClient(client)
	minInSync := fmt.Sprintf("%d", destination.MinInSync)
	createTopic(adminClient, int32(destination.NumPartitions), int16(destination.ReplicationFactor), map[string]*string{
		"min.insync.replicas": sak.Ptr(minInSync),
	})
	if err != nil {
		return destination, err
	}
	return destination, err
}

func CreateDestination(destination Destination) (resolved Destination, err error) {
	for retryCount := 0; retryCount < 15; retryCount++ {
		resolved, err = createDestination(destination)
		if isNetworkError(err) {
			time.Sleep(time.Second)
		}
	}
	return
}

func createSource(source Source) (Source, error) {
	sourceTopicClient, err := NewClient(source.SourceCluster, kgo.RequestRetries(20), kgo.RetryTimeout(30*time.Second))
	if err != nil {
		return source, err
	}
	eosClient, err := NewClient(source.stateCluster(), kgo.RequestRetries(20))
	if err != nil {
		return source, err
	}

	sourceTopicAdminClient := kadm.NewClient(sourceTopicClient)
	eosAdminClient := kadm.NewClient(eosClient)
	createTopic(sourceTopicAdminClient, int32(source.NumPartitions), replicationFactorConfig(source), map[string]*string{
		"min.insync.replicas": sak.Ptr(minInSyncConfig(source)),
	}, source.Topic)

	createTopic(eosAdminClient, commitLogPartitionsConfig(source), replicationFactorConfig(source), map[string]*string{
		"cleanup.policy":            sak.Ptr("compact"),
		"min.insync.replicas":       sak.Ptr(minInSyncConfig(source)),
		"min.cleanable.dirty.ratio": sak.Ptr("0.9"),
	}, source.CommitLogTopicNameForGroupId())

	changeLogPartitions := int32(source.NumPartitions)

	createTopic(eosAdminClient, changeLogPartitions, replicationFactorConfig(source), map[string]*string{
		"cleanup.policy":            sak.Ptr("compact"),
		"min.insync.replicas":       sak.Ptr(minInSyncConfig(source)),
		"min.cleanable.dirty.ratio": sak.Ptr("0.5"),
	}, source.ChangeLogTopicName())

	source, err = resolveTopicMetadata(source, sourceTopicAdminClient, eosAdminClient)
	sourceTopicClient.Close()
	eosClient.Close()
	return source, err
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	var opError *net.OpError
	if errors.As(err, &opError) {
		log.Warnf("network error for operation: %s, error: %v", opError.Op, opError)
		return true
	}
	return false
}

// Creates all necessary topics in the Kafka appropriate clusters as defined by Source.
// Automatically invoked as part of NewSourceConsumer(). Ignores errros TOPIC_ALREADT_EXISTS errors.
// Returns a corrected Source where NumPartitions and CommitLogPartitions are pulled from a ListTopics call. This is to prevent drift errors.
// Returns an error if the details for Source topics could not be retrieved, or if there is a mismatch in partition counts fo the source topic and change log topic.
func CreateSource(source Source) (resolved Source, err error) {
	for retryCount := 0; retryCount < 15; retryCount++ {
		resolved, err = createSource(source)
		if isNetworkError(err) {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	return
}

func resolveTopicMetadata(source Source, sourceTopicAdminClient, eosAdminClient *kadm.Client) (Source, error) {

	res, err := sourceTopicAdminClient.ListTopicsWithInternal(context.Background(), source.Topic)
	if err != nil {
		return source, err
	}
	if len(res) != 1 {
		return source, fmt.Errorf("source topic does not exist")
	}
	source.NumPartitions = len(res[source.Topic].Partitions.Numbers())
	source.ReplicationFactor = res[source.Topic].Partitions.NumReplicas()

	commitLogName := source.CommitLogTopicNameForGroupId()
	changLogName := source.ChangeLogTopicName()
	res, err = eosAdminClient.ListTopicsWithInternal(context.Background(), commitLogName, changLogName)
	if err != nil {
		return source, err
	}
	if topicDetail, ok := res[commitLogName]; ok {
		source.CommitLogPartitions = len(topicDetail.Partitions.Numbers())
	} else {
		return source, fmt.Errorf("commit log topic does not exist")
	}

	if topicDetail, ok := res[changLogName]; ok {
		changeLogPartitionCount := len(topicDetail.Partitions.Numbers())
		if changeLogPartitionCount != source.NumPartitions {
			return source, fmt.Errorf("change log partitition count (%d) does not match source topic partition count (%d)",
				changeLogPartitionCount, source.NumPartitions)
		}
	} else {
		return source, fmt.Errorf("change log topic does not exist")
	}

	return source, nil
}

// Deletes all topics associated with a Source. Provided for local testing purpoose only.
// Do not call this in deployed applications unless your topics are transient in nature.
func DeleteSource(source Source) error {
	sourceTopicClient, err := NewClient(source.SourceCluster)
	if err != nil {
		return err
	}
	eosClient, err := NewClient(source.stateCluster())
	if err != nil {
		return err
	}

	sourceTopicAdminClient := kadm.NewClient(sourceTopicClient)
	eosAdminClient := kadm.NewClient(eosClient)
	sourceTopicAdminClient.DeleteTopics(context.Background(), source.Topic)
	eosAdminClient.DeleteTopics(context.Background(),
		source.CommitLogTopicNameForGroupId(),
		source.ChangeLogTopicName())
	return nil
}

func isMarkerRecord(record *kgo.Record) bool {
	return len(record.Headers) == 1 && record.Headers[0].Key == markerKeyString
}
