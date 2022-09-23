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
	"strconv"
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

// var missingTopicError = errors.New("topic does not exist")

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

func createTopicFromConfigMap(adminClient *kadm.Client, numPartitions int32, replicationFactor int16, config map[string]*string, topic ...string) error {
	res, err := adminClient.CreateTopics(context.Background(), numPartitions, replicationFactor, config, topic...)
	log.Infof("createTopic res: %+v, err: %v", res, err)
	return err
}

func createTopic(adminClient *kadm.Client, numPartitions int, replicationFactor int, minInsync int, cleanupPolicy CleanupPolicy, dirtyRatio float64, topic ...string) error {
	configMap := map[string]*string{
		"min.insync.replicas": sak.Ptr(strconv.Itoa(minInsync)),
	}
	if cleanupPolicy == CompactCleanupPolicy {
		configMap["cleanup.policy"] = sak.Ptr("compact")
		configMap["min.cleanable.dirty.ratio"] = sak.Ptr(strconv.FormatFloat(dirtyRatio, 'f', 2, 64))
	}
	return createTopicFromConfigMap(adminClient, int32(numPartitions), int16(replicationFactor), configMap, topic...)
}

func createDestination(destination Destination) (Destination, error) {
	client, err := NewClient(destination.Cluster)
	adminClient := kadm.NewClient(client)
	minInSync := fmt.Sprintf("%d", destination.MinInSync)
	createTopicFromConfigMap(adminClient, int32(destination.NumPartitions), int16(destination.ReplicationFactor), map[string]*string{
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

func createSource(source *Source) (*Source, error) {
	sourceTopicClient, err := NewClient(source.config.SourceCluster, kgo.RequestRetries(20), kgo.RetryTimeout(30*time.Second))
	if err != nil {
		return source, err
	}
	eosClient, err := NewClient(source.stateCluster(), kgo.RequestRetries(20))
	if err != nil {
		return source, err
	}

	sourceTopicAdminClient := kadm.NewClient(sourceTopicClient)
	eosAdminClient := kadm.NewClient(eosClient)
	source, err = resolveOrCreateTopics(source, sourceTopicAdminClient, eosAdminClient)
	// createTopic(sourceTopicAdminClient, source.NumPartitions(),
	// 	replicationFactorConfig(source), minInSyncConfig(source), DeleteCleanupPolicy, 1, source.Topic())

	// createTopic(eosAdminClient, commitLogPartitionsConfig(source),
	// 	replicationFactorConfig(source), minInSyncConfig(source), CompactCleanupPolicy, 0.9, source.CommitLogTopicNameForGroupId())

	// createTopic(eosAdminClient, source.NumPartitions(),
	// 	replicationFactorConfig(source), minInSyncConfig(source), CompactCleanupPolicy, 0.5, source.ChangeLogTopicName())

	// source, err = resolveTopicMetadata(source, sourceTopicAdminClient, eosAdminClient)
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
	} else if err != nil {
		log.Errorf("non network error for operation: %s, error: %v", opError.Op, opError)
	}
	return false
}

// Creates all necessary topics in the Kafka appropriate clusters as defined by Source.
// Automatically invoked as part of NewSourceConsumer(). Ignores errros TOPIC_ALREADT_EXISTS errors.
// Returns a corrected Source where NumPartitions and CommitLogPartitions are pulled from a ListTopics call. This is to prevent drift errors.
// Returns an error if the details for Source topics could not be retrieved, or if there is a mismatch in partition counts fo the source topic and change log topic.
func CreateSource(sourceConfig EventSourceConfig) (resolved *Source, err error) {
	source := &Source{sourceConfig}
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

func resolveOrCreateTopics(source *Source, sourceTopicAdminClient, eosAdminClient *kadm.Client) (*Source, error) {
	topic := source.Topic()
	commitLogName := source.CommitLogTopicNameForGroupId()
	changLogName := source.ChangeLogTopicName()
	res, err := sourceTopicAdminClient.ListTopicsWithInternal(context.Background(), topic)
	if err != nil {
		return nil, err
	}
	if val, ok := res[topic]; ok && val.Err == nil {
		source.config.NumPartitions = len(val.Partitions.Numbers())
		source.config.ReplicationFactor = val.Partitions.NumReplicas()
	} else {
		err = createTopic(sourceTopicAdminClient, source.NumPartitions(),
			replicationFactorConfig(source), minInSyncConfig(source), DeleteCleanupPolicy, 1, topic)
		if err != nil {
			return nil, err
		}
	}

	res, err = eosAdminClient.ListTopicsWithInternal(context.Background(), commitLogName, changLogName)
	if err != nil {
		return nil, err
	}
	if val, ok := res[commitLogName]; ok && val.Err == nil {
		source.config.CommitLogPartitions = len(val.Partitions.Numbers())
	} else {
		err = createTopic(eosAdminClient, commitLogPartitionsConfig(source),
			replicationFactorConfig(source), minInSyncConfig(source), CompactCleanupPolicy, 0.9, commitLogName)
		if err != nil {
			return nil, err
		}
	}

	if val, ok := res[changLogName]; ok && val.Err == nil {
		changeLogPartitionCount := len(val.Partitions.Numbers())
		if changeLogPartitionCount != source.config.NumPartitions {
			return nil, fmt.Errorf("change log partitition count (%d) does not match source topic partition count (%d)",
				changeLogPartitionCount, source.config.NumPartitions)
		}
	} else {
		err = createTopic(eosAdminClient, source.NumPartitions(),
			replicationFactorConfig(source), minInSyncConfig(source), CompactCleanupPolicy, 0.5, changLogName)
		if err != nil {
			return nil, err
		}
	}

	return source, nil
}

// func resolveTopicMetadata(source *Source, sourceTopicAdminClient, eosAdminClient *kadm.Client) (*Source, error) {
// 	res, err := sourceTopicAdminClient.ListTopicsWithInternal(context.Background(), source.config.Topic)
// 	if err != nil {
// 		return source, err
// 	}
// 	if len(res) != 1 {
// 		return source, missingTopicError
// 	}
// 	source.config.NumPartitions = len(res[source.config.Topic].Partitions.Numbers())
// 	source.config.ReplicationFactor = res[source.config.Topic].Partitions.NumReplicas()

// 	commitLogName := source.CommitLogTopicNameForGroupId()
// 	changLogName := source.ChangeLogTopicName()
// 	res, err = eosAdminClient.ListTopicsWithInternal(context.Background(), commitLogName, changLogName)
// 	if err != nil {
// 		return source, err
// 	}
// 	if topicDetail, ok := res[commitLogName]; ok {
// 		source.config.CommitLogPartitions = len(topicDetail.Partitions.Numbers())
// 	} else {
// 		return source, missingTopicError
// 	}

// 	if topicDetail, ok := res[changLogName]; ok {
// 		changeLogPartitionCount := len(topicDetail.Partitions.Numbers())
// 		if changeLogPartitionCount != source.config.NumPartitions {
// 			return source, fmt.Errorf("change log partitition count (%d) does not match source topic partition count (%d)",
// 				changeLogPartitionCount, source.config.NumPartitions)
// 		}
// 	} else {
// 		return source, missingTopicError
// 	}

// 	return source, nil
// }

// Deletes all topics associated with a Source. Provided for local testing purpoose only.
// Do not call this in deployed applications unless your topics are transient in nature.
func DeleteSource(sourceConfig EventSourceConfig) error {
	source := &Source{sourceConfig}
	sourceTopicClient, err := NewClient(source.config.SourceCluster)
	if err != nil {
		return err
	}
	eosClient, err := NewClient(source.stateCluster())
	if err != nil {
		return err
	}

	sourceTopicAdminClient := kadm.NewClient(sourceTopicClient)
	eosAdminClient := kadm.NewClient(eosClient)
	sourceTopicAdminClient.DeleteTopics(context.Background(), source.config.Topic)
	eosAdminClient.DeleteTopics(context.Background(),
		source.CommitLogTopicNameForGroupId(),
		source.ChangeLogTopicName())
	return nil
}

func isMarkerRecord(record *kgo.Record) bool {
	return len(record.Headers) == 1 && record.Headers[0].Key == markerKeyString
}

func toTopicPartitions(topic string, partitions ...int32) []TopicPartition {
	tps := make([]TopicPartition, len(partitions))
	for i, p := range partitions {
		tps[i] = ntp(p, topic)
	}
	return tps
}
