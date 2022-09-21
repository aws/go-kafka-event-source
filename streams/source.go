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

import "fmt"

type SourcePartitionEventHandler func(*Source, int32)

type EventSourceConfig struct {
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
	// The Kafka cluster on which Topic resides, or the source of incoming events.
	SourceCluster Cluster
	// StateCluster is the Kafka cluster on which the commit log and the StateStore topic resides. If left unset (recommended), defaults to SourceCluster.
	StateCluster Cluster
	// The consumer rebalance strategies to use for the underlying Kafka consumer group.
	BalanceStrategies []BalanceStrategy
	/*
		CommitOffsets should be set to true if you are migrating from a traditional consumer group.
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
	// If non-nil, the EventSorce will emit [Metric] objects of varying types. This is backed by a channel. If the channel is full
	// (presumably because the MetricHandler is not able to keep up),
	// GKES will drop the metric and log at WARN level to prevent processing slow down.
	MetricsHandler MetricsHandler

	// Called when a partition has been assigned to the EventSource consumer client. This does not indicate that the partion is being processed.
	OnPartitionAssigned SourcePartitionEventHandler

	// Called when a perviously assigned partition has been activated, meaning the EventSource will start processing events for this partition. At the time this handler is called, the  StateStore associated with this partition has been bootstrapped and is ready for use.
	OnPartitionActivated SourcePartitionEventHandler

	// Called when a partition is about to be revoked from the EventSource consumer client.
	// This is a blocking call and, as such, should return quickly.
	OnPartitionWillRevoke SourcePartitionEventHandler
	// Called when a partition has been revoked from the EventSource consumer client.
	// This handler is invoked after GKES has stopped processing and has finished removing any associated resources for the partition.
	OnPartitionRevoked          SourcePartitionEventHandler
	DeserializationErrorHandler DeserializationErrorHandler
	EosErrorHandler             EosErrorHandler
}

type Source struct {
	config EventSourceConfig
}

func (s *Source) AsDestination() Destination {
	return Destination{
		DefaultTopic:  s.config.Topic,
		NumPartitions: s.config.NumPartitions,
		Cluster:       s.config.SourceCluster,
	}
}

func (s *Source) onPartitionsAssigned(partitions []int32) {
	s.executeHandler(s.config.OnPartitionAssigned, partitions)
}

func (s *Source) onPartitionWillRevoke(partition int32) {
	s.executeHandler(s.config.OnPartitionWillRevoke, []int32{partition})
}

func (s *Source) onPartitionActivated(partition int32) {
	s.executeHandler(s.config.OnPartitionActivated, []int32{partition})
}

func (s *Source) onPartitionsRevoked(partitions []int32) {
	s.executeHandler(s.config.OnPartitionRevoked, partitions)
}

func (s *Source) shouldMarkCommit() bool {
	return s.config.CommitOffsets
}

func (s *Source) eosErrorHandler() EosErrorHandler {
	if s.config.EosErrorHandler == nil {
		return DefaultEosErrorHandler
	}
	return s.config.EosErrorHandler
}

func (s *Source) deserializationErrorHandler() DeserializationErrorHandler {
	if s.config.EosErrorHandler == nil {
		return DefaultDeserializationErrorHandler
	}
	return s.config.DeserializationErrorHandler
}

func (s *Source) executeHandler(handler SourcePartitionEventHandler, partitions []int32) {
	if handler != nil {
		for _, p := range partitions {
			handler(s, p)
		}
	}
}

func (s *Source) Topic() string {
	return s.config.Topic
}

func (s *Source) GroupId() string {
	return s.config.GroupId
}

func (s *Source) Config() EventSourceConfig {
	return s.config
}

func (s *Source) BalanceStrategies() []BalanceStrategy {
	return s.config.BalanceStrategies
}

func (s *Source) NumPartitions() int {
	return s.config.NumPartitions
}

// Returns the formatted topic name usewd for the commit log of Source
func (s *Source) CommitLogTopicNameForGroupId() string {
	return fmt.Sprintf("gkes_commit_log_%s", s.config.GroupId)
}

// Returns the formatted topic name used for the change log ([StateStore]) of Source
func (s *Source) ChangeLogTopicName() string {
	return fmt.Sprintf("gkes_change_log_%s_%s", s.config.Topic, s.config.GroupId)
}

// Returns Source.StateCluster if defined, otherwise Source.Cluster
func (s *Source) stateCluster() Cluster {
	if s.config.StateCluster == nil {
		return s.config.SourceCluster
	}
	return s.config.StateCluster
}

func minInSyncConfig(source *Source) int {
	factor := replicationFactorConfig(source)
	if factor <= 1 {
		return 1
	}
	if source.config.MinInSync >= int(factor) {
		return source.config.ReplicationFactor - 1
	}
	return source.config.MinInSync
}

func replicationFactorConfig(source *Source) int {
	if source.config.ReplicationFactor <= 0 {
		return 1
	}
	return source.config.ReplicationFactor
}

func commitLogPartitionsConfig(source *Source) int {
	if source.config.CommitLogPartitions <= 0 {
		return 5
	}
	return source.config.CommitLogPartitions
}
