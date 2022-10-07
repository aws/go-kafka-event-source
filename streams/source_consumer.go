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
	"sync"
	"time"

	"github.com/aws/go-kafka-event-source/streams/sak"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A thick wrapper around a kgo.Client. Handles interaction with IncrementalRebalancer, as well as providing mechanisms for interjecting into a stream.
type eventSourceConsumer[T StateStore] struct {
	client             *kgo.Client
	partitionedStore   *partitionedChangeLog[T]
	stateStoreConsumer *stateStoreConsumer[T]
	ctx                context.Context
	workers            map[int32]*partitionWorker[T]
	prepping           map[int32]*stateStorePartition[T]
	workerMux          sync.Mutex
	preppingMux        sync.Mutex
	incrBalancer       IncrementalGroupRebalancer
	eventSource        *EventSource[T]
	source             *Source
	commitLog          *eosCommitLog
	producerPool       *eosProducerPool[T]
	metrics            chan Metric
	// prepping           map[int32]*partitionPrepper[T]
}

// Creates a new eventSourceConsumer.
// `eventSource` must be a fully initialized EventSource.
func newEventSourceConsumer[T StateStore](eventSource *EventSource[T], additionalClientOptions ...kgo.Opt) (*eventSourceConsumer[T], error) {
	cl := newEosCommitLog(eventSource.source, int(commitLogPartitionsConfig(eventSource.source)))
	var partitionedStore *partitionedChangeLog[T]
	source := eventSource.source
	partitionedStore = newPartitionedChangeLog(eventSource.createChangeLogReceiver, source.StateStoreTopicName())

	sc := &eventSourceConsumer[T]{
		partitionedStore: partitionedStore,
		ctx:              eventSource.runStatus.Ctx(),
		workers:          make(map[int32]*partitionWorker[T]),
		prepping:         make(map[int32]*stateStorePartition[T]),
		eventSource:      eventSource,
		source:           source,
		commitLog:        cl,
		metrics:          eventSource.metrics,
	}
	balanceStrategies := source.config.BalanceStrategies
	if len(balanceStrategies) == 0 {
		balanceStrategies = DefaultBalanceStrategies
		source.config.BalanceStrategies = balanceStrategies
	}
	groupBalancers := toGroupBalancers(sc, balanceStrategies)
	balancerOpt := kgo.Balancers(groupBalancers...)
	opts := []kgo.Opt{
		balancerOpt,
		kgo.ConsumerGroup(source.config.GroupId),
		kgo.ConsumeTopics(source.config.Topic),
		kgo.OnPartitionsAssigned(sc.partitionsAssigned),
		kgo.OnPartitionsRevoked(sc.partitionsRevoked),
		kgo.SessionTimeout(6 * time.Second),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.FetchMaxWait(time.Second),
		kgo.AdjustFetchOffsetsFn(sc.adjustOffsetsBeforeAssign)}

	if len(additionalClientOptions) > 0 {
		opts = append(opts, additionalClientOptions...)
	}

	if source.shouldMarkCommit() {
		// we're in a migrating consumer group (non-GKES to GKES otr vice-versa)
		opts = append(opts, kgo.AutoCommitMarks(), kgo.AutoCommitInterval(time.Second*5))
	} else {
		opts = append(opts, kgo.DisableAutoCommit())
	}
	client, err := NewClient(
		source.config.SourceCluster, opts...)

	eosConfig := source.config.EosConfig
	if eosConfig.IsZero() {
		eosConfig = DefaultEosConfig
		source.config.EosConfig = eosConfig
	}

	eosConfig.validate()
	sc.producerPool = newEOSProducerPool[T](source, cl, eosConfig, client, eventSource.metrics)

	for _, gb := range groupBalancers {
		if igr, ok := gb.(IncrementalGroupRebalancer); ok {
			sc.incrBalancer = igr
			break
		}
	}
	sc.client = client
	sc.stateStoreConsumer = mewStateStoreConsumer[T](source)
	if err != nil {
		return nil, err
	}
	return sc, nil
}

// Since we're using out own commit log, adjust the starting offset for a newly assigned partition to refelct what is in the commitLog.
func (sc *eventSourceConsumer[T]) adjustOffsetsBeforeAssign(ctx context.Context, assignments map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	for topic, partitionAssignments := range assignments {
		partitions := sak.MapKeysToSlice(partitionAssignments)
		for _, p := range partitions {
			tp := ntp(p, topic)
			offset := sc.commitLog.Watermark(tp)
			log.Infof("starting consumption for %+v at offset: %d", tp, offset+1)
			if offset > 0 {
				partitionAssignments[p] = kgo.NewOffset().At(offset)
			}
		}
	}
	return assignments, nil
}

func (sc *eventSourceConsumer[T]) Client() *kgo.Client {
	return sc.client
}

// Needed to fulfill the IncrRebalanceInstructionHandler interface defined by IncrementalGroupRebalancer.
// Should NOT be invoked directly.
func (sc *eventSourceConsumer[T]) PrepareTopicPartition(tp TopicPartition) {
	sc.preppingMux.Lock()
	defer sc.preppingMux.Unlock()
	partition := tp.Partition
	if _, ok := sc.prepping[partition]; !ok {
		store := sc.partitionedStore.assign(partition)

		ssp := sc.stateStoreConsumer.preparePartition(partition, store)
		sc.prepping[partition] = ssp
		go func() {
			start := time.Now()
			log.Debugf("prepping %+v", tp)

			ssp.sync()
			processed := ssp.processed()
			duration := time.Since(start)

			log.Debugf("Prepped %+v, %d messages in %v (tps: %d)",
				tp, processed, duration, int(float64(processed)/duration.Seconds()))
			sc.incrBalancer.PartitionPrepared(tp)
			if sc.metrics != nil {
				sc.metrics <- Metric{
					Operation:      PartitionPreppedOperation,
					StartTime:      start,
					EndTime:        time.Now(),
					PartitionCount: 1,
					Partition:      partition,
					Count:          int(processed),
					Bytes:          int(ssp.processedBytes()),
					GroupId:        sc.source.GroupId(),
				}
			}
		}()
	}
}

// Needed to fulfill the IncrRebalanceInstructionHandler interface defined by IncrementalGroupRebalancer.
// Should NOT be invoked directly.
func (sc *eventSourceConsumer[T]) ForgetPreparedTopicPartition(tp TopicPartition) {
	sc.preppingMux.Lock()
	defer sc.preppingMux.Unlock()
	if _, ok := sc.prepping[tp.Partition]; ok {
		sc.stateStoreConsumer.cancelPartition(tp.Partition)
		delete(sc.prepping, tp.Partition)
	} else {
		// what to do? probably nothing, but if we have a double assignment, we could have problems
		// need to investigate this race condition further
		log.Warnf("ForgetPreparedTopicPartition failed for %+v", tp)
	}
}

func (sc *eventSourceConsumer[T]) assignPartitions(topic string, partitions []int32) {
	sc.workerMux.Lock()
	defer sc.workerMux.Unlock()
	sc.preppingMux.Lock()
	defer sc.preppingMux.Unlock()
	for _, p := range partitions {
		tp := TopicPartition{Partition: p, Topic: topic}
		store := sc.partitionedStore.assign(p)

		// we want to pause this until the partitionWorker is ready
		// otherwise we could fill our buffer and block other partitions while we sync the state store
		// sc.client.PauseFetchPartitions(map[string][]int32{topic: {p}})

		if prepper, ok := sc.prepping[p]; ok {
			log.Infof("syncing prepped partition %+v", prepper.topicPartition)
			delete(sc.prepping, p)
			sc.workers[p] = newPartitionWorker(sc.eventSource, tp, sc.commitLog, store, sc.producerPool, func() {
				prepper.sync()
				// sc.client.ResumeFetchPartitions(map[string][]int32{topic: {p}})
			})
		} else if _, ok := sc.workers[p]; !ok {
			prepper = sc.stateStoreConsumer.activatePartition(p, store)
			log.Infof("syncing unprepped partition %+v", prepper.topicPartition)
			sc.workers[p] = newPartitionWorker(sc.eventSource, tp, sc.commitLog, store, sc.producerPool, func() {
				prepper.sync()
				// sc.client.ResumeFetchPartitions(map[string][]int32{topic: {p}})
			})
		}

	}
	sc.incrBalancer.PartitionsAssigned(toTopicPartitions(topic, partitions...)...)
	// notify observers
	sc.source.onPartitionsAssigned(partitions)
}

func (sc *eventSourceConsumer[T]) revokePartitions(topic string, partitions []int32) {
	sc.workerMux.Lock()
	defer sc.workerMux.Unlock()
	if sc.partitionedStore == nil {
		return
	}

	for _, p := range partitions {
		sc.source.onPartitionWillRevoke(p)
		if worker, ok := sc.workers[p]; ok {
			worker.revoke()
			delete(sc.workers, p)
		}
		sc.partitionedStore.revoke(p)
	}
	// notify observers
	sc.source.onPartitionsRevoked(partitions)
}

func (sc *eventSourceConsumer[T]) partitionsAssigned(ctx context.Context, _ *kgo.Client, assignments map[string][]int32) {
	for topic, partitions := range assignments {
		log.Debugf("assigned topic: %s, partitions: %v", topic, assignments)
		sc.assignPartitions(topic, partitions)
	}
}

func (sc *eventSourceConsumer[T]) partitionsRevoked(ctx context.Context, _ *kgo.Client, assignments map[string][]int32) {
	for topic, partitions := range assignments {
		log.Debugf("revoked topic: %s, partitions: %v", topic, assignments)
		sc.revokePartitions(topic, partitions)
	}
}

func (sc *eventSourceConsumer[T]) receive(p kgo.FetchTopicPartition) {
	sc.workerMux.Lock()
	worker, ok := sc.workers[p.Partition]
	sc.workerMux.Unlock()
	if !ok || len(p.Records) == 0 {
		return
	}
	worker.add(p.Records)
}

// Starts the underlying kafka client and syncs the local commit log for the consumer group.
// Once synced, polls for records and forwards them to partitionWorkers.
func (sc *eventSourceConsumer[T]) start() {
	go sc.commitLog.Start()
	sc.commitLog.syncAll()
	for {
		ctx, cancel := context.WithTimeout(sc.ctx, 10*time.Second)
		f := sc.client.PollFetches(ctx)
		cancel()
		if f.IsClientClosed() {
			log.Infof("client closed for group: %v", sc.source.GroupId())
			return
		}
		for _, err := range f.Errors() {
			if err.Err != ctx.Err() {
				log.Errorf("%v", err)
			}
		}
		f.EachPartition(sc.receive)
	}
}

// Inserts the interjection into the appropriate partition workers interjectionChannel. Returns immediately if the partiotns is not currently assigned.
func (sc *eventSourceConsumer[T]) interject(partition int32, cmd Interjector[T]) <-chan error {
	sc.workerMux.Lock()
	defer sc.workerMux.Unlock()
	c := make(chan error, 1)
	w := sc.workers[partition]
	if w == nil {
		c <- ErrPartitionNotAssigned
		return c
	}
	if !w.canInterject() {
		c <- ErrPartitionNotAssigned
		return c
	}
	w.interjectionInput <- &interjection[T]{
		isOneOff:       true,
		topicPartition: w.topicPartition,
		interjector: func(ec *EventContext[T], t time.Time) ExecutionState {
			state := cmd(ec, t)
			close(c)
			return state
		},
	}
	return c
}

// A convenience function which allows you to Interject into every active partition assigned to the consumer
// without create an individual timer per partition.
// InterjectNow() will be invoked each active partition, blocking on each iteration until the Interjection can be processed.
// Useful for gathering store statistics, but can be used in place of a standard Interjection.
func (sc *eventSourceConsumer[T]) forEachChangeLogPartitionSync(interjector Interjector[T]) {
	sc.workerMux.Lock()
	ps := sak.MapKeysToSlice(sc.workers)
	sc.workerMux.Unlock()
	for _, p := range ps {
		if err := <-sc.interject(p, interjector); err != nil {
			log.Errorf("Could not interject into %d, error: %v", p, err)
		}
	}
}

type interjectionTracker struct {
	partition int32
	c         <-chan error
}

func (sc *eventSourceConsumer[T]) forEachChangeLogPartitionAsync(interjector Interjector[T]) {
	sc.workerMux.Lock()
	ps := sak.MapKeysToSlice(sc.workers)
	sc.workerMux.Unlock()

	its := make([]interjectionTracker, 0, len(ps))
	for _, p := range ps {
		its = append(its, interjectionTracker{p, sc.interject(p, interjector)})
	}
	for _, it := range its {
		if err := <-it.c; err != nil {
			log.Errorf("Could not interject into %d, error: %v", it.partition, err)
		}
	}
}

// TODO: This needs some more work after we provide balancer configuration.
// If the group only has 1 allowed protocol, there is no need for this check.
// If there are multiple, we need to interrogate Kafa to see which is active
func (sc *eventSourceConsumer[T]) currentProtocolIsIncremental() bool {
	if sc.incrBalancer == nil {
		return false
	}
	if len(sc.source.BalanceStrategies()) <= 1 {
		return true
	}
	// we're going to attempt to retrieve the current protocol
	// this is somehat unrealiable as if the group state is in PreparingRebalance mode,
	// the protocol returned will be empty
	adminClient := kadm.NewClient(sc.Client())
	groups, err := adminClient.DescribeGroups(context.Background(), sc.source.GroupId())
	if err != nil || len(groups) == 0 {
		log.Errorf("could not confirm group protocol: %v", err)
		return false
	}
	log.Debugf("consumerGroup protocol response: %+v", groups)
	group := groups[sc.source.GroupId()]
	if len(group.Protocol) == 0 {
		log.Warnf("could not retrieve group rebalance protocol, group state: %v", group.State)
	}
	return group.Protocol == IncrementalCoopProtocol

}

// Signals the IncrementalReblancer to start the process of shutting down this consumer in an orderly fashion.
func (sc *eventSourceConsumer[T]) leave() <-chan struct{} {
	log.Infof("leave signaled for group: %v", sc.source.GroupId())
	c := make(chan struct{}, 1)
	if sc.incrBalancer == nil || !sc.currentProtocolIsIncremental() {
		sc.stop()
		c <- struct{}{}
		return c
	}
	go func() {
		<-sc.incrBalancer.GracefullyLeaveGroup()
		sc.stop()
		c <- struct{}{}
	}()
	return c
}

// Immediately stops the consumer, leaving the consumer group abruptly.
func (sc *eventSourceConsumer[T]) stop() {
	sc.client.Close()
	sc.commitLog.Stop()
	sc.stateStoreConsumer.stop()
	log.Infof("left group: %v", sc.source.GroupId())
}
