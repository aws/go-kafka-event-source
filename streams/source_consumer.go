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
	client           *kgo.Client
	partitionedStore *partitionedChangeLog[T]
	ctx              context.Context
	workers          map[TopicPartition]*partitionWorker[T]
	prepping         map[TopicPartition]*partitionPrepper[T]
	workerMux        sync.Mutex
	preppingMux      sync.Mutex
	incrBalancer     IncrementalGroupRebalancer
	eventSource      *EventSource[T]
	commitLog        *eosCommitLog
	producerPool     *eosProducerPool[T]
}

// Creates a new eventSourceConsumer.
// `eventSource` must be a fully initialized EventSource.
func newEventSourceConsumer[T StateStore](eventSource *EventSource[T]) (*eventSourceConsumer[T], error) {
	cl := newEosCommitLog(eventSource.source, int(commitLogPartitionsConfig(eventSource.source)))
	var partitionedStore *partitionedChangeLog[T]
	var producerPool *eosProducerPool[T]
	source := eventSource.source

	partitionedStore = newPartitionedChangeLog(eventSource.createChangeLogReceiver, source.ChangeLogTopicName())
	producerPool = newEOSProducerPool[T](source.stateCluster(), cl, 3, 100, 10000, 1)

	sc := &eventSourceConsumer[T]{
		partitionedStore: partitionedStore,
		ctx:              eventSource.runStatus.Ctx(),
		workers:          make(map[TopicPartition]*partitionWorker[T]),
		prepping:         make(map[TopicPartition]*partitionPrepper[T]),
		eventSource:      eventSource,
		commitLog:        cl,
		producerPool:     producerPool,
	}
	balanceStrategies := eventSource.source.BalanceStrategies
	if len(balanceStrategies) == 0 {
		balanceStrategies = DefaultBalanceStrategies
	}
	groupBalancers := toGroupBalancers(sc, balanceStrategies)
	balancerOpt := kgo.Balancers(groupBalancers...)

	for _, gb := range groupBalancers {
		if igr, ok := gb.(IncrementalGroupRebalancer); ok {
			sc.incrBalancer = igr
			break
		}
	}

	client, err := NewClient(
		source.SourceCluster,
		balancerOpt,
		kgo.ConsumerGroup(source.GroupId),
		kgo.ConsumeTopics(source.Topic),
		kgo.OnPartitionsAssigned(sc.partitionsAssigned),
		kgo.OnPartitionsRevoked(sc.partitionsRevoked),
		kgo.RequireStableFetchOffsets(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.SessionTimeout(6*time.Second),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.FetchMaxWait(time.Second),
		kgo.DisableAutoCommit(),
		kgo.AdjustFetchOffsetsFn(sc.adjustOffsetsBeforeAssign),
	)
	sc.client = client
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
			tp := TopicPartition{
				Partition: p,
				Topic:     topic,
			}
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
	if _, ok := sc.prepping[tp]; !ok {
		sc.partitionedStore.assign(tp.Partition)
		sb := newPartitionPrepper(tp, sc.eventSource.source, sc.client, sc.partitionedStore)
		sc.prepping[tp] = sb
		go func() {
			start := time.Now()
			log.Debugf("prepping %+v", tp)
			sb.prepare()
			sb.waitUntilPrepared()
			processed := sb.processed()
			duration := time.Since(start)
			log.Infof("Prepped %+v, %d messages in %v (tps: %d)",
				tp, processed, duration, int(float64(processed)/duration.Seconds()))
			sc.incrBalancer.PartitionPrepared(tp)
		}()
	}
}

// Needed to fulfill the IncrRebalanceInstructionHandler interface defined by IncrementalGroupRebalancer.
// Should NOT be invoked directly.
func (sc *eventSourceConsumer[T]) ForgetPreparedTopicPartition(tp TopicPartition) {
	sc.preppingMux.Lock()
	defer sc.preppingMux.Unlock()
	if sb, ok := sc.prepping[tp]; ok {
		sb.cancel()
	} else {
		// what to do? probably nothing, but if we have a double assignment, we could have problems
		// need to investigate this race condition further
		log.Warnf("ForgetPreparedTopicPartition failed for %+v", tp)
	}
}

func (sc *eventSourceConsumer[T]) assignPartitions(topic string, partitions []int32) {
	validPartitions := make([]int32, 0, len(partitions))
	allPartitions := make([]TopicPartition, 0, len(partitions))
	unprepped := make([]TopicPartition, 0, len(partitions))
	assignedPartitions := make([]TopicPartition, 0, len(partitions))
	for _, p := range partitions {
		allPartitions = append(allPartitions, TopicPartition{Partition: p, Topic: topic})
		sc.partitionedStore.assign(p)
	}
	sc.workerMux.Lock()
	defer sc.workerMux.Unlock()
	sc.preppingMux.Lock()
	defer sc.preppingMux.Unlock()
	for _, tp := range allPartitions {
		if sb, ok := sc.prepping[tp]; ok {
			assignedPartitions = append(assignedPartitions, tp)
			delete(sc.prepping, tp)
			sb.activate()
			store, _ := sc.partitionedStore.getStore(tp.Partition)
			sc.workers[tp] = newPartitionWorker(sc.eventSource, tp, sc.commitLog, store, sc.producerPool, sb.waitUntilActive)
		} else {
			unprepped = append(unprepped, tp)
		}
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	for _, tp := range unprepped {
		if _, ok := sc.workers[tp]; !ok {
			validPartitions = append(validPartitions, tp.Partition)
			assignedPartitions = append(assignedPartitions, tp)
			store, _ := sc.partitionedStore.getStore(tp.Partition)
			sc.workers[tp] = newPartitionWorker(sc.eventSource, tp, sc.commitLog, store, sc.producerPool, wg.Wait)
		}
	}
	go sc.populateChangeLogs(validPartitions, wg)
	sc.incrBalancer.PartitionsAssigned(assignedPartitions...)
}

func (sc *eventSourceConsumer[T]) populateChangeLogs(partitions []int32, wg *sync.WaitGroup) {
	if sc.partitionedStore == nil {
		return
	}
	start := time.Now()
	changeLog := newChangeLogGroupConsumer(sc.eventSource.source, partitions, sc.client, sc.partitionedStore)
	changeLog.start()
	changeLog.activate()
	processed := uint64(0)
	changeLog.waitUntilActive()
	processed += changeLog.processed()
	wg.Done()
	duration := time.Since(start)
	log.Infof("Received %d messages in %v (tps: %d)",
		processed, duration, int(float64(processed)/duration.Seconds()))
}

func (sc *eventSourceConsumer[T]) revokePartitions(topic string, partitions []int32) {
	sc.workerMux.Lock()
	defer sc.workerMux.Unlock()
	if sc.partitionedStore == nil {
		return
	}
	for _, p := range partitions {
		tap := TopicPartition{Partition: p, Topic: topic}
		if worker, ok := sc.workers[tap]; ok {
			worker.revoke()
			delete(sc.workers, tap)
		}
		sc.partitionedStore.revoke(p)
	}
}

func (sc *eventSourceConsumer[T]) partitionsAssigned(ctx context.Context, _ *kgo.Client, assignments map[string][]int32) {
	if len(assignments) > 0 {
		log.Debugf("assigned:%v", assignments)
		for topic, partitions := range assignments {
			sc.assignPartitions(topic, partitions)
		}
	}
}

func (sc *eventSourceConsumer[T]) partitionsRevoked(ctx context.Context, _ *kgo.Client, assignments map[string][]int32) {
	if len(assignments) > 0 {
		log.Debugf("revoked: %v", assignments)
		for topic, partitions := range assignments {
			sc.revokePartitions(topic, partitions)
		}
	}
}

func (sc *eventSourceConsumer[T]) receive(p kgo.FetchTopicPartition) {
	tap := TopicPartition{Partition: p.Partition, Topic: p.Topic}
	sc.workerMux.Lock()
	worker, ok := sc.workers[tap]
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
			log.Infof("client closed for group: %v", sc.eventSource.source.GroupId)
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
func (sc *eventSourceConsumer[T]) interject(tp TopicPartition, cmd Interjector[T], callback func()) {
	sc.workerMux.Lock()
	w := sc.workers[tp]
	sc.workerMux.Unlock()
	if w == nil {
		if callback != nil {
			callback()
		}
		return
	}
	w.interjectionChannel <- &interjection[T]{
		isOneOff:       true,
		topicPartition: w.topicPartition,
		interjector: func(ec *EventContext[T], t time.Time) ExecutionState {
			state := cmd(ec, t)
			if callback != nil {
				callback()
			}
			return state
		},
	}
}

// A convenience function which allows you to Interject into every active partition assigned to the consumer
// without create an individual timer per partition.
// InterjectNow() will be invoked each active partition, blocking on each iteration until the Interjection can be processed.
// Useful for gathering store statistics, but can be used in place of a standard Interjection.
func (sc *eventSourceConsumer[T]) forEachChangeLogPartitionSync(interjector Interjector[T]) {
	sc.workerMux.Lock()
	tps := sak.MapKeysToSlice(sc.workers)
	sc.workerMux.Unlock()

	for _, tp := range tps {
		wg := sync.WaitGroup{}
		wg.Add(1)
		sc.interject(tp, interjector, wg.Done)
		wg.Wait()
	}
}

func (sc *eventSourceConsumer[T]) forEachChangeLogPartitionAsync(interjector Interjector[T]) {
	sc.workerMux.Lock()
	tps := sak.MapKeysToSlice(sc.workers)
	sc.workerMux.Unlock()
	wg := &sync.WaitGroup{}
	wg.Add(len(tps))
	for _, tp := range tps {
		sc.interject(tp, interjector, wg.Done)
	}
	wg.Wait()
}

// TODO: This needs some more work after we provide balancer configuration.
// If the group only has 1 allowed protocol, there is no need for this check.
// If there are multiple, we need to interrogate Kafa to see which is active
func (sc *eventSourceConsumer[T]) currentProtocolIsIncremental() bool {
	adminClient := kadm.NewClient(sc.Client())
	groups, err := adminClient.DescribeGroups(context.Background(), sc.eventSource.source.GroupId)
	if err != nil || len(groups) == 0 {
		log.Errorf("could not confirm group protocol: %v", err)
		return false
	}
	log.Infof("consumerGroup protocol response: %+v", groups)
	group := groups[sc.eventSource.source.GroupId]
	return group.Protocol == IncrementalCoopProtocol
}

// Signals the IncrementalReblancer to start the process of shutting down this consumer in an orderly fashion.
func (sc *eventSourceConsumer[T]) leave() <-chan struct{} {
	log.Infof("leave signaled for group: %v", sc.eventSource.source.GroupId)
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
	log.Infof("left group: %v", sc.eventSource.source.GroupId)
}
