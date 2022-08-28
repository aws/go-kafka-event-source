package streams

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type partitionPrepper[T StateStore] struct {
	topicPartition   TopicPartition
	producer         *kgo.Client
	partitionedStore *partitionedChangeLog[T]
	changeLog        *changeLogGroupConsumer[T]
	readyWaitGroup   sync.WaitGroup
	activeWaitGroup  sync.WaitGroup
	source           Source
}

func newPartitionPrepper[T StateStore](tp TopicPartition, source Source, producer *kgo.Client,
	partitionedStore *partitionedChangeLog[T]) *partitionPrepper[T] {

	sp := &partitionPrepper[T]{
		topicPartition:   tp,
		producer:         producer,
		source:           source,
		partitionedStore: partitionedStore,
	}
	sp.readyWaitGroup.Add(1)
	sp.activeWaitGroup.Add(1)
	return sp
}

func (sp *partitionPrepper[T]) prepare() {
	partitions := []int32{sp.topicPartition.Partition}
	sp.changeLog = newChangeLogGroupConsumer(sp.source, partitions, sp.producer, sp.partitionedStore)
	go sp.changeLog.start()
	sp.changeLog.prepare()
}

func (sp *partitionPrepper[T]) processed() uint64 {
	return sp.changeLog.processed()
}

func (sp *partitionPrepper[T]) cancel() {
	sp.changeLog.cancel()
}

func (sp *partitionPrepper[T]) activate() {
	go sp.changeLog.activate()
}

func (sp *partitionPrepper[T]) waitUntilPrepared() {
	sp.changeLog.waitUntilPrepared()
}

func (sp *partitionPrepper[T]) waitUntilActive() {
	sp.changeLog.waitUntilActive()
}
