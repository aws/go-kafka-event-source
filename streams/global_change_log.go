package streams

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A GlobalChangeLog is simply a consumer which continously consumes all partitions within the given topic and
// forwards all records to it's StateStore. GlobalChangeLogs can be useful for sharing small amounts of data between
// a group of hosts. For example, gstreams uses a global change log to keep track of consumer group offsets.
type GlobalChangeLog struct {
	receiver StateStore
	client   *kgo.Client
}

// Creates a NewGlobalChangeLog consumer and forward all records to `receiver`.
func NewGlobalChangeLog(cluster Cluster, receiver StateStore, numPartitions int, topic string) GlobalChangeLog {
	assignments := make(map[int32]kgo.Offset)
	for i := 0; i < numPartitions; i++ {
		assignments[int32(i)] = kgo.NewOffset().AtStart()
	}
	client, err := NewClient(
		cluster,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: assignments,
		}),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	if err != nil {
		panic(err)
	}

	return GlobalChangeLog{
		client:   client,
		receiver: receiver,
	}
}

func (cl GlobalChangeLog) Stop() {
	cl.client.Close()
}

func (cl GlobalChangeLog) Start() {
	go cl.consume()
}

func (cl GlobalChangeLog) consume() {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		f := cl.client.PollFetches(ctx)
		cancel()
		if f.IsClientClosed() {
			log.Debugf("client closed")
			return
		}
		for _, err := range f.Errors() {
			if err.Err != ctx.Err() {
				log.Errorf("%v", err)
			}
		}
		f.EachRecord(cl.forwardChange)
	}
}

func (cl GlobalChangeLog) forwardChange(r *kgo.Record) {
	cl.receiver.ReceiveChange(newIncomingRecord(r))
}
