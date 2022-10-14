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

/*
GKES (Go Kafka Event Source) attempts to fill the gaps ub the Go/Kafka library ecosystem. It supplies Exactly Once Semantics (EOS),
local state stores and incremental consumer rebalancing to Go Kafka consumers, making it a viable alternative to
a traditional Kafka Streams application written in Java.

# What it is

GKES is Go/Kafka library tailored towards the development of
[Event Sourcing applications], by providing a high-throughput, low-latency
Kafka client framework. Using Kafka transactions, it provides for EOS, data integrity and high availability.
If you wish to use GKES as straight Kafka consumer, it will fit the bill as well. Though there are plenty of libraries for that,
and researching which best fits your use case is time well spent.

GKES is not an all-in-one, do-everything black box. Some elements, in particular the StateStore, have been left without
comprehensive implementations.

# StateStores

A useful and performant local state store rarely has a flat data structure. If your state store does, there are some
convenient implementations provided. However, to achieve optimum performance, you will not only need to write a StateStore implementation,
but will also need to understand what the proper data structures are for your use case (trees, heaps, maps, disk-based LSM trees or combinations thereof).
You can use the provided [github.com/aws/go-kafka-event-source/streams/stores.SimpleStore] as a starting point.

# Vending State

GKES purposefully does not provide a pre-canned way for exposing StateStore data, other than a producing to another Kafka topic.
There are as many ways to vend data as there are web applications. Rather than putting effort into inventing yet another one,
GKES provides the mechanisms to query StateStores via Interjections. This mechanism can be plugged into whatever request/response mechanism that suits your use-case
(gRPC, RESTful HTTP service...any number of web frameworks already in the Go ecosystem).
[TODO: provide a simple http example]

# Interjections

For this familiar with thw Kafka Streams API, GKES provides for stream `Punctuators“, but we call them `Interjections` (because it sounds cool).
Interjections allow you to insert actions into your EventSource at specicifed interval per partition assigned via [streams.EventSource.ScheduleInterjection],
or at any time via [streams.EventSource.Interject]. This is useful for bookeeping activities, aggregated metric production or
even error handling. Interjections have full access to the StateStore associated with an EventSource and can interact with output topics
like any other EventProcessor.

# Incremental Consumer Rebalancing

One issue that Kafka conumer applications have long suffered from are latency spikes during a consumer rebalance. The cooperative sticky rebalancing introduced by Kafka and implemented
by [kgo] helps resolve this issue. However, once StateStore are thrown into the mix, things get a bit more complicated because initializing the StateStore on a host
invloves consuming a compacted TopicPartion from start to end. GKES solves this with the [IncrementalRebalancer] and takes it one step further.
The [IncrementalRebalancer] rebalances consumer partitions in a controlled fashion, minimizing latency spikes and limiting the blast of a bad deployment.

# Async Processing

GKES provides conventions for asynchronously processing events on the same Kafka partition while still maintaining data/stream integrity.
The [AsyncBatcher] and [AsyncJobScheduler] allow you to split a TopicPartition into sub-streams by key,
ensuring all events for a partitcular key are processed in order, allowing for parallel processing on a given TopicPartition.

For more details, see [Async Processing Examples]

# High-Throughput/Low-Latency EOS

A Kafka transaction is a powerful tool which allows for Exactly Once Semantics (EOS) by linking a consumer offset commit to one or more records
that are being produced by your application (a StateStore record for example). The history of Kafka EOS is a long and complicated one with varied degrees of performance and efficiency.

Early iterations required one producer transaction per consumer partition, which was very ineffiecient as Topic with 1000 partitions would also require 1000 clients in order to provide EOS.
This has since been addressed, but depending on client implementations, there is a high risk of running into "producer fenced" errors as well as reduced throughput.

In a traditional Java Kafka Streams application, transactions are committed according to the auto-commit frequency, which defaults to 100ms. This means that your application will only produce
readable records every 100ms per partition. The effect of this is that no matter what you do, your tail latency will be at least 100ms and downstream consumers will receive records in bursts
rather than a steady stream. For many use cases, this is unaceptable.

GKES solves this issue by using a configurable transactional producer pool and a type of "Nagle's algorithm". Uncommitted offsets are added to the transaction pool in sequence.
Once a producer has reach its record limit, or enough time has elapsed (10ms by default), the head transaction will wait for any incomplete events to finsh, then flush and commit.
While this transaction is committing, GKES continues to process events and optimistically begins a new transaction and produces records on the next
producer in pool. Since trasnaction produce in sequence, there is no danger of commit offset overlap or duplicate message processing in the case of a failure.

To ensure EOS, your [EventSource] must use either the [IncrementalRebalancer], or [kgo]s cooperative sticky implementation. Though if you're using a StateStore, [IncrementalRebalancer]
should be used to avoid lengthy periods of inactivity during application deployments.

# Kafka Client Library

Rather than create yet another Kafka driver, GKES is built on top of [kgo]. This Kafka client was chosen
as it (in our testing) has superior throughput and latency profiles compared to other client libraries currently available to Go developers.

One other key adavantage is that it provides a migration path to cooperative consumer rebalancing, required for our EOS implementation. Other Go Kafka libraries provide cooperative rebalancing,
but do not allow you to migrate froma non-cooperative rebalancing strategy (range, sticky etc.). This is a major roadblock for existing deployemtns as the
only migration paths are an entirely new consumer group, or to bring your application completely down and re-deploy with a new rebalance strategy.
These migration plans, to put it mildly, are big challenge for zero-downtime/live applications.
The [kgo] package now makes this migration possible with zero downtime.

Kgo also has the proper hooks need to implement the [IncrementalGroupRebalancer], which is necessary for safe deployments when using a local state store.
Kudos to [kgo]!

[Event Sourcing applications]: https://martinfowler.com/eaaDev/EventSourcing.html
[kgo]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo
[Async Processing Examples]: https://github.com/aws/go-kafka-event-source/blame/main/docs/asyncprocessing.md
*/
package streams
