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
GKES (Go Kafka Event Source) attempts to fill the gaps ub the Go/Kafka library ecosystem. It supplies EOS (Exactly Once Semantics),
local state stores and incremental consumer rebalancing to Go Kafka consumers, making it a viable alternative to
a traditional Kafka Streams application written in Java.

GKES takes advantage of Go generics. As such, thie minimum requred Go version is 1.18.

# What it is

GKES is Go/Kafka library tailored towards the development of
[Event Sourcing applications], by providing a high-throughput, low-latency
Kafka client framework. Using Kafka transactions, it provides for EOS, data integrity and high availability.
If you wish to use GKES as straight Kafka consumer, it will fit the bill as well. Though there are plenty of libraries for that,
and researching which best fits your use case is time well spent.

# What it is not

GKES is not an all-in-one, do-everything black box. Some elements, in particular the StateStore, have been left without
comprehensive implementations.

# StateStores

A useful and performant local state store rarely has a flat data structure. If your state store does, there are some
convenient implementations provided. However, to achieve optimum performance, you will not only need to write a StateStore implementation,
but will also need to understand what the proper data structures are for your use case (trees, heaps, maps, disk-based LSM trees or combinations thereof).
You can use the provided [github.com/aws/go-kafka-event-source/streams/stores.SimpleStore] as a starting point.

# Exposing State

[TODO]

# Incremental Consumer Rebalancing

[TODO]

# Interjections

For this familiar with thw Kafka Streams API, GKES provides for stream Punctuators, but we call them `Interjections` (because it sounds cool).
Interjections allow you to insert actions into your EventSource at specicifed interval per partition assigned via [streams.EventSource.ScheduleInterjection],
or at any time via [streams.EventSource.Interject]. This is useful for bookeeping activities, aggregated metric production or
even error handling. Interjections have full access to the StateStore associated with an EventSource and can interact with output topics
like any other EventProcessor.

# EOS GKES Style

[TODO]

# Kafka Client Library

Rather than create yet another Kafka driver, GKES is built on top of [kgo]. This Kafka client was chosen
as it has superior throughput and latency profiles compared to other client libraries currently available to Go developers.

One other key adavantage is that it provides a migration path to cooperative consumer rebalancing. Other kafka libraries provide cooperative rebalancing,
but do not allow you to migrate froma non-cooperative rebalancing strategy (range, sticky etc.). This is a major roadblock for existing deployemtns as the
only migration path is an entirely new consumer group. Which is, to put it mildly, a big challenge for live applications which are allowed zero downtime.
The kgo package now makes this possible.

Kgo also has the proper hooks need to implement the [streams.IncrementalGroupRebalancer], which is necessary for safe deployments when using a local state store.

[Event Sourcing applications]: https://martinfowler.com/eaaDev/EventSourcing.html
[kgo]: https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo
*/
package streams
