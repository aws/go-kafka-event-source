## Go Kafka Event Source

GKES (Go Kafka Event Source) attempts to fill the gaps ub the Go/Kafka library ecosystem. It supplies EOS (Exactly Once Semantics),
local state stores and incremental consumer rebalancing to Go Kafka consumer applications, making it a viable alternative to
a traditional Kafka Streams application written in Java.

## Getting Started

This repository is organized into several modules. The primary module being "streams".
To use GKES in your go program, simply add it as you would any other Go module.

```
go get github.com/aws/go-kafka-event-source/streams
```

If you are using AWS MSK, you may find the provided "msk" module useful for cluster connectivity, though this is not required.

```
go get github.com/aws/go-kafka-event-source/msk
```

For API documentation on these modules, see https://pkg.go.dev/github.com/aws/go-kafka-event-source/streams and https://pkg.go.dev/github.com/aws/go-kafka-event-source/msk.

## Project Status

GKES is usable in it's current form but there are a few things slated for the very near future. This library is being used by AWS for internal workloads.

- [ ] More comprehensive documentation, specifically around StateStores and async processing - In Progress
- [ ] Per topic partitioner support for EventContext.Forward() and Producer/BatchProducer APIs, Currently, only the Java default murmur2 partitioner is supported - In Progress
- [ ] Instructions for testing locally - In progress
- [ ] More robust functional tests for async processing. Though this is tested extenisively using a local test harness, this process needs to be more repeatable and available to contributors - In progress

## Compatibility Notes

It is recommnded tp use Go v1.19.2 or greater for GKES. There was a known compiler issue in previous versions of Go 1.19 which prevented modules using GKES from compiling. There was a back-port fix made to previous versions of Go, but it probably simpler and safer to update your Go environment to the latest available if you run into this issue.

GKES has been extensively test with Kafka 3.2 but should be fine to use with any Kafka version > 2.5.1. Kafka versions < 2.5.1 are not likely to be compatible with GKES due to transaction semantics, and they have not been tested.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

