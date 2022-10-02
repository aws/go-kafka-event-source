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

// in structs GKES and how to proceed when an error is encountered.
type ErrorResponse int

const (
	// Instructs GKES to ignore any error stateand continue processing as normal. If this is used in response to
	// Kafka transaction error, there will likely be data loss or corruption. This ErrorResponse is not recommended as it is unlikely that
	// a consumer will be able to recover gracefully from a transaction error. In almost all situations, FailConsumer is preferred.
	Continue ErrorResponse = iota

	// Instructs GKES to immediately stop processing and the consumer to immediately leave the group.
	// This is preferable to a FatallyExit as Kafka will immediatly recognize the consumer as exiting the group
	// (if there is still comminication with the cluster) and processing of the
	// failed partitions will begin without waiting for the session timeout value.
	FailConsumer

	// As the name implies, the application will fatally exit. The partitions owned by this consumer will not be reassigned until the configured
	// session timeout on the broker.
	FatallyExit
)

type ErrorContext interface {
	TopicPartition() TopicPartition
	Offset() int64
	Input() (IncomingRecord, bool)
}

type DeserializationErrorHandler func(ec ErrorContext, eventType string, err error) ErrorResponse
type TxnErrorHandler func(err error) ErrorResponse

func DefaultDeserializationErrorHandler(ec ErrorContext, eventType string, err error) ErrorResponse {
	log.Errorf("failed to deserialize record for %+v, offset: %d, eventType: %s,error: %v", ec.TopicPartition(), ec.Offset(), eventType, err)
	return Continue
}

func DefaultTxnErrorHandler(err error) ErrorResponse {
	log.Errorf("failing consumer due to eos txn error: %v", err)
	return FatallyExit
}
