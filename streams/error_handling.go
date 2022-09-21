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
	// Instructs GKES to mark the event in error state as complete and continue processing as normal.
	CompleteAndContinue ErrorResponse = iota
	// Instructs GKES to immediately stop processing the partition in error. If using an IncermentalBalancer, this partition will go into a `Fail`
	// mode and be reassigned to another consumer.
	FailPartition

	// Instructs GKES to immediately stop processing the partition in error. Also instructs the consumer to leave the group.
	// If using an IncermentalBalancer, the leave will be graceful. Tgis stratgey is recommnded for identifying bad deployments. The idea would be to capture
	// this event via metrics and Alarm, causing a graceful rollback.
	FailConsumer

	// As the name implies, the application will fatally exit.
	FatallyExit
)

type ErrorContext interface {
	TopicPartition() TopicPartition
	Offset() int64
	Input() (IncomingRecord, bool)
}

type DeserializationErrorHandler func(ec ErrorContext, eventType string, err error) ErrorResponse
type EosErrorHandler func(topicPartition TopicPartition, err error) ErrorResponse

func DefaultDeserializationErrorHandler(ec ErrorContext, eventType string, err error) ErrorResponse {
	log.Errorf("failed to deserialize record for %+v, offset: %d, eventType: %s,error: %v", ec.TopicPartition(), ec.Offset(), eventType, err)
	return CompleteAndContinue
}

func DefaultEosErrorHandler(topicPartition TopicPartition, err error) ErrorResponse {
	log.Errorf("failing consumer due to eos failure in %+v, error: %v", topicPartition, err)
	return FailConsumer
}
