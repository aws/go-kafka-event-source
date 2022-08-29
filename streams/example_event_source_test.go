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

package streams_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/go-kafka-event-source/streams"
	"github.com/aws/go-kafka-event-source/streams/codec"
	"github.com/aws/go-kafka-event-source/streams/stores"
)

type Contact struct {
	Id          string
	PhoneNumber string
	Email       string
	FirstName   string
	LastName    string
}

type NotifyContactEvent struct {
	ContactId        string
	NotificationType string
}

func (c Contact) Key() string {
	return c.Id
}

// setting up a wait group to shut down the consumer once our example is finished
var wg = &sync.WaitGroup{}

func createContact(ctx *streams.EventContext[ContactStore], contact Contact) (streams.ExecutionState, error) {
	contactStore := ctx.Store()
	ctx.RecordChange(contactStore.Put(contact))
	fmt.Printf("Created contact: %s\n", contact.Id)
	wg.Done()
	return streams.Complete, nil
}

func deleteContact(ctx *streams.EventContext[ContactStore], contact Contact) (streams.ExecutionState, error) {
	contactStore := ctx.Store()
	if entry, ok := contactStore.Delete(contact); ok {
		ctx.RecordChange(entry)
		fmt.Printf("Deleted contact: %s\n", contact.Id)
	}
	wg.Done()
	return streams.Complete, nil
}

func notifyContact(ctx *streams.EventContext[ContactStore], notification NotifyContactEvent) (streams.ExecutionState, error) {
	contactStore := ctx.Store()
	if contact, ok := contactStore.Get(notification.ContactId); ok {
		fmt.Printf("Notifying contact: %s by %s\n", contact.Id, notification.NotificationType)
	} else {
		fmt.Printf("Contact %s does not exist!\n", notification.ContactId)
	}
	wg.Done()
	return streams.Complete, nil
}

// simply providing an example of how you might wrap the store into your own type
type ContactStore struct {
	*stores.SimpleStore[Contact]
}

func NewContactStore(tp streams.TopicPartition) ContactStore {
	return ContactStore{stores.NewSimpleStore[Contact](tp)}
}

var contactsCluster = streams.SimpleCluster([]string{"127.0.0.1:9092"})
var source = streams.Source{
	GroupId:       "ContactsExampleGroup",
	Topic:         "ContactsExample",
	NumPartitions: 10,
	SourceCluster: contactsCluster,
}

var destination = streams.Destination{
	Cluster:      source.SourceCluster,
	DefaultTopic: source.Topic,
}

func ExampleEventSource() {
	streams.InitLogger(streams.SimpleLogger(streams.LogLevelError), streams.LogLevelError)

	source, err := streams.CreateSource(source)
	if err != nil {
		panic(err)
	}

	eventSource, err := streams.NewEventSource(source, NewContactStore, nil)
	if err != nil {
		panic(err)
	}

	streams.RegisterEventType(eventSource, codec.JsonItemDecoder[Contact], createContact, "CreateContact")
	streams.RegisterEventType(eventSource, codec.JsonItemDecoder[Contact], deleteContact, "DeleteContact")
	streams.RegisterEventType(eventSource, codec.JsonItemDecoder[NotifyContactEvent], notifyContact, "NotifyContact")

	eventSource.ConsumeEvents()

	wg.Add(4) // we're expecting 4 records in this example

	contact := Contact{
		Id:          "123",
		PhoneNumber: "+18005551212",
		FirstName:   "Billy",
		LastName:    "Bob",
	}

	notification := NotifyContactEvent{
		ContactId:        "123",
		NotificationType: "email",
	}

	producer := streams.NewProducer(destination)

	createContactRecord := codec.JsonItemEncoder("CreateContact", contact)
	createContactRecord.WriteKeyString(contact.Id)

	deleteContactRecord := codec.JsonItemEncoder("DeleteContact", contact)
	deleteContactRecord.WriteKeyString(contact.Id)

	notificationRecord := codec.JsonItemEncoder("NotifyContact", notification)
	notificationRecord.WriteKeyString(notification.ContactId)

	producer.Produce(context.Background(), createContactRecord)
	producer.Produce(context.Background(), notificationRecord)
	producer.Produce(context.Background(), deleteContactRecord)
	producer.Produce(context.Background(), notificationRecord)

	wg.Wait()
	eventSource.Stop()
	<-eventSource.Done()
	// cleaning up our local Kafka cluster
	// you probably don't want to delete your topic
	streams.DeleteSource(source)
	// Output: Created contact: 123
	// Notifying contact: 123 by email
	// Deleted contact: 123
	// Contact 123 does not exist!
}
