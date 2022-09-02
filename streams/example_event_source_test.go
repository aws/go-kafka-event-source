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
	"time"

	"github.com/aws/go-kafka-event-source/streams"
	"github.com/aws/go-kafka-event-source/streams/stores"
)

type Contact struct {
	Id          string
	PhoneNumber string
	Email       string
	FirstName   string
	LastName    string
	LastContact time.Time
}

type NotifyContactEvent struct {
	ContactId        string
	NotificationType string
}

type EmailNotification struct {
	ContactId string
	Address   string
	Payload   string
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

var notificationScheduler *streams.AsyncJobScheduler[ContactStore, string, EmailNotification]

func notifyContactAsync(ctx *streams.EventContext[ContactStore], notification NotifyContactEvent) (streams.ExecutionState, error) {
	contactStore := ctx.Store()
	defer wg.Done()

	if contact, ok := contactStore.Get(notification.ContactId); ok {
		fmt.Printf("Notifying contact: %s asynchronously by %s\n", contact.Id, notification.NotificationType)
		return notificationScheduler.Schedule(ctx, contact.Email, EmailNotification{
			ContactId: contact.Id,
			Address:   contact.Email,
			Payload:   "sending you mail...from a computer!",
		})
	} else {
		fmt.Printf("Contact %s does not exist!\n", notification.ContactId)
	}
	return streams.Complete, nil
}

func sendEmailToContact(key string, notification EmailNotification) error {
	// note: the AsyncJobProcessor does not have access to the StateStore
	fmt.Printf("Processing an email job with key: '%s'. This may take some time, emails are tricky!\n", key)
	time.Sleep(500 * time.Millisecond) // simulating how long it might to send an email
	return nil
}

func emailToContactComplete(ctx *streams.EventContext[ContactStore], _ string, email EmailNotification, err error) (streams.ExecutionState, error) {
	// the AsyncJobFinalizer has access to the StateStore associated with this event
	contactStore := ctx.Store()
	if contact, ok := contactStore.Get(email.ContactId); ok {
		fmt.Printf("Notified contact: %s, address: %s, payload: '%s'\n", contact.Id, email.Address, email.Payload)
		contact.LastContact = time.Now()
		contactStore.Put(contact)
	}
	wg.Done()
	return streams.Complete, err
}

func ExampleEventSource() {
	streams.InitLogger(streams.SimpleLogger(streams.LogLevelError), streams.LogLevelError)

	var contactsCluster = streams.SimpleCluster([]string{"127.0.0.1:9092"})
	var source = streams.Source{
		GroupId:       "ExampleEventSourceGroup",
		Topic:         "ExampleEventSource",
		NumPartitions: 10,
		SourceCluster: contactsCluster,
	}

	var destination = streams.Destination{
		Cluster:      source.SourceCluster,
		DefaultTopic: source.Topic,
	}

	source, err := streams.CreateSource(source)
	if err != nil {
		panic(err)
	}

	eventSource, err := streams.NewEventSource(source, NewContactStore, nil)
	if err != nil {
		panic(err)
	}

	streams.RegisterEventType(eventSource, streams.JsonItemDecoder[Contact], createContact, "CreateContact")
	streams.RegisterEventType(eventSource, streams.JsonItemDecoder[Contact], deleteContact, "DeleteContact")
	streams.RegisterEventType(eventSource, streams.JsonItemDecoder[NotifyContactEvent], notifyContact, "NotifyContact")

	wg.Add(4) // we're expecting 4 records in this example
	eventSource.ConsumeEvents()

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

	createContactRecord := streams.JsonItemEncoder("CreateContact", contact)
	createContactRecord.WriteKeyString(contact.Id)

	deleteContactRecord := streams.JsonItemEncoder("DeleteContact", contact)
	deleteContactRecord.WriteKeyString(contact.Id)

	notificationRecord := streams.JsonItemEncoder("NotifyContact", notification)
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

func ExampleAsyncJobScheduler() {
	streams.InitLogger(streams.SimpleLogger(streams.LogLevelError), streams.LogLevelError)

	var contactsCluster = streams.SimpleCluster([]string{"127.0.0.1:9092"})
	var source = streams.Source{
		GroupId:       "ExampleAsyncJobSchedulerGroup",
		Topic:         "ExampleAsyncJobScheduler",
		NumPartitions: 10,
		SourceCluster: contactsCluster,
	}

	var destination = streams.Destination{
		Cluster:      source.SourceCluster,
		DefaultTopic: source.Topic,
	}

	source, err := streams.CreateSource(source)
	if err != nil {
		panic(err)
	}

	eventSource, err := streams.NewEventSource(source, NewContactStore, nil)
	if err != nil {
		panic(err)
	}

	streams.RegisterEventType(eventSource, streams.JsonItemDecoder[Contact], createContact, "CreateContact")
	streams.RegisterEventType(eventSource, streams.JsonItemDecoder[NotifyContactEvent], notifyContactAsync, "NotifyContact")

	notificationScheduler, err = streams.CreateAsyncJobScheduler(eventSource,
		sendEmailToContact, emailToContactComplete, streams.DefaultConfig)
	if err != nil {
		panic(err)
	}
	wg.Add(3) // we're expecting 3 records in this example
	eventSource.ConsumeEvents()

	contact := Contact{
		Id:          "123",
		Email:       "billy@bob.com",
		PhoneNumber: "+18005551212",
		FirstName:   "Billy",
		LastName:    "Bob",
	}

	notification := NotifyContactEvent{
		ContactId:        "123",
		NotificationType: "email",
	}

	producer := streams.NewProducer(destination)

	createContactRecord := streams.JsonItemEncoder("CreateContact", contact)
	createContactRecord.WriteKeyString(contact.Id)

	notificationRecord := streams.JsonItemEncoder("NotifyContact", notification)
	notificationRecord.WriteKeyString(notification.ContactId)

	producer.Produce(context.Background(), createContactRecord)
	producer.Produce(context.Background(), notificationRecord)

	wg.Wait()
	eventSource.Stop()
	<-eventSource.Done()
	// cleaning up our local Kafka cluster
	// you probably don't want to delete your topic
	streams.DeleteSource(source)
	// Output: Created contact: 123
	// Notifying contact: 123 asynchronously by email
	// Processing an email job with key: 'billy@bob.com'. This may take some time, emails are tricky!
	// Notified contact: 123, address: billy@bob.com, payload: 'sending you mail...from a computer!'
}
