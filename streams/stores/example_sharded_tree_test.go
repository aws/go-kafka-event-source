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

package stores_test

import (
	"fmt"
	"strings"

	"github.com/aws/go-kafka-event-source/streams/stores"
)

// This example creates a tree with 32 shards (2<<4).
// Each tree shard will be sorted by LastName, FirstName in ascending order.
func ExampleShardedTree_contact() {
	type Contact struct {
		PhoneNumber string
		FirstName   string
		LastName    string
	}

	contactSort := func(a, b Contact) bool {
		res := strings.Compare(a.LastName, b.LastName)
		if res != 0 {
			return res < 0
		}
		return a.FirstName < b.FirstName
	}

	shardedTree := stores.NewShardedTree(4, stores.StringHash, contactSort)
	contact := Contact{
		PhoneNumber: "+18005551213",
		FirstName:   "Billy",
		LastName:    "Bob",
	}

	tree := shardedTree.For(contact.LastName)
	tree.ReplaceOrInsert(contact)

	contact.PhoneNumber = "+18005551212"

	if oldContact, updated := tree.ReplaceOrInsert(contact); updated {
		fmt.Printf("PhoneNumber update from %s to %s\n", oldContact.PhoneNumber, contact.PhoneNumber)
	}
	// Output: PhoneNumber updated from +18005551213 to +18005551212
}

func ExampleShardedTree_string() {
	shardedTree := stores.NewShardedTree(4, stores.StringHash, stores.StringLess)
	partionKey := "Foo"
	item := "Bar"
	shardedTree.For(partionKey).ReplaceOrInsert(item)
}

func ExampleShardedTree_number() {
	shardedTree := stores.NewShardedTree(4, stores.StringHash, stores.NumberLess[int])
	partionKey := "Foo"
	value := 1000
	shardedTree.For(partionKey).ReplaceOrInsert(value)
}
