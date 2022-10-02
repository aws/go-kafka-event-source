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

import "github.com/twmb/franz-go/pkg/kgo"

type BalanceStrategy int

func toGroupBalancers(instructionHandler IncrRebalanceInstructionHandler, rs []BalanceStrategy) []kgo.GroupBalancer {
	balancers := []kgo.GroupBalancer{}
	for _, balancer := range rs {
		switch balancer {
		case RangeBalanceStrategy:
			balancers = append(balancers, kgo.RangeBalancer())
		case RoundRobinBalanceStrategy:
			balancers = append(balancers, kgo.RoundRobinBalancer())
		case CooperativeStickyBalanceStrategy:
			balancers = append(balancers, kgo.CooperativeStickyBalancer())
		case IncrementalBalanceStrategy:
			balancers = append(balancers, IncrementalRebalancer(instructionHandler))
		}
	}
	return balancers
}

const (
	RangeBalanceStrategy             BalanceStrategy = 0
	RoundRobinBalanceStrategy        BalanceStrategy = 1
	CooperativeStickyBalanceStrategy BalanceStrategy = 2
	IncrementalBalanceStrategy       BalanceStrategy = 3
)

var DefaultBalanceStrategies = []BalanceStrategy{IncrementalBalanceStrategy}
