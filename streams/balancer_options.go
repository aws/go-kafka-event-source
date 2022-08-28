// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

type BalanceType int

type BalanceStrategy struct {
	BalanceType   string
	BalanceOption int
}

func toGroupBalancers(instructionHandler IncrRebalanceInstructionHandler, rs []BalanceStrategy) []kgo.GroupBalancer {
	balancers := []kgo.GroupBalancer{}
	for _, balancer := range rs {
		switch balancer.BalanceType {
		case RangeBalanceType:
			balancers = append(balancers, kgo.RangeBalancer())
		case RoundRobinBalanceType:
			balancers = append(balancers, kgo.RoundRobinBalancer())
		case CooperativeStickyBalanceType:
			balancers = append(balancers, kgo.CooperativeStickyBalancer())
		case IncrementalBalanceType:
			balancers = append(balancers, IncrementalRebalancer(instructionHandler, balancer.BalanceOption))
		}
	}
	return balancers
}

const (
	RangeBalanceType             = "range"
	RoundRobinBalanceType        = "round_robin"
	CooperativeStickyBalanceType = "sticky"
	IncrementalBalanceType       = "incr_coop"
)

var (
	RangeBalancer             = BalanceStrategy{BalanceType: RangeBalanceType}
	RoundRobinBalancer        = BalanceStrategy{BalanceType: RoundRobinBalanceType}
	CooperativeStickyBalancer = BalanceStrategy{BalanceType: CooperativeStickyBalanceType}
	IncrementalBalancer       = BalanceStrategy{BalanceType: IncrementalBalanceType, BalanceOption: 1}
)

var DefaultBalanceStrategies = []BalanceStrategy{IncrementalBalancer, RangeBalancer}
