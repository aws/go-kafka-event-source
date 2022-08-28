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
