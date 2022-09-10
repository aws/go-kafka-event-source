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

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/btree"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const IncrementalCoopProtocol = "incr_coop"

// The IncrementalGroupRebalancer interface is an extension to kgo.GroupBalancer. This balancer allows for slowly moving partions during
// consumer topology changes. This helps reduce blast radius in the case of failures, as well as keep the inherent latency penalty of trasnistioning partitions to a minumum.
type IncrementalGroupRebalancer interface {
	kgo.GroupBalancer
	// Must be called by the InstuctionHandler once a TopicPartition is ready for consumption
	PartitionPrepared(TopicPartition)
	// Must be called by the InstuctionHandler if it fails to prepare a TopicPartition it was previously instructed to prepare
	PartitionPreparationFailed(TopicPartition)
	// Must be called by the InstuctionHandler once it receives an assignment
	PartitionsAssigned(...TopicPartition)
	// Must be called by the InstuctionHandler if it wishes to leave the consumer group in a graceful fashion
	GracefullyLeaveGroup() <-chan struct{}
}

// The status of a consumer group member.
type MemberStatus int

const (
	ActiveMember MemberStatus = iota
	InactiveMember
	Defunct
)

type IncrGroupMemberInstructions struct {
	Prepare []TopicPartition
	Forget  []TopicPartition // not currently used
}

type IncrGroupPartitionState struct {
	Preparing []TopicPartition
	Ready     []TopicPartition
}

type IncrGroupMemberMeta struct {
	Preparing []TopicPartition
	Ready     []TopicPartition
	Status    MemberStatus
	LeftAt    int64
}

func (igmm IncrGroupMemberMeta) isPreparingOrReadyFor(partition int32, topic string) bool {
	return igmm.isReadyFor(partition, topic) || igmm.isPreparingFor(partition, topic)
}

func (igmm IncrGroupMemberMeta) isReadyFor(partition int32, topic string) bool {
	return containsTopicPartition(partition, topic, igmm.Ready)
}

func (igmm IncrGroupMemberMeta) isPreparingFor(partition int32, topic string) bool {
	return containsTopicPartition(partition, topic, igmm.Preparing)
}

func containsTopicPartition(partition int32, topic string, tps []TopicPartition) bool {
	tp := ntp(partition, topic)
	for _, candidate := range tps {
		if candidate == tp {
			return true
		}
	}
	return false
}

type planWrapper struct {
	plan         map[string]map[string][]int32
	instructions map[string]*IncrGroupMemberInstructions
}

type incrGroupMember struct {
	member       *kmsg.JoinGroupResponseMember
	meta         IncrGroupMemberMeta
	assignments  *btree.BTreeG[int32]
	donatable    *btree.BTreeG[int32]
	donating     *btree.BTreeG[int32]
	topic        string
	instructions IncrGroupMemberInstructions
}

func (igm *incrGroupMember) String() string {
	status := "ACTIVE"
	if igm.meta.Status == InactiveMember {
		status = "INACTIVE"
	}
	return fmt.Sprintf(
		"{Member: %v, Status: %s, Assigned:  %v, Donatable: %v, Donating: %v, Preparing: %v, Ready: %v, ToPrepare: %v, SortValue: %v}",
		igm.member.MemberID,
		status,
		igm.assignments.Len(),
		igm.donatable.Len(),
		igm.donating.Len(),
		len(igm.meta.Preparing),
		len(igm.meta.Ready),
		len(igm.instructions.Prepare),
		igm.activeAndPendingAssignments())
}

func (igm *incrGroupMember) activeAndPendingAssignments() int {
	// assignments are by partition and Preparing and Tready by TopicPartition
	// so determine the count by TopicPartition ... hence multiply assignments by len(topics)
	return igm.donatable.Len() + igm.donating.Len() + len(igm.meta.Preparing) + len(igm.meta.Ready)
}

type balanceWrapper struct {
	consumerBalancer *kgo.ConsumerBalancer
}

func (bw balanceWrapper) Balance(topic map[string]int32) kgo.IntoSyncAssignment {
	return bw.consumerBalancer.Balance(topic)
}

func (bw balanceWrapper) BalanceOrError(topic map[string]int32) (kgo.IntoSyncAssignment, error) {
	return bw.consumerBalancer.BalanceOrError(topic)
}

type incrementalBalanceController struct {
	budget             int
	instructionHandler IncrRebalanceInstructionHandler
}

func (ib incrementalBalanceController) Balance(cb *kgo.ConsumerBalancer, topicData map[string]int32) kgo.IntoSyncAssignment {
	start := time.Now()
	defer func() {
		log.Debugf("Balance took %v", time.Since(start))
	}()
	plan := cb.NewPlan()
	instructionsByMemberId := make(map[string]*IncrGroupMemberInstructions)

	for topic, partitionCount := range topicData {
		gs, imbalanced := ib.balanceTopic(cb, plan, partitionCount, topic)
		log.Infof("Group for %s is balanced: %v", topic, !imbalanced)
		for memId, incrMem := range gs.members {
			if instructions, ok := instructionsByMemberId[memId]; ok {
				instructions.Prepare = append(instructions.Prepare, incrMem.instructions.Prepare...)
			} else {
				instructions := new(IncrGroupMemberInstructions)
				*instructions = incrMem.instructions
				instructionsByMemberId[memId] = instructions
			}
		}
	}

	// plan.AdjustCooperative will make the balance a 2 step phase,
	// first revoke any assignments that were moved
	// this will force another rebalance request
	// at which time they will be assigned to the proper receiver
	plan.AdjustCooperative(cb)
	return planWrapper{plan.AsMemberIDMap(), instructionsByMemberId}
}

func (ib incrementalBalanceController) balanceTopic(cb *kgo.ConsumerBalancer, plan *kgo.BalancePlan, partitionCount int32, topic string) (groupState, bool) {

	gs := newGroupState(cb, partitionCount, topic)

	log.Infof("balancing %s, partitions: %d, member state - activeMembers: %d, inactiveMembers: %d",
		topic, partitionCount, gs.activeMembers.Len(), gs.inactiveMembers.Len())

	if gs.activeMembers.Len() == 0 {
		return gs, false
	}

	imbalanced := gs.balance(ib.budget)
	// finally add all our decisions to the balance plan
	gs.inactiveMembers.Ascend(func(mem *incrGroupMember) bool {
		// log.Debugf("assigned for inactive member: %d", items[0].assignments.Len())
		addMemberToPlan(plan, mem)
		return true
	})

	gs.activeMembers.Ascend(func(mem *incrGroupMember) bool {
		addMemberToPlan(plan, mem)
		return true
	})
	return gs, imbalanced
}

func (pw planWrapper) IntoSyncAssignment() []kmsg.SyncGroupRequestGroupAssignment {
	kassignments := make([]kmsg.SyncGroupRequestGroupAssignment, 0, len(pw.plan))
	for member, assignment := range pw.plan {
		var kassignment kmsg.ConsumerMemberAssignment
		instructions := pw.instructions[member]
		instructionBytes, _ := json.Marshal(instructions)
		kassignment.UserData = instructionBytes
		for topic, partitions := range assignment {
			sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
			assnTopic := kmsg.NewConsumerMemberAssignmentTopic()
			assnTopic.Topic = topic
			assnTopic.Partitions = partitions
			kassignment.Topics = append(kassignment.Topics, assnTopic)
		}
		sort.Slice(kassignment.Topics, func(i, j int) bool { return kassignment.Topics[i].Topic < kassignment.Topics[j].Topic })
		syncAssn := kmsg.NewSyncGroupRequestGroupAssignment()
		syncAssn.MemberID = member
		syncAssn.MemberAssignment = kassignment.AppendTo(nil)
		kassignments = append(kassignments, syncAssn)
	}
	sort.Slice(kassignments, func(i, j int) bool { return kassignments[i].MemberID < kassignments[j].MemberID })

	return kassignments
}

func addMemberToPlan(plan *kgo.BalancePlan, mem *incrGroupMember) {
	partitions := make([]int32, 0, mem.assignments.Len())
	mem.assignments.Ascend(func(key int32) bool {
		partitions = append(partitions, key)
		return true
	})
	plan.AddPartitions(mem.member, mem.topic, partitions)
}

type incrementalRebalancer struct {
	balancerController incrementalBalanceController
	quitChan           chan struct{}
	gracefulChan       chan struct{}
	memberStatus       MemberStatus
	leaveTime          int64
	preparing          TopicPartitionSet
	ready              TopicPartitionSet
	instructionHandler IncrRebalanceInstructionHandler
	statusLock         sync.Mutex
}

// Defines the interface needed for the IncrementalGroupRebalancer to function.
// EventSource fulfills this interface. If you are using EventSource, there is nothing else for you to implement.
type IncrRebalanceInstructionHandler interface {
	// Called by the IncrementalGroupRebalancer. Signals the instruction handler that this partition is destined for this consumer.
	// In the case of the EventSource, prepartion involves pre-populating the StateStore for this partition.
	PrepareTopicPartition(tp TopicPartition)
	// Called by the IncrementalGroupRebalancer. Signals the instruction handler that it is safe to forget this previously prepped TopicPartition.
	ForgetPreparedTopicPartition(tp TopicPartition)
	// // Called by the IncrementalGroupRebalancer. A valid *kgo.Client, which is on the same cluster as the Source.Topic, must be returned.
	Client() *kgo.Client
}

// Creates an IncrementalRebalancer suitatble for use by the kgo Kafka driver. In most cases, the instructionHandler is the EventSource.
// `activeTransitions` defines how many partitons may be in receivership at any given point in time.
//
// Example, when `activeTransitions` is 1 and the grpoup stat is imbalanced
// (a new member is added or a member signals it wishes to leave the group), the IncrementalGroupRebalancer will choose 1 partition to move. Once the receiver
// of that partition signals it is ready for the partition, it will assign it, then choose anothe partion to move. This process continues until the group has reached a
// balanced state.
//
// In all cases, any unassigned partitions will be assigned immediately.
// If a consumer host crashes, for example, it's partitions will be assigned immediately, regardless of preparation state.
//
// receivership - the state of being dealt with by an official receiver.
func IncrementalRebalancer(instructionHandler IncrRebalanceInstructionHandler, activeTransitions int) IncrementalGroupRebalancer {
	return &incrementalRebalancer{
		// if we want to increase the number of max in-transition partitions, we need to do some more work.
		// though there will never be an occasion where partitions are left unsassigned, they may get assigned to a
		// consumer that is not prepared for them. once this is debugged, accept `maxIntransition int' as a
		// constructor argument
		balancerController: incrementalBalanceController{
			budget:             activeTransitions,
			instructionHandler: instructionHandler,
		},
		quitChan:           make(chan struct{}, 1),
		gracefulChan:       make(chan struct{}),
		memberStatus:       ActiveMember,
		instructionHandler: instructionHandler,
		preparing:          NewTopicPartitionSet(),
		ready:              NewTopicPartitionSet(),
	}
}

// PartitionPrepared must be called once peparations for a given TopicPartion are complete.
// In the case of the EventSource, it calls this method once it has finished populating the StateStore for the TopicPartition.
func (ir *incrementalRebalancer) PartitionPrepared(tp TopicPartition) {
	ir.statusLock.Lock()
	defer ir.statusLock.Unlock()
	ir.preparing.Remove(tp)
	ir.ready.Insert(tp)
	go ir.client().ForceRebalance()
}

// PartitionPrepared must be called if the IncrRebalanceInstructionHandler was unable to prepare a partition it was previously instructed to do so.
func (ir *incrementalRebalancer) PartitionPreparationFailed(tp TopicPartition) {
	ir.statusLock.Lock()
	defer ir.statusLock.Unlock()
	rebalance := false
	if ir.preparing.Remove(tp) {
		rebalance = true
	}
	if ir.ready.Remove(tp) {
		rebalance = true
	}
	if rebalance {
		go ir.client().ForceRebalance()
	}
}

// PartitionsAssigned must be called after a partition is assigned to the consumer. If you are using SourceConsumr, there is no need to inteact with this methid directly.
func (ir *incrementalRebalancer) PartitionsAssigned(tps ...TopicPartition) {
	ir.statusLock.Lock()
	defer ir.statusLock.Unlock()
	rebalance := false
	for _, tp := range tps {
		if ir.preparing.Remove(tp) {
			rebalance = true
		}
		if ir.ready.Remove(tp) {
			rebalance = true
		}
	}
	if rebalance {
		go ir.client().ForceRebalance()
	}
}

func (ir *incrementalRebalancer) client() *kgo.Client {
	return ir.instructionHandler.Client()
}

// Signals the group that this member wishes to leave. Returns a channel which blocks until all partitions for this member have been reassigned.
func (ir *incrementalRebalancer) GracefullyLeaveGroup() <-chan struct{} {
	ir.statusLock.Lock()
	defer ir.statusLock.Unlock()
	if ir.memberStatus == ActiveMember {
		ir.memberStatus = InactiveMember
		ir.leaveTime = time.Now().UnixMilli()
		time.AfterFunc(time.Second, ir.client().ForceRebalance)
	}
	return ir.gracefulChan
}

// Needed to fulfill the kgo.GroupBalancer interface. There should be non need to interact with this directly.
func (ir *incrementalRebalancer) IsCooperative() bool {
	return true
}

// Needed to fulfill the kgo.GroupBalancer interface. There should be non need to interact with this directly.
func (ir *incrementalRebalancer) ProtocolName() string {
	return IncrementalCoopProtocol
}

// Needed to fulfill the kgo.GroupBalancer interface. There should be no need to interact with this directly,
// though we are hijacking this method to parse balance instructions like 'Prepare' and 'Forget'
func (ir *incrementalRebalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	cma := new(kmsg.ConsumerMemberAssignment)
	err := cma.ReadFrom(assignment)
	if err != nil {
		return nil, err
	}
	var instructions IncrGroupMemberInstructions
	json.Unmarshal(cma.UserData, &instructions)

	parsed := make(map[string][]int32, len(cma.Topics))
	for _, topic := range cma.Topics {
		parsed[topic.Topic] = topic.Partitions
	}

	ir.statusLock.Lock()
	defer ir.statusLock.Unlock()
	if ir.memberStatus == InactiveMember {
		shouldClose := true

		for _, assignments := range parsed {
			if len(assignments) > 0 {
				shouldClose = false
				break
			}
		}
		if shouldClose {
			ir.memberStatus = Defunct
			close(ir.gracefulChan)
		}
	}

	for _, tp := range instructions.Prepare {
		if ir.preparing.Insert(tp) {
			go ir.instructionHandler.PrepareTopicPartition(tp)
		}
	}
	for _, tp := range instructions.Forget {
		prepping := ir.preparing.Remove(tp)
		prepped := ir.ready.Remove(tp)
		if prepping || prepped {
			go ir.instructionHandler.ForgetPreparedTopicPartition(tp)
		}
	}

	return parsed, err
}

// Needed to fulfill the kgo.GroupBalancer interface. There should be non need to interact with this directly.
func (ir *incrementalRebalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (kgo.GroupMemberBalancer, map[string]struct{}, error) {
	cb, err := kgo.NewConsumerBalancer(ir.balancerController, members)
	return balanceWrapper{consumerBalancer: cb}, cb.MemberTopics(), err
}

// Needed to fulfill the kgo.GroupBalancer interface. There should be non need to interact with this directly.
// We'll use the same meta data format as kgo itself (using copy-and-paste technology) and user The supplied UserData foeld to provide IncrementalRebalancer
// specific data. This should allow us to be compatible with the coop_sticky that is already supplied by kgo.
func (ir *incrementalRebalancer) JoinGroupMetadata(interests []string, currentAssignment map[string][]int32, generation int32) []byte {
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Topics = interests
	meta.Version = 1
	for topic, partitions := range currentAssignment {
		metaPart := kmsg.NewConsumerMemberMetadataOwnedPartition()
		metaPart.Topic = topic
		metaPart.Partitions = partitions
		meta.OwnedPartitions = append(meta.OwnedPartitions, metaPart)
	}
	// KAFKA-12898: ensure our topics are sorted
	metaOwned := meta.OwnedPartitions
	sort.Slice(metaOwned, func(i, j int) bool { return metaOwned[i].Topic < metaOwned[j].Topic })
	meta.UserData = ir.userData()
	return meta.AppendTo(nil)
}

func (ir *incrementalRebalancer) userData() []byte {
	ir.statusLock.Lock()
	defer ir.statusLock.Unlock()
	data, _ := json.Marshal(IncrGroupMemberMeta{
		Status:    ir.memberStatus,
		LeftAt:    ir.leaveTime,
		Preparing: ir.preparing.Items(),
		Ready:     ir.ready.Items(),
	})

	return data
}
