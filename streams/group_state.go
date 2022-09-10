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
	"math/rand"

	"github.com/google/btree"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// sort function for inactiveMembers btree
// by sorting by leftAt, we drain one host at a time
func incrGroupInactiveLess(a, b *incrGroupMember) bool {
	res := a.meta.LeftAt - b.meta.LeftAt
	if res != 0 {
		return res < 0
	}
	return a.member.MemberID < b.member.MemberID
}

// sort function for activeMembers btree
func incrGroupActiveLess(a, b *incrGroupMember) bool {
	res := a.activeAndPendingAssignments() - b.activeAndPendingAssignments()
	if res != 0 {
		return res < 0
	}

	// we likely have partitions in receivership, so we have less priority
	// meaning we are les likely to donate a partition
	res = (a.donatable.Len() + a.donating.Len()) - (b.donatable.Len() + b.donating.Len())
	if res != 0 {
		return res < 0
	}
	return a.member.MemberID < b.member.MemberID
}

// withoutPartition - make our topic partition list act like a set without the headache of converting
// deserialized array into a set and vice versa
// the `n` is small here, so it's not a performance issue
func withoutPartition(tps []TopicPartition, partition int32) []TopicPartition {
	newTps := make([]TopicPartition, 0, len(tps))
	for _, tp := range tps {
		if tp.Partition != partition {
			newTps = append(newTps, tp)
		}
	}
	return newTps
}

type groupState struct {
	inReceivership  *btree.BTreeG[int32]
	inTransition    *btree.BTreeG[int32]
	unassigned      map[int32]struct{}
	assigned        map[int32]*incrGroupMember
	activeMembers   *btree.BTreeG[*incrGroupMember]
	inactiveMembers *btree.BTreeG[*incrGroupMember]
	preparing       map[TopicPartition]*incrGroupMember
	ready           map[TopicPartition]*incrGroupMember
	members         map[string]*incrGroupMember
	partitionCount  int32
	topic           string
}

func newGroupState(cb *kgo.ConsumerBalancer, partitionCount int32, topic string) groupState {
	gs := groupState{
		inReceivership:  btree.NewOrderedG[int32](16),
		inTransition:    btree.NewOrderedG[int32](16),
		unassigned:      make(map[int32]struct{}, partitionCount),
		assigned:        make(map[int32]*incrGroupMember),
		activeMembers:   btree.NewG(16, incrGroupActiveLess),
		inactiveMembers: btree.NewG(16, incrGroupInactiveLess),
		preparing:       make(map[TopicPartition]*incrGroupMember),
		ready:           make(map[TopicPartition]*incrGroupMember),
		members:         make(map[string]*incrGroupMember),
		partitionCount:  partitionCount,
		topic:           topic,
	}
	for i := int32(0); i < partitionCount; i++ {
		gs.unassigned[i] = struct{}{}
	}

	cb.EachMember(func(member *kmsg.JoinGroupResponseMember, meta *kmsg.ConsumerMemberMetadata) {
		incrMem := &incrGroupMember{
			member:      member,
			assignments: btree.NewOrderedG[int32](16),
			donatable:   btree.NewOrderedG[int32](16),
			donating:    btree.NewOrderedG[int32](16),
			// if meta data is not supplied, assume an active member
			meta:  IncrGroupMemberMeta{Status: ActiveMember},
			topic: topic,
		}

		gs.members[member.MemberID] = incrMem
		gs.initReceivership(incrMem, member, meta)
		gs.initAssignments(incrMem, meta)
	})
	// if a partition is in receivership, it is not eligible to be donated
	// we need to adjust mem.donatable by subtraction everthing scheduled to be moved
	gs.inReceivership.Ascend(func(p int32) bool {
		if mem, ok := gs.assigned[p]; ok {
			mem.donatable.Delete(p)
			mem.donating.ReplaceOrInsert(p)
		}
		return true
	})

	// now that we have initial state set, our sort is stable and we can insrt into our btrees
	for _, incrMem := range gs.members {
		if incrMem.meta.Status == InactiveMember {
			gs.inactiveMembers.ReplaceOrInsert(incrMem)
		} else {
			gs.activeMembers.ReplaceOrInsert(incrMem)
		}
	}
	log.Debugf("activeMembers: %d, inactiveMembers: %d", gs.activeMembers.Len(), gs.inactiveMembers.Len())
	return gs
}

func (gs groupState) initAssignments(incrMem *incrGroupMember, meta *kmsg.ConsumerMemberMetadata) {
	for _, owned := range meta.OwnedPartitions {
		if gs.topic == owned.Topic {
			for _, p := range owned.Partitions {
				delete(gs.unassigned, p)
				if owner, ok := gs.assigned[p]; ok {
					// somehow we ended up double assigning a partiton ...going to take some investigation
					// we'll probably need to do some more complex collision detection here
					// this seems to happen when a member leaves mid-rebalance
					// for now, we will just reject this, but it may cause some latency spikes if we're using state stores

					// update: this seems to have been fixed by ensuring that inactive members with Preparing or Ready partitions
					// are not counted for the purposes for being `In Receivership`. see comments in `initReceivership()`
					// However, let's leave this here in case there are other oddball conditions
					log.Errorf("GroupState collision for partition: %d, rejected: %s, owner: %s", p, incrMem.member.MemberID, owner.member.MemberID)
				} else {
					gs.assigned[p] = incrMem
					incrMem.assignments.ReplaceOrInsert(p)
					incrMem.donatable.ReplaceOrInsert(p)
				}
			}
			// we're breaking because all partitions will have the same assignment
			// for all topics in this iteration of the groupState
			// if we continue, we may will up with duplicates
			break
		}
	}
}
func (gs groupState) initReceivership(incrMem *incrGroupMember, member *kmsg.JoinGroupResponseMember, meta *kmsg.ConsumerMemberMetadata) {

	if len(meta.UserData) > 0 {
		// will populate the correct member status if supplied
		if err := json.Unmarshal(meta.UserData, &incrMem.meta); err != nil {
			log.Errorf("%v", err)
		} else if incrMem.meta.Status == ActiveMember {
			// only put Ready/Preparing in receivership if the member is active
			// they member may have left mid cycle
			for _, tp := range incrMem.meta.Preparing {
				gs.preparing[tp] = incrMem
			}
			for _, tp := range incrMem.meta.Ready {
				gs.ready[tp] = incrMem
			}
			for _, tp := range incrMem.meta.Preparing {
				gs.inReceivership.ReplaceOrInsert(tp.Partition)
			}
			for _, tp := range incrMem.meta.Ready {
				gs.inReceivership.ReplaceOrInsert(tp.Partition)
			}
		} else {
			incrMem.instructions.Forget = append(incrMem.instructions.Forget, incrMem.meta.Preparing...)
			incrMem.instructions.Forget = append(incrMem.instructions.Forget, incrMem.meta.Ready...)
		}
	}
	log.Debugf("receievd %s, userMeta: %+v", incrMem.member.MemberID, incrMem.meta)
}

func (gs groupState) printGroupState(label string) {
	log.Debugf(label)
	min, _ := gs.activeMembers.Min()
	max, _ := gs.activeMembers.Max()
	log.Debugf("min: ", min.member.MemberID)
	log.Debugf("max: ", max.member.MemberID)
	gs.activeMembers.Ascend(func(incrMem *incrGroupMember) bool {
		log.Debugf("%v", incrMem)
		return true
	})
	gs.inactiveMembers.Ascend(func(incrMem *incrGroupMember) bool {
		log.Debugf("%v", incrMem)
		return true
	})
	log.Debugf(label)
}

func (gs groupState) balance(shifts int) bool {
	// first  enure that all unassigned partitions find a home
	gs.printGroupState("intial goup state")
	gs.distributeUnassigned()
	// if there are any scheduled moves, make the assignment
	gs.printGroupState("post distributeUnassigned group state")
	gs.deliverScheduledPartitionMoves()
	// now schedule future moves
	// tell the recipient to prepare for the partitions
	// when it is ready, it will trigger another rebalance
	// if we've already moved our budget of partitions, this will be a no-op
	// until the next scheduled rebalance, time determined by configuration

	log.Debugf("schedulePartitionMoves - budget: %d, (max: %d, inReceivership: %d, inTransition: %d)",
		shifts-gs.inReceivership.Len(), shifts, gs.inReceivership.Len(), gs.inTransition.Len())
	gs.printGroupState("post deliverScheduledPartitionMoves group state")
	return gs.schedulePartitionMoves(shifts - gs.inReceivership.Len() - gs.inTransition.Len())
}

func (gs groupState) assignToActiveMember(partition int32, recipient *incrGroupMember) {
	tp := ntp(partition, gs.topic)
	log.Debugf("assignToActiveMember: %v, partition: %d", recipient, partition)
	delete(gs.ready, tp)
	delete(gs.preparing, tp)
	gs.activeMembers.Delete(recipient)
	recipient.assignments.ReplaceOrInsert(partition)
	recipient.donatable.ReplaceOrInsert(partition)
	recipient.meta.Preparing = withoutPartition(recipient.meta.Preparing, partition)
	recipient.meta.Ready = withoutPartition(recipient.meta.Ready, partition)
	recipient.instructions.Prepare = withoutPartition(recipient.instructions.Prepare, partition)
	gs.activeMembers.ReplaceOrInsert(recipient)

	gs.inReceivership.ReplaceOrInsert(partition)
	gs.inTransition.ReplaceOrInsert(partition)
}

func (gs groupState) instructActiveMemberToPrepareFor(partition int32, recipient *incrGroupMember) {
	log.Debugf("instructActiveMemberToPrepareFor: %v, partition: %d", recipient, partition)
	gs.activeMembers.Delete(recipient)
	recipient.instructions.Prepare = append(recipient.instructions.Prepare, ntp(partition, gs.topic))
	gs.activeMembers.ReplaceOrInsert(recipient)
}

func (gs groupState) unassignFromDonor(partition int32, theDonor *incrGroupMember) {
	log.Debugf("unassignFromDonor: %v, partition: %d", theDonor, partition)
	if _, ok := gs.inactiveMembers.Get(theDonor); ok {
		theDonor.assignments.Delete(partition)
		theDonor.donatable.Delete(partition)
		theDonor.donating.ReplaceOrInsert(partition)
	} else {
		// gs.activeMembers.Delete(theDonor)
		theDonor.assignments.Delete(partition)
		theDonor.donatable.Delete(partition)
		theDonor.donating.ReplaceOrInsert(partition)
		// gs.activeMembers.ReplaceOrInsert(theDonor)
	}
}

func (gs groupState) distributeUnassigned() int {
	if gs.activeMembers.Len() == 0 {
		return 0
	}
	for p := range gs.unassigned {
		// start here. If there was an abnormal shutdown, nobidy may be ready for this partition
		// in this case, we're sending the partition to the consumer with the least #partitions
		recipient, _ := gs.activeMembers.Min()
		// first check to see if there is any member who is ready, or getting ready for this partition
		gs.activeMembers.Ascend(func(candidate *incrGroupMember) bool {
			if candidate.meta.isPreparingOrReadyFor(p, gs.topic) {
				recipient = candidate
				return false
			}
			return true
		})
		gs.assignToActiveMember(p, recipient)
	}
	return len(gs.unassigned)
}

func (gs groupState) deliverScheduledPartitionMoves() {
	// we're assigning all topics for the same partition
	fullyReady := make(map[int32]*incrGroupMember)
	for tp, incrMem := range gs.ready {
		// check to see if it is ready for all topics in the case of a multi-topic consumer
		if incrMem.meta.isReadyFor(tp.Partition, gs.topic) {
			fullyReady[tp.Partition] = incrMem
		}
	}
	for partition, recipient := range fullyReady {
		if donor, ok := gs.assigned[partition]; ok {
			gs.unassignFromDonor(partition, donor)
			gs.assignToActiveMember(partition, recipient)
		}
	}
}

func randomPartition(donor *incrGroupMember) int32 {
	i := rand.Intn(donor.donatable.Len())
	current := 0
	var p int32
	donor.donatable.Ascend(func(item int32) bool {
		if current == i {
			p = item
			return false
		}
		current++
		return true
	})
	return p
}

func memberAt(tree *btree.BTreeG[*incrGroupMember], index int) (mem *incrGroupMember, ok bool) {
	i := 0
	tree.Ascend(func(item *incrGroupMember) bool {
		if index == i {
			mem = item
			ok = true
			return false
		}
		i++
		return true
	})
	return
}

// begins the transition process for a partition
// the partition is chososen randomly from the donor
// some extra logic may be needed here to make sure we're not donating a partition that is in transition already
// this could only happen if our maxIntranstionPartions > 1 which is not recommended
func (gs groupState) donatePartition(donor *incrGroupMember, recipient *incrGroupMember) {

	_, deleted := gs.activeMembers.Delete(donor)
	p := randomPartition(donor)

	if deleted {
		gs.activeMembers.ReplaceOrInsert(donor)
	}
	log.Debugf("donatePartition donor: %v, recipient: %v, partition: %d", donor, recipient, p)
	gs.instructActiveMemberToPrepareFor(p, recipient)
}

func (gs groupState) schedulePartitionMoves(budget int) (shouldRebalanceAgain bool) {
	if gs.activeMembers.Len() == 0 {
		return
	}
	// we just actively assigned partions, but the move has not occurred yet
	// remove the in transition partitions from the budget, or we will move partitions
	// too quickly
	for i := 0; budget > 0; {
		if gs.inactiveMembers.Len() == 0 {
			break
		}
		if donor, ok := memberAt(gs.inactiveMembers, i); ok {
			if donor.donatable.Len() == 0 {
				i++
				continue
			}
			recipient, _ := gs.activeMembers.Min()
			gs.donatePartition(donor, recipient)
			budget--
		} else {
			break
		}
	}

	var donor *incrGroupMember
	var recipient *incrGroupMember
	for budget > 0 {
		shouldRebalanceAgain, donor, recipient = gs.isImbalanced()
		if !shouldRebalanceAgain {
			break
		}
		gs.donatePartition(donor, recipient)
		budget--
	}
	return
}

func (gs *groupState) isImbalanced() (shouldRebalance bool, donor *incrGroupMember, recipient *incrGroupMember) {
	if gs.activeMembers.Len() < 2 {
		return false, nil, nil
	}
	targetCount := int(gs.partitionCount) / gs.activeMembers.Len()
	donor, _ = gs.activeMembers.Max()
	recipient, _ = gs.activeMembers.Min()

	shouldRebalance =
		// firstly: the donor has something.donate
		donor.donatable.Len() > 1 &&
			// secondly: does th recipient have fewer assignemnts than the target count
			// Also, if we have a remainder, we could still be imbalanced as follows
			/*
				example- 20 partitions and 3 consumer yields a targetCount of 6, we could end up in the following state:
				A -> 6
				B -> 6
				C -> 8

				to fix this, also check donor.activeAndPendingAssignments() > targetCount+1 which will yield:
				A -> 6
				B -> 7
				C -> 7
				as close to balanced as possible
			*/
			(recipient.activeAndPendingAssignments() < targetCount || donor.activeAndPendingAssignments() > targetCount+1)
	return
}
