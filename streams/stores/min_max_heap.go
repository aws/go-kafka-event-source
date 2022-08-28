package stores

import (
	"math/bits"
)

type Prioritizable[T any] interface {
	HasPriorityOver(item T) bool
}

type PrioritizedItem[T Prioritizable[T]] struct {
	index int
	Value T
}

// MinMaxHeap provides min-max heap operations for any type that
// implements heap.Interface. A min-max heap can be used to implement a
// double-ended priority queue.
//
// Min-max heap implementation from the 1986 paper "Min-Max Heaps and
// Generalized Priority Queues" by Atkinson et. al.
// https://doi.org/10.1145/6617.6621.
type MinMaxHeap[T Prioritizable[T]] struct {
	items []*PrioritizedItem[T]
	maxed bool
}

func NewMinMaxHeap[T Prioritizable[T]](items ...T) *MinMaxHeap[T] {
	pq := &MinMaxHeap[T]{
		items: make([]*PrioritizedItem[T], len(items)),
	}
	for i, v := range items {
		pq.items[i] = &PrioritizedItem[T]{index: i, Value: v}
	}
	initHeap(pq)
	return pq
}

func (pq *MinMaxHeap[T]) less(i, j int) bool {
	a := pq.items[i]
	b := pq.items[j]
	return a.Value.HasPriorityOver(b.Value)
}

func (pq *MinMaxHeap[T]) swap(i, j int) {
	// a, b :=
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *MinMaxHeap[T]) Len() int {
	return len(pq.items)
}

func (pq *MinMaxHeap[T]) Push(item *PrioritizedItem[T]) {
	pq.maxed = false
	push(pq, item)
}

func (pq *MinMaxHeap[T]) Update(item *PrioritizedItem[T]) {
	pq.maxed = false
	fix(pq, item.index)
}

func (pq *MinMaxHeap[T]) Remove(item *PrioritizedItem[T]) {
	pq.maxed = false
	remove(pq, item.index)
}

func (pq *MinMaxHeap[T]) Min() *PrioritizedItem[T] {
	if pq.Len() == 0 {
		return nil
	}
	return pq.items[0]
}

func (pq *MinMaxHeap[T]) Max() *PrioritizedItem[T] {
	if pq.Len() == 0 {
		return nil
	}
	if !pq.maxed {
		setMax(pq)
		pq.maxed = true
	}
	return pq.items[pq.Len()-1]
}

func (pq *MinMaxHeap[T]) PopMin() *PrioritizedItem[T] {
	if pq.Len() == 0 {
		return nil
	}
	pq.maxed = false
	return popMin(pq)
}

func (pq *MinMaxHeap[T]) PopMax() *PrioritizedItem[T] {
	if pq.Len() == 0 {
		return nil
	}
	if !pq.maxed {
		setMax(pq)
	}
	pq.maxed = false
	return pq.pop()
}

func (pq *MinMaxHeap[T]) pop() *PrioritizedItem[T] {
	n := len(pq.items) - 1
	v := pq.items[n]
	pq.items[n] = nil
	pq.items = pq.items[0:n]
	return v
}

func (pq *MinMaxHeap[T]) push(item *PrioritizedItem[T]) {
	pq.items = append(pq.items, item)
}

// Interface copied from the heap package, so code that imports minmaxheap does
// not also have to import "container/heap".

func level(i int) int {
	// floor(log2(i + 1))
	return bits.Len(uint(i)+1) - 1
}

func isMinLevel(i int) bool {
	return level(i)%2 == 0
}

func lchild(i int) int {
	return i*2 + 1
}

func rchild(i int) int {
	return i*2 + 2
}

func parent(i int) int {
	return (i - 1) / 2
}

func hasParent(i int) bool {
	return i > 0
}

func hasGrandparent(i int) bool {
	return i > 2
}

func grandparent(i int) int {
	return parent(parent(i))
}

func down[T Prioritizable[T]](h *MinMaxHeap[T], i, n int) bool {
	min := isMinLevel(i)
	i0 := i
	for {
		m := i

		l := lchild(i)
		if l >= n || l < 0 /* overflow */ {
			break
		}
		if h.less(l, m) == min {
			m = l
		}

		r := rchild(i)
		if r < n && h.less(r, m) == min {
			m = r
		}

		// grandchildren are contiguous i*4+3+{0,1,2,3}
		for g := lchild(l); g < n && g <= rchild(r); g++ {
			if h.less(g, m) == min {
				m = g
			}
		}

		if m == i {
			break
		}

		h.swap(i, m)

		if m == l || m == r {
			break
		}

		// m is grandchild
		p := parent(m)
		if h.less(p, m) == min {
			h.swap(m, p)
		}
		i = m
	}
	return i > i0
}

func up[T Prioritizable[T]](h *MinMaxHeap[T], i int) {
	min := isMinLevel(i)

	if hasParent(i) {
		p := parent(i)
		if h.less(p, i) == min {
			h.swap(i, p)
			min = !min
			i = p
		}
	}

	for hasGrandparent(i) {
		g := grandparent(i)
		if h.less(i, g) != min {
			return
		}

		h.swap(i, g)
		i = g
	}
}

// initHeap establishes the heap invariants required by the other routines in this
// package. initHeap may be called whenever the heap invariants may have been
// invalidated.
// The complexity is O(n) where n = h.Len().
func initHeap[T Prioritizable[T]](h *MinMaxHeap[T]) {
	n := h.Len()
	for i := n/2 - 1; i >= 0; i-- {
		down(h, i, n)
	}
}

// push pushes the element x onto the heap.
// The complexity is O(log n) where n = h.Len().
func push[T Prioritizable[T]](h *MinMaxHeap[T], item *PrioritizedItem[T]) {
	h.push(item)
	up(h, h.Len()-1)
}

// popMin removes and returns the minimum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
func popMin[T Prioritizable[T]](h *MinMaxHeap[T]) *PrioritizedItem[T] {
	n := h.Len() - 1
	h.swap(0, n)
	down(h, 0, n)
	return h.pop()
}

// popMax removes and returns the maximum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
func setMax[T Prioritizable[T]](h *MinMaxHeap[T]) {
	n := h.Len()

	i := 0
	l := lchild(0)
	if l < n && !h.less(l, i) {
		i = l
	}

	r := rchild(0)
	if r < n && !h.less(r, i) {
		i = r
	}

	h.swap(i, n-1)
	down(h, i, n-1)
}

// // popMax removes and returns the maximum element (according to Less) from the heap.
// // The complexity is O(log n) where n = h.Len().
// func popMax[T Prioritizable[T]](h *MinMaxHeap[T]) *PrioritizedItem[T] {
// 	n := h.Len()

// 	i := 0
// 	l := lchild(0)
// 	if l < n && !h.less(l, i) {
// 		i = l
// 	}

// 	r := rchild(0)
// 	if r < n && !h.less(r, i) {
// 		i = r
// 	}

// 	h.swap(i, n-1)
// 	down(h, i, n-1)
// 	return h.pop()
// }

// remove removes and returns the element at index i from the heap.
// The complexity is O(log n) where n = h.Len().
func remove[T Prioritizable[T]](h *MinMaxHeap[T], i int) interface{} {
	n := h.Len() - 1
	if n != i {
		h.swap(i, n)
		if !down(h, i, n) {
			up(h, i)
		}
	}
	return h.pop()
}

// fix re-establishes the heap ordering after the element at index i has
// changed its value. Changing the value of the element at index i and then
// calling fix is equivalent to, but less expensive than, calling Remove(h, i)
// followed by a Push of the new value.
// The complexity is O(log n) where n = h.Len().
func fix[T Prioritizable[T]](h *MinMaxHeap[T], i int) {
	if !down(h, i, h.Len()) {
		up(h, i)
	}
}
