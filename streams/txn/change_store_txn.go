package txn

import "sync"

type TransactableSupplier interface {
	ForTxnContext(uint64) (uint64, Transactable)
}

type Transactable interface {
	Commit() error
	Abort()
}

type StateStoreTransaction[T any] struct {
	currentId     uint64
	eventCount    int
	transactables []Transactable
	wg            sync.WaitGroup
}

func NewTxn[T any]() *StateStoreTransaction[T] {
	return &StateStoreTransaction[T]{}
}

func (txn *StateStoreTransaction[T]) IncrEventCount() {
	txn.eventCount++
}

func (txn *StateStoreTransaction[T]) EventCount() int {
	return txn.eventCount
}

func (txn *StateStoreTransaction[T]) Commit() {
	for _, t := range txn.transactables {
		t.Commit()
		txn.wg.Done()
	}
}

func (txn *StateStoreTransaction[T]) Abort() {
	for _, t := range txn.transactables {
		t.Abort()
		txn.wg.Done()
	}
}

func (txn *StateStoreTransaction[T]) Wait() {
	txn.wg.Wait()
}

func (txn *StateStoreTransaction[T]) Add(s TransactableSupplier) {
	nextId, transactable := s.ForTxnContext(txn.currentId)
	if nextId != txn.currentId {
		txn.transactables = append(txn.transactables, transactable)
		txn.currentId = nextId
		txn.wg.Add(1)
	}
}
