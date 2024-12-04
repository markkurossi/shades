//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"fmt"
)

// BaseTransaction implements a base transaction.
type BaseTransaction struct {
	cache    *Cache
	pt       *PageTable
	rw       bool
	writable map[PhysicalID]PhysicalID
}

// NewPage allocates a new page.
func (tr *BaseTransaction) NewPage() (*PageRef, LogicalID, error) {
	if !tr.rw {
		return nil, 0, fmt.Errorf("read-only transaction")
	}
	id, err := tr.pt.allocLogicalID()
	if err != nil {
		return nil, 0, err
	}
	pid, err := tr.pt.allocPhysicalID()
	if err != nil {
		tr.pt.freeLogicalID(id)
		return nil, 0, err
	}
	err = tr.pt.set(tr, id, pid)
	if err != nil {
		tr.pt.freePhysicalID(pid)
		tr.pt.freeLogicalID(id)
		return nil, 0, err
	}
	tr.writable[pid] = 0

	ref, err := tr.cache.Get(pid)
	if err != nil {
		delete(tr.writable, pid)
		tr.pt.freePhysicalID(pid)
		tr.pt.freeLogicalID(id)
		return nil, 0, err
	}
	ref.refcount++

	return ref, id, nil
}

// ReadablePage returns a read-only reference to the page id.
func (tr *BaseTransaction) ReadablePage(id LogicalID) (*PageRef, error) {
	pid, err := tr.pt.get(tr, id)
	if err != nil {
		return nil, err
	}
	return tr.cache.Get(pid)
}

// WritablePage returns a writable reference to the page id.
func (tr *BaseTransaction) WritablePage(id LogicalID) (*PageRef, error) {
	if !tr.rw {
		return nil, fmt.Errorf("read-only transaction")
	}
	pid, err := tr.pt.get(tr, id)
	if err != nil {
		return nil, err
	}
	_, writable := tr.writable[pid]
	if writable {
		// The page is writable in this transaction.
		return tr.cache.Get(pid)
	}

	// Make page writable.

	newPid, err := tr.pt.allocPhysicalID()
	if err != nil {
		return nil, err
	}
	// Make shadow copy of the page.
	oldRef, err := tr.cache.Get(pid)
	if err != nil {
		tr.pt.freePhysicalID(newPid)
		return nil, err
	}
	newRef, err := tr.cache.Get(newPid)
	if err != nil {
		tr.pt.freePhysicalID(newPid)
		oldRef.Release()
		return nil, err
	}
	copy(newRef.Data(), oldRef.Read())
	oldRef.Release()

	err = tr.pt.set(tr, id, newPid)
	if err != nil {
		tr.pt.freePhysicalID(newPid)
		newRef.Release()
		return nil, err
	}
	tr.writable[newPid] = pid
	return newRef, nil
}

// Commit commits the transaction.
func (tr *BaseTransaction) Commit() error {
	return tr.pt.commit(tr)
}

// Abort aborts the transaction.
func (tr *BaseTransaction) Abort() error {
	return tr.pt.abort(tr)
}
