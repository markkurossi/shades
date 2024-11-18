//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"fmt"
)

// Transaction implements a database transaction.
type Transaction struct {
	db       *DB
	rw       bool
	writable map[PhysicalID]bool
}

func (tr *Transaction) NewPage() ([]byte, LogicalID, error) {
	if !tr.rw {
		return nil, 0, fmt.Errorf("read-only transaction")
	}
	return nil, 0, fmt.Errorf("Transaction.NewPage not implemented yet")
}

func (tr *Transaction) ReadablePage(id LogicalID) ([]byte, error) {
	return nil, fmt.Errorf("Transaction.ReadablePage not implemented yet")
}

func (tr *Transaction) WritablePage(id LogicalID) ([]byte, error) {
	if !tr.rw {
		return nil, fmt.Errorf("read-only transaction")
	}
	return nil, fmt.Errorf("Transaction.WritablePage not implemented yet")
}

func (tr *Transaction) ReleasePage(id LogicalID) error {
	return fmt.Errorf("Transaction.ReleasePage not implemented yet")
}

func (tr *Transaction) Commit() error {
	if tr.rw {
		return fmt.Errorf("Transaction.Commit not implemented yet")
	}
	return nil
}

func (tr *Transaction) Abort() error {
	return fmt.Errorf("Transaction.Abort not implemented yet")
}
