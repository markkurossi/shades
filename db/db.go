//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"fmt"
	"os"
)

// Device implements an I/O device.
type Device interface {
	Close() error
	ReadAt(b []byte, off int64) (n int, err error)
	Sync() error
	WriteAt(b []byte, off int64) (n int, err error)
}

var (
	_ Device = &os.File{}
	_ Device = &MemDevice{}
)

// DB implements the Shades database.
type DB struct {
	params Params
	device Device
	pt     *PageTable
	cache  *Cache
}

// Create creates a new database with the parameters and I/O device.
func Create(params Params, device Device) (*DB, error) {
	// Check parameters.
	var pageSize int
	for pageSize = 1024; pageSize < params.PageSize; pageSize *= 2 {
	}
	if pageSize != params.PageSize {
		return nil, fmt.Errorf("page size must be power of 2 and >= 1024")
	}

	db, err := newDB(params, device)
	if err != nil {
		return nil, err
	}
	err = db.pt.Init()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Open opens the database from the I/O device.
func Open(params Params, device Device) (*DB, error) {
	pt, err := NewPageTable(nil)
	if err != nil {
		return nil, err
	}
	// Open the root block and read database page size.
	for pageSize := 1024; pageSize <= 1024*1024; pageSize *= 2 {
		buf := make([]byte, pageSize)
		_, err := device.ReadAt(buf, 0)
		if err != nil {
			return nil, err
		}
		err = pt.parseRootBlock(buf)
		if err == nil {
			// Parsed the existing root pointer. Use the generation
			// time values and open the database.
			params.PageSize = int(pt.root.PageSize)
			return open(params, device)
		}
	}
	return nil, fmt.Errorf("not a valid shades DB file")
}

// NewTransaction starts a new transaction in read-only or read-write
// mode depeneding on the argument rw.
func (db *DB) NewTransaction(rw bool) (*Transaction, error) {
	tr := &Transaction{
		db: db,
		rw: rw,
	}
	if rw {
		tr.writable = make(map[PhysicalID]bool)
	}
	return tr, nil
}

func open(params Params, device Device) (*DB, error) {
	db, err := newDB(params, device)
	if err != nil {
		return nil, err
	}
	err = db.pt.Open()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func newDB(params Params, device Device) (*DB, error) {
	var err error

	db := &DB{
		params: params,
		device: device,
	}
	db.cache, err = NewCache(db)
	if err != nil {
		return nil, err
	}
	db.pt, err = NewPageTable(db)
	if err != nil {
		return nil, err
	}

	return db, nil
}
