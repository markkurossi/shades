//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
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
