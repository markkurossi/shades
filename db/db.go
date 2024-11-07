//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

// Device implements an I/O device.
type Device interface {
	Close() error
	ReadAt(b []byte, off int64) (n int, err error)
	Sync() error
	WriteAt(b []byte, off int64) (n int, err error)
}

// DB implements the Shades database.
type DB struct {
	params Params
	device Device
	pt     *PageTable
	cache  *Cache
}

// NewDB creates a new database with the parameters and I/O device.
func NewDB(params Params, device Device) (*DB, error) {
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
