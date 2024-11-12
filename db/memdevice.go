//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

// MemDevice implements memory device.
type MemDevice struct {
}

// Close implements Device.Close.
func (mem *MemDevice) Close() error {
	return nil
}

// ReadAt implements Device.ReadAt.
func (mem *MemDevice) ReadAt(b []byte, off int64) (n int, err error) {
	return len(b), nil
}

// Sync implements Device.Sync.
func (mem *MemDevice) Sync() error {
	return nil
}

// WriteAt implements Device.WriteAt.
func (mem *MemDevice) WriteAt(b []byte, off int64) (n int, err error) {
	return len(b), nil
}
