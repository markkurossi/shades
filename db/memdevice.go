//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"fmt"
)

// MemDevice implements memory device.
type MemDevice struct {
	buf []byte
}

// NewMemDevice creates a new memory device with the size capacity.
func NewMemDevice(size int) *MemDevice {
	return &MemDevice{
		buf: make([]byte, size),
	}
}

// Close implements Device.Close.
func (mem *MemDevice) Close() error {
	return nil
}

// ReadAt implements Device.ReadAt.
func (mem *MemDevice) ReadAt(b []byte, off int64) (n int, err error) {
	if int(off)+len(b) > len(mem.buf) {
		return 0, fmt.Errorf("reading %v bytes out of range [0...%v[",
			int(off)+len(b)-len(mem.buf), len(mem.buf))
	}
	return copy(b, mem.buf[off:]), nil
}

// Sync implements Device.Sync.
func (mem *MemDevice) Sync() error {
	return nil
}

// WriteAt implements Device.WriteAt.
func (mem *MemDevice) WriteAt(b []byte, off int64) (n int, err error) {
	if int(off)+len(b) > len(mem.buf) {
		return 0, fmt.Errorf("writing %v bytes out of range [0...%v[",
			int(off)+len(b)-len(mem.buf), len(mem.buf))
	}
	return copy(mem.buf[off:], b), nil
}
