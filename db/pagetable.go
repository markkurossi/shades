//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"fmt"
)

// PhysicalID defines a physical page ID.
type PhysicalID uint64

// PhysicalID component masks.
const (
	PIDMetaMask    uint64 = 0xffff000000000000
	PIDPagenumMask uint64 = 0x0000ffffffffffff
)

// NewPhysicalID creates a new physical page ID from the arguments.
func NewPhysicalID(meta uint16, pagenum uint64) PhysicalID {
	if pagenum&PIDMetaMask != 0 {
		panic("physical page number too big")
	}
	return PhysicalID(uint64(meta)<<48 | pagenum&PIDPagenumMask)
}

// Meta returns the meta field of the physical page ID.
func (pid PhysicalID) Meta() uint16 {
	return uint16(pid >> 48)
}

// Pagenum returns the page number of the physical page ID.
func (pid PhysicalID) Pagenum() uint64 {
	return uint64(pid) & PIDPagenumMask
}

func (pid PhysicalID) String() string {
	return fmt.Sprintf("%04x:%012x", pid.Meta(), pid.Pagenum())
}

// LogicalID defines a logical page ID.
type LogicalID uint64

// LogicalID component masks.
const (
	IDMetaMask     uint64 = 0xc000000000000000
	IDObjectIDMask uint64 = 0x3fff000000000000
	IDPagenumMask  uint64 = 0x0000ffffffffffff
)

// NewLogicalID creates a new logical page ID from the arguments.
func NewLogicalID(meta byte, objectID uint16, pagenum uint64) LogicalID {
	if meta&0xfc != 0 {
		panic("logical page meta too big")
	}
	if objectID&0xc000 != 0 {
		panic("logical page object ID too big")
	}
	if pagenum&(IDMetaMask|IDObjectIDMask) != 0 {
		panic("logical page number too big")
	}
	return LogicalID(uint64(meta)<<62 | uint64(objectID)<<48 | pagenum)
}

// Meta returns the meta field of the logical page ID.
func (id LogicalID) Meta() byte {
	return byte(id >> 62)
}

// ObjectID returns the object ID of the logical page ID.
func (id LogicalID) ObjectID() uint16 {
	return uint16((uint64(id) & IDObjectIDMask) >> 48)
}

// Pagenum returns the page number of the logical page ID.
func (id LogicalID) Pagenum() uint64 {
	return uint64(id) & IDPagenumMask
}
