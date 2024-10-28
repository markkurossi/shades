//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

import (
	"crypto/aes"
	"encoding/binary"
)

var (
	bo = binary.BigEndian
)

// ID defines an object ID.
type ID [aes.BlockSize]byte

// Uint64 gets the uint64 part of the identifier.
func (id *ID) Uint64() uint64 {
	return bo.Uint64(id[0:])
}

// PutUint64 sets the uint64 part of the identifier.
func (id *ID) PutUint64(v uint64) {
	bo.PutUint64(id[0:], v)
}
