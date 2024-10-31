//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

import (
	"bytes"
)

// XSet implements an xtag set.
type XSet struct {
	base [][][]byte
}

// NewXSet creates a new XSet for database with n keyword occurrences.
func NewXSet(n int) *XSet {
	return &XSet{
		base: make([][][]byte, n/4),
	}
}

// Add adds the xtag data to the set.
func (xset *XSet) Add(data []byte) {
	hash := xset.hash(data)
	xset.base[hash] = append(xset.base[hash], data)
}

func (xset *XSet) hash(data []byte) int {
	return (int(data[0])<<24 | int(data[1])<<16 |
		int(data[2])<<8 | int(data[3])) % len(xset.base)
}

// Lookup finds the xtag data from the set.
func (xset *XSet) Lookup(data []byte) bool {
	hash := xset.hash(data)
	for _, x := range xset.base[hash] {
		if bytes.Compare(x, data) == 0 {
			return true
		}
	}
	return false
}
