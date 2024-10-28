//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

import (
	"testing"
)

func TestID(t *testing.T) {
	var e ID

	var v uint64 = 0xa1a2a3a4e1e2e3e4

	e.PutUint64(v)
	if e.Uint64() != v {
		t.Errorf("got %v, expected %v", e.Uint64(), v)
	}
}
