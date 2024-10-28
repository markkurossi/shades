//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

import (
	"testing"
)

func TestEncrypt(t *testing.T) {
	var key [16]byte
	enc, err := NewENC(key[:])
	if err != nil {
		t.Fatal(err)
	}

	var e ID
	var v uint64 = 0xa1a2a3a4e1e2e3e4

	e.PutUint64(v)

	enc.Encrypt(e[:], e[:])
	enc.Decrypt(e[:], e[:])

	if e.Uint64() != v {
		t.Errorf("got %x, expected %x", e.Uint64(), v)
	}

	var cipher, plain ID

	enc.Encrypt(cipher[:], e[:])
	enc.Decrypt(plain[:], cipher[:])

	if plain.Uint64() != v {
		t.Errorf("got %x, expected %x", plain.Uint64(), v)
	}
}
