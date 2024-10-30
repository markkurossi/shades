//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

import (
	"crypto/aes"
	"crypto/cipher"
)

// Hash implements 512-bit hash computation using AES-CBC.
type Hash struct {
	cipher cipher.Block
}

// NewHash creates a new AES-CBC hash.
func NewHash() (*Hash, error) {
	var key [16]byte

	cipher, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	return &Hash{
		cipher: cipher,
	}, nil
}

// Sum512 computes 512-bit hash of the data.
func (hash *Hash) Sum512(data []byte) [64]byte {
	var sum [64]byte
	var ofs, prev, round int

	prev = -1

	for len(data) > 0 || ofs < len(sum) {
		if ofs >= len(sum) {
			ofs = 0
			round++
		}
		var n int
		if len(data) > 0 {
			n = copy(sum[ofs:], data)
			data = data[n:]
		}
		if round > 0 {
			for ; n < 16; n++ {
				sum[ofs+n] = 0
			}
		}
		if prev >= 0 {
			// CBC mode.
			for i := 0; i < 16; i++ {
				sum[ofs+i] ^= sum[prev+i]
			}
		}
		hash.cipher.Encrypt(sum[ofs:], sum[ofs:])
		prev = ofs
		ofs += 16
	}

	return sum
}
