//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

import (
	"crypto/aes"
	"crypto/cipher"
	"hash"
)

var (
	_ hash.Hash = hash.Hash(&PRF{})
)

// NewPRF creates a new pseudorandom function with the key.
func NewPRF(key []byte) (*PRF, error) {
	cipher, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return &PRF{
		cipher:    cipher,
		blockSize: cipher.BlockSize(),
		input:     make([]byte, cipher.BlockSize()),
		output:    make([]byte, cipher.BlockSize()),
	}, nil
}

// PRF implements pseudorandom function with AES.
type PRF struct {
	cipher    cipher.Block
	blockSize int
	round     int
	inputOfs  int
	input     []byte
	output    []byte
}

// Write implements io.Writer.
func (prf *PRF) Write(p []byte) (n int, err error) {
	n = len(p)
	for len(p) > 0 {
		i := copy(prf.input[prf.inputOfs:], p)
		prf.inputOfs += i
		p = p[i:]
		if prf.inputOfs >= prf.blockSize {
			if prf.round > 0 {
				// CBC mode.
				for i := 0; i < prf.inputOfs; i++ {
					prf.input[i] ^= prf.output[i]
				}
			}
			prf.cipher.Encrypt(prf.output, prf.input)
			prf.inputOfs = 0
			prf.round++
		}
	}
	return
}

// Sum implements hash.Hash.Sum.
func (prf *PRF) Sum(b []byte) []byte {
	if prf.inputOfs > 0 {
		for i := prf.inputOfs; i < prf.blockSize; i++ {
			prf.input[i] = 0
		}
		_, err := prf.Write(prf.input[prf.inputOfs:])
		if err != nil {
			panic(err)
		}
	}
	prf.Reset()
	return append(b, prf.output...)
}

// Int computes random value for i and appends it to b.
func (prf *PRF) Int(i uint64, b []byte) []byte {
	var buf ID

	buf.PutUint64(uint64(i))
	_, err := prf.Write(buf[:])
	if err != nil {
		panic(err)
	}
	return prf.Sum(b)
}

// Data computes random value for data and appends it to b.
func (prf *PRF) Data(data, b []byte) []byte {
	_, err := prf.Write(data)
	if err != nil {
		panic(err)
	}
	return prf.Sum(b)
}

// Reset implements hash.Hash.Reset.
func (prf *PRF) Reset() {
	prf.round = 0
	prf.inputOfs = 0
}

// Size implements hash.Hash.Size.
func (prf *PRF) Size() int {
	return aes.BlockSize
}

// BlockSize implements hash.Hash.BlockSize.
func (prf *PRF) BlockSize() int {
	return aes.BlockSize
}
