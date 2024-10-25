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
	_ hash.Hash = &prfAES{}
)

// NewPRF creates a new pseudorandom function with the key.
func NewPRF(key []byte) (hash.Hash, error) {
	cipher, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return &prfAES{
		cipher:    cipher,
		blockSize: cipher.BlockSize(),
		input:     make([]byte, cipher.BlockSize()),
		output:    make([]byte, cipher.BlockSize()),
	}, nil
}

type prfAES struct {
	cipher    cipher.Block
	blockSize int
	round     int
	inputOfs  int
	input     []byte
	output    []byte
}

// Write implements io.Writer.
func (prf *prfAES) Write(p []byte) (n int, err error) {
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
func (prf *prfAES) Sum(b []byte) []byte {
	_, err := prf.Write(b)
	if err != nil {
		panic(err)
	}
	if prf.inputOfs > 0 {
		for i := prf.inputOfs; i < prf.blockSize; i++ {
			prf.input[i] = 0
		}
		_, err = prf.Write(prf.input[prf.inputOfs:])
		if err != nil {
			panic(err)
		}
	}
	prf.Reset()
	return prf.output
}

// Sum implements hash.Hash.Reset.
func (prf *prfAES) Reset() {
	prf.round = 0
	prf.inputOfs = 0
}

// Sum implements hash.Hash.Size.
func (prf *prfAES) Size() int {
	return aes.BlockSize
}

// Sum implements hash.Hash.BlockSize.
func (prf *prfAES) BlockSize() int {
	return aes.BlockSize
}
