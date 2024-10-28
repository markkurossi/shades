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

// NewENC creates a new cipher for the key.
func NewENC(key []byte) (cipher.Block, error) {
	return aes.NewCipher(key)
}
