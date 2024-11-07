//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package crypto

import (
	"crypto/sha512"
	"fmt"
	"testing"
)

var (
	_ Sum512er = &Hash{}
	_ Sum512er = &sha512Hasher{}
)

// Sum512er defines interface for 512-bit hash computation.
type Sum512er interface {
	Sum512(data []byte) [64]byte
}

func TestAES512(t *testing.T) {
	var data [16]byte

	hash, err := NewHash()
	if err != nil {
		t.Fatal(err)
	}

	sum := hash.Sum512(data[:])
	_ = sum

	if false {
		fmt.Printf("Sum(%x)=%x\n", data[:], sum[:])
	}
}

func benchmarkHASH(b *testing.B, hash Sum512er, in []byte) {

	for i := 0; i < b.N; i++ {
		in[0] = byte(i)
		in[1] = byte(i >> 8)
		in[2] = byte(i >> 16)
		in[3] = byte(i >> 24)
		in[4] = byte(i >> 32)
		in[5] = byte(i >> 40)
		in[6] = byte(i >> 48)
		in[7] = byte(i >> 56)

		d := hash.Sum512(in)
		_ = d
		if i == 0 && false {
			fmt.Printf("hash: %x\n", d)
		}
	}
}

type sha512Hasher struct {
}

func (sha *sha512Hasher) Sum512(data []byte) [64]byte {
	return sha512.Sum512(data)
}

func BenchmarkSHA512(b *testing.B) {
	var in [16]byte
	benchmarkHASH(b, &sha512Hasher{}, in[:])
}

func BenchmarkAES512(b *testing.B) {
	var in [16]byte

	hash, err := NewHash()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	benchmarkHASH(b, hash, in[:])
}
