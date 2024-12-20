//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"testing"
)

var prfTestVectors = []struct {
	key string
	in  string
	out string
}{
	{
		key: "2b7e151628aed2a6abf7158809cf4f3c",
		in:  "6bc1bee22e409f96e93d7e117393172a",
		out: "3ad77bb40d7a3660a89ecaf32466ef97",
	},
	{
		key: "2b7e151628aed2a6abf7158809cf4f3c",
		in:  "6bc1bee22e409f96e93d7e117393172a6bc1bee22e409f96e93d7e117393172a",
		out: "025c61efee87e604cd1b12ce9dde5c51",
	},
}

func TestCBC(t *testing.T) {
	test := prfTestVectors[1]
	key, err := hex.DecodeString(test.key)
	if err != nil {
		t.Fatal(err)
	}
	in, err := hex.DecodeString(test.in)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := hex.DecodeString(test.out)
	if err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	if len(in)%aes.BlockSize != 0 {
		t.Fatalf("CBC input not multiple of the block size")
	}
	numBlocks := len(in) / aes.BlockSize

	ciphertext := make([]byte, aes.BlockSize+len(in))
	mode := cipher.NewCBCEncrypter(block, ciphertext[:aes.BlockSize])
	mode.CryptBlocks(ciphertext[aes.BlockSize:], in)

	out := ciphertext[numBlocks*aes.BlockSize:]
	if bytes.Compare(out, expected) != 0 {
		t.Errorf("got %x, expected %x", out, expected)
	}
}

func TestPRF(t *testing.T) {
	for idx, test := range prfTestVectors {
		key, err := hex.DecodeString(test.key)
		if err != nil {
			t.Fatal(err)
		}
		in, err := hex.DecodeString(test.in)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := hex.DecodeString(test.out)
		if err != nil {
			t.Fatal(err)
		}
		prf, err := NewPRF(key)
		if err != nil {
			t.Fatal(err)
		}

		_, err = prf.Write(in)
		if err != nil {
			t.Fatal(err)
		}
		out := prf.Sum(nil)
		if bytes.Compare(out, expected) != 0 {
			t.Errorf("a) test-%d: got %x, expected %x", idx, out, expected)
		}

		for i := 0; i < len(in); i++ {
			_, err = prf.Write(in[i : i+1])
			if err != nil {
				t.Fatal(err)
			}
		}
		out = prf.Sum(nil)
		if bytes.Compare(out, expected) != 0 {
			t.Errorf("b) test-%d: got %x, expected %x", idx, out, expected)
		}
	}
}

func benchmarkPRF(b *testing.B, key, in, out []byte) {
	prf, err := NewPRF(key[:])
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		in[0] = byte(i)
		in[1] = byte(i >> 8)
		in[2] = byte(i >> 16)
		in[3] = byte(i >> 24)
		in[4] = byte(i >> 32)
		in[5] = byte(i >> 40)
		in[6] = byte(i >> 48)
		in[7] = byte(i >> 56)

		prf.Write(in[:])
		d := prf.Sum(out[:0])
		_ = d
		if i == 0 && false {
			fmt.Printf("out: %x\n", d)
		}
	}
}

func BenchmarkPRF(b *testing.B) {
	var key [16]byte
	var in [16]byte
	var out [16]byte

	benchmarkPRF(b, key[:], in[:], out[:])
}

func BenchmarkPRFShort(b *testing.B) {
	var key [16]byte
	var in [8]byte
	var out [16]byte

	benchmarkPRF(b, key[:], in[:], out[:])
}

func BenchmarkPRFLong(b *testing.B) {
	var key [16]byte
	var in [32]byte
	var out [16]byte

	benchmarkPRF(b, key[:], in[:], out[:])
}
