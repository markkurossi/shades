//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

import (
	"bytes"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
)

// TSet implements a tuple set (T-Set).
type TSet struct {
	records [][]record
	kt      []byte
	prf     *PRF
}

// TSetSetup creates the TSet for the database.
func TSetSetup(T map[string][]ID) (*TSet, error) {
	var count int
	for _, t := range T {
		count += len(t)
	}

	b := count / 2
	if b == 0 {
		b = count
	}
	const s int = 8

	tset := &TSet{
		records: make([][]record, b),
		kt:      make([]byte, 16),
	}
	free := make([]int, b)

	_, err := rand.Read(tset.kt)
	if err != nil {
		return nil, err
	}
	tset.prf, err = NewPRF(tset.kt)
	if err != nil {
		return nil, err
	}

	// For every keyword w ∈ W.

	stag := make([]byte, 16)
	ilambda := make([]byte, 16)

	for w, t := range T {
		// Set stag = F(kt, w)
		stag = tset.GetTag([]byte(w), stag[:0])

		prff, err := NewPRF(stag[:])
		if err != nil {
			return nil, err
		}

		// For each i = 1, ..., |t|, si=t[i]:
		for i, si := range t {
			ilambda = prff.Int(uint64(i), ilambda[:0])

			b, L, K := tset.hash(ilambda)

			j := free[b]
			free[b]++
			if j > s {
				fmt.Printf("free[%d] is empty (j=%v)\n", b, j)
			}
			var beta byte
			if i+1 < len(t) {
				beta = 0xff
			}
			var value [1 + 16]byte
			value[0] = beta
			copy(value[1:], si[:])
			for idx, k := range K {
				value[idx] ^= k
			}
			r := record{
				label: L,
				value: value,
			}
			tset.records[b] = append(tset.records[b], r)
		}
	}

	return tset, nil
}

// GetTag creates the stag for the keyword w and appends it to the
// argument stag.
func (tset *TSet) GetTag(w, stag []byte) []byte {
	return tset.prf.Data(w, stag)
}

// Retrieve retrieves all matches of the stag.
func (tset *TSet) Retrieve(stag []byte) ([]ID, error) {

	var t []ID
	var beta byte = 0xff

	prff, err := NewPRF(stag)
	if err != nil {
		return nil, err
	}

	ilambda := make([]byte, 16)
	var value [1 + 16]byte

	for i := 0; beta != 0; i++ {
		ilambda = prff.Int(uint64(i), ilambda[:0])

		b, L, K := tset.hash(ilambda)
		found := false
		for _, r := range tset.records[b] {
			if bytes.Compare(r.label, L) == 0 {
				found = true
				copy(value[0:], r.value[:])
				for idx, k := range K {
					value[idx] ^= k
				}
				beta = value[0]
				t = append(t, ID(value[1:]))
			}
		}
		if !found {
			return nil, fmt.Errorf("not found")
		}
	}
	return t, nil
}

func (tset *TSet) hash(data []byte) (int, []byte, []byte) {
	digest := sha512.Sum512(data)
	b := int(bo.Uint32(digest[0:4]))
	return b % len(tset.records), digest[4 : 4+16], digest[4+16 : 4+16+16+1]
}

type record struct {
	label []byte
	value [1 + 16]byte
}
