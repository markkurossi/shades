//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

import (
	"crypto/rand"
	"fmt"
)

// BXT implements the Basic Cross-Tags Protocol (BXT).
type BXT struct {
	ks    []byte
	kx    []byte
	prfKs *PRF
	prfKx *PRF
	tset  *TSet
	xset  *XSet
}

// BXTSetup sets up the encrypted database for the Basic Cross-Tags
// Protocol (BXT).
func BXTSetup(db map[string][]int) (SSE, error) {
	var ks [16]byte
	_, err := rand.Read(ks[:])
	if err != nil {
		return nil, err
	}
	prfKs, err := NewPRF(ks[:])
	if err != nil {
		return nil, err
	}

	var kx [16]byte
	_, err = rand.Read(kx[:])
	if err != nil {
		return nil, err
	}
	prfKx, err := NewPRF(kx[:])
	if err != nil {
		return nil, err
	}

	T := make(map[string][]ID)

	var numTokens int
	for _, w := range db {
		numTokens += len(w)
	}
	xset := NewXSet(numTokens)

	ke := make([]byte, 16)
	xtrap := make([]byte, 16)

	for w, indices := range db {
		var t []ID

		ke = prfKs.Data([]byte(w), ke[:0])
		xtrap = prfKx.Data([]byte(w), xtrap[:0])

		enc, err := NewENC(ke)
		if err != nil {
			return nil, err
		}

		for _, ind := range indices {
			var i, e ID
			i.PutUint64(uint64(ind))
			enc.Encrypt(e[:], i[:])
			t = append(t, e)

			f, err := NewPRF(xtrap)
			if err != nil {
				return nil, err
			}
			xtag := f.Data(i[:], nil)
			xset.Add(xtag)
		}
		T[w] = t
	}

	tset, err := TSetSetup(T)
	if err != nil {
		return nil, err
	}

	return &BXT{
		ks:    ks[:],
		kx:    kx[:],
		prfKs: prfKs,
		prfKx: prfKx,
		tset:  tset,
		xset:  xset,
	}, nil
}

// Search searches the query and returns a list of matching document
// indices.
func (bxt *BXT) Search(query []string) ([]int, error) {
	if len(query) < 1 {
		return nil, fmt.Errorf("BXT needs 1 or more query terms")
	}

	q := []byte(query[0])

	stag := bxt.tset.GetTag(q, nil)

	var xtraps [][]byte
	for i := 1; i < len(query); i++ {
		xtrap := bxt.prfKx.Data([]byte(query[i]), nil)
		xtraps = append(xtraps, xtrap)
	}

	t, err := bxt.tset.Retrieve(stag)
	if err != nil {
		return nil, err
	}

	ke := bxt.prfKs.Data(q, nil)
	dec, err := NewENC(ke)
	if err != nil {
		return nil, err
	}

	var result []int

	for _, id := range t {
		var plain ID
		dec.Decrypt(plain[:], id[:])

		found := 1

		for i := 1; i < len(query); i++ {
			f, err := NewPRF(xtraps[i-1])
			if err != nil {
				return nil, err
			}
			xtag := f.Data(plain[:], nil)
			if bxt.xset.Lookup(xtag) {
				found++
			}
		}
		if found == len(query) {
			result = append(result, int(plain.Uint64()))
		}
	}

	return result, nil
}
