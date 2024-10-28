//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

import (
	"fmt"
)

// EDBSetup sets up the encrypted database.
func EDBSetup(ks []byte, db map[string][]int) error {
	prf, err := NewPRF(ks)
	if err != nil {
		return err
	}

	T := make(map[string][][]byte)
	ke := make([]byte, 16)

	for w, indices := range db {
		fmt.Printf("%20s:%v\n", w, indices)
		_, err := prf.Write([]byte(w))
		if err != nil {
			return err
		}
		ke = prf.Sum(ke[:0])
		// fmt.Printf("ke: %x\n", ke)
		enc, err := NewENC(ke)
		if err != nil {
			return err
		}
		var t [][]byte
		for _, ind := range indices {
			e := make([]byte, 16)
			bo.PutUint64(e, uint64(ind))
			enc.Encrypt(e, e)
			t = append(t, e)
		}
		T[w] = t
	}
	for w, indices := range T {
		fmt.Printf("T[%s]:\n", w)
		for idx, i := range indices {
			fmt.Printf(" %d) %x\n", idx, i)
		}
	}

	return nil
}
