//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

// EDBSetup sets up the encrypted database.
func EDBSetup(ks []byte, db map[string][]int) (map[string][]ID, error) {
	prf, err := NewPRF(ks)
	if err != nil {
		return nil, err
	}

	T := make(map[string][]ID)
	ke := make([]byte, 16)

	for w, indices := range db {
		ke = prf.Data([]byte(w), ke[:0])
		enc, err := NewENC(ke)
		if err != nil {
			return nil, err
		}
		var t []ID
		for _, ind := range indices {
			var e ID
			e.PutUint64(uint64(ind))
			enc.Encrypt(e[:], e[:])
			t = append(t, e)
		}
		T[w] = t
	}

	return T, nil
}
