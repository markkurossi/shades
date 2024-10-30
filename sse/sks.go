//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package sse

// SKS implements the Single-Keyword SSE Scheme (SKS).
type SKS struct {
	ks   []byte
	prf  *PRF
	tset *TSet
}

// SKSSetup sets up the encrypted database for the Single-Keyword SSE
// Scheme (SKS).
func SKSSetup(ks []byte, db map[string][]int) (*SKS, error) {
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

	tset, err := TSetSetup(T)
	if err != nil {
		return nil, err
	}

	return &SKS{
		ks:   ks,
		prf:  prf,
		tset: tset,
	}, nil
}

// Search searches the query and returns a list of matching document
// indices.
func (sks *SKS) Search(query []byte) ([]int, error) {
	stag := sks.tset.GetTag(query, nil)

	t, err := sks.tset.Retrieve(stag)
	if err != nil {
		return nil, err
	}

	ke := sks.prf.Data(query, nil)

	dec, err := NewENC(ke)
	if err != nil {
		return nil, err
	}

	var result []int

	for _, id := range t {
		var plain ID
		dec.Decrypt(plain[:], id[:])
		result = append(result, int(plain.Uint64()))
	}

	return result, nil
}
