//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"testing"
)

func TestCache(t *testing.T) {
	db, err := newDB(NewParams(), &MemDevice{})
	if err != nil {
		t.Fatal(err)
	}
	_ = db
}
