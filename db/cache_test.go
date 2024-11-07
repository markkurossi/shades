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
	db, err := NewDB(NewParams(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = db
}
