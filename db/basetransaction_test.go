//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"os"
	"testing"
)

func TestTrBasic(t *testing.T) {
	var device Device
	var err error

	if false {
		// XXX syscall.O_DIRECT
		device, err = os.OpenFile(",test.shades", os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		device = NewMemDevice(1024 * 1024)
	}
	params := NewParams()
	params.PageSize = 1024

	db, err := Create(params, device)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := db.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.NewTransaction(false)
	if err == nil {
		t.Fatal("concurrent base transaction allowed")
	}

	_, _, err = tr.NewPage()
	if err == nil {
		t.Errorf("NewPage succeeded for read-only transaction")
	}
	err = tr.Commit()
	if err != nil {
		t.Error(err)
	}

	if true {
		tr, err = db.NewTransaction(true)
		if err != nil {
			t.Fatal(err)
		}
		ref, id, err := tr.NewPage()
		if err != nil {
			t.Fatal(err)
		}

		buf := ref.Data()
		bo.PutUint64(buf, uint64(id))
		for i := 8; i < len(buf); i++ {
			buf[i] = byte(i)
		}
		ref.Release()

		err = tr.Commit()
		if err != nil {
			t.Error(err)
		}

		// Read page and verify data.
		tr, err := db.NewTransaction(false)
		if err != nil {
			t.Fatal(err)
		}
		ref, err = tr.ReadablePage(id)
		if err != nil {
			t.Fatal(err)
		}
		buf = ref.Read()
		if bo.Uint64(buf) != uint64(id) {
			t.Errorf("page ID mismatch: got %v, expected %v",
				bo.Uint64(buf), id)
			for i := 8; i < len(buf); i++ {
				if buf[i] != byte(i) {
					t.Errorf("data[%v]: got %v, expected %v",
						i, buf[i], i)
				}
			}
		}

		ref.Release()

		err = tr.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}
}
