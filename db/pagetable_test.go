//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

var pidMeta = []uint16{
	0, 1, 0xffff,
}

var pidPagenum = []uint64{
	0, 1, PIDPagenumMask,
}

func TestPhysicalID(t *testing.T) {
	for _, meta := range pidMeta {
		for _, pagenum := range pidPagenum {
			pid := NewPhysicalID(meta, pagenum)
			if pid.Meta() != meta {
				t.Errorf("meta: got=%v, expected=%v", pid.Meta(), meta)
			}
			if pid.Pagenum() != pagenum {
				t.Errorf("pagenum: got=%v, expected=%v", pid.Pagenum(), pagenum)
			}
		}
	}
}

var idMeta = []byte{
	0, 1, 2, 3,
}

var idObjectID = []uint16{
	0, 1, 0x3fff,
}

var idPagenum = []uint64{
	0, 1, IDPagenumMask,
}

func TestLogicalID(t *testing.T) {
	for _, meta := range idMeta {
		for _, objectID := range idObjectID {
			for _, pagenum := range idPagenum {
				id := NewLogicalID(meta, objectID, pagenum)
				if id.Meta() != meta {
					t.Errorf("meta: got=%v, expected=%v", id.Meta(), meta)
				}
				if id.ObjectID() != objectID {
					t.Errorf("objectID: got=%v, expected=%v",
						id.ObjectID(), objectID)
				}
				if id.Pagenum() != pagenum {
					t.Errorf("pagenum: got=%v, expected=%v",
						id.Pagenum(), pagenum)
				}
			}
		}
	}
}

func newTestDevice() (Device, error) {
	if false {
		// XXX syscall.O_DIRECT
		return os.OpenFile(",test.shades", os.O_RDWR|os.O_CREATE, 0644)
	}
	return NewMemDevice(1024 * 1024 * 1024), nil
}

func TestPageTableOpen(t *testing.T) {
	device, err := newTestDevice()
	if err != nil {
		t.Fatal(err)
	}
	params := NewParams()

	db, err := Create(params, device)
	if err != nil {
		t.Fatal(err)
	}

	pid, err := db.pt.get(nil,
		NewLogicalID(0, 0, uint64(db.pt.root1.numPages())))
	if err == nil {
		t.Fatalf("got invalid page")
	}
	_ = pid

	params.PageSize = 1024
	_, err = Open(params, device)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt n-1 bytes in the root block.
	dev, ok := device.(*MemDevice)
	if ok {
		params = NewParams()
		count := params.PageSize/RootPtrSize - 1
		for i := 0; i < count; i++ {
			idx := rand.Int() % params.PageSize
			dev.buf[idx]++
		}
		_, err = Open(params, device)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPageTableLevels(t *testing.T) {
	device, err := newTestDevice()
	if err != nil {
		t.Fatal(err)
	}
	params := NewParams()
	params.PageSize = 1024

	db, err := Create(params, device)
	if err != nil {
		t.Fatal(err)
	}

	tr, err := db.NewTransaction(true)
	if err != nil {
		t.Fatal(err)
	}

	perPage := db.pt.root0.idsPerPage()

	// Create 3 levels of page table.
	count := perPage * perPage * perPage

	fmt.Printf("perPage: %v, count: %v\n", perPage, count)

	const lmeta = 0x3
	const lobj = 0xfff

	const pmeta = 0xde

	// Map first and last ID of each leaf page.
	for i := 0; i < count; i += perPage {
		if i != 0 {
			err = db.pt.set(tr, NewLogicalID(lmeta, lobj, uint64(i)),
				NewPhysicalID(pmeta, uint64(i)))
			if err != nil {
				t.Fatal(err)
			}
		}
		last := uint64(i + perPage - 1)
		err = db.pt.set(tr, NewLogicalID(lmeta, lobj, last),
			NewPhysicalID(pmeta, last))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = tr.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Open database and verify mappings.

	db, err = Open(params, device)
	if err != nil {
		t.Fatal(err)
	}

	tr, err = db.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < count; i += perPage {
		if i != 0 {
			pid, err := db.pt.get(tr, NewLogicalID(lmeta, lobj, uint64(i)))
			if err != nil {
				t.Fatal(err)
			}
			if pid.Meta() != pmeta {
				t.Errorf("ID %v mapped to meta %v, expected %v\n",
					i, pid.Meta(), pmeta)
			}
			if pid.Pagenum() != uint64(i) {
				t.Errorf("ID %v mapped to %v, expected %v\n",
					i, pid.Pagenum(), i)
			}
		}
		last := uint64(i + perPage - 1)
		pid, err := db.pt.get(tr, NewLogicalID(lmeta, lobj, last))
		if err != nil {
			t.Fatal(err)
		}
		if pid.Meta() != pmeta {
			t.Errorf("ID %v mapped to meta %v, expected %v\n",
				i, pid.Meta(), 0xde)
		}
		if pid.Pagenum() != last {
			t.Errorf("ID %v mapped to %v, expected %v\n",
				i, pid.Pagenum(), last)
		}
	}
}
