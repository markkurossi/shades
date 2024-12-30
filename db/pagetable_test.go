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

func TestPageTableOpen(t *testing.T) {
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
	device := NewMemDevice(1024 * 1024 * 1024)
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

	// Map first and last ID of each leaf page.
	for i := 0; i < count; i += perPage {
		err = db.pt.set(tr, LogicalID(i), PhysicalID(i+1))
		if err != nil {
			t.Fatal(err)
		}
		last := i + perPage - 1
		err = db.pt.set(tr, LogicalID(last), PhysicalID(last+1))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = tr.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Open database and verify mappings.

	if true {
		db, err = Open(params, device)
		if err != nil {
			t.Fatal(err)
		}
	}

	tr, err = db.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < count; i += perPage {
		pid, err := db.pt.get(tr, LogicalID(i))
		if err != nil {
			t.Fatal(err)
		}
		if pid.Pagenum() != uint64(i+1) {
			t.Errorf("ID %v mapped to %v, expected %v\n", i, pid, i)
		}
		last := i + perPage - 1
		pid, err = db.pt.get(tr, LogicalID(last))
		if err != nil {
			t.Fatal(err)
		}
	}
}
