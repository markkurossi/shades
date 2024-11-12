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

func TestPageTableGet(t *testing.T) {
	// XXX syscall.O_DIRECT
	f, err := os.OpenFile(",test.shades", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatal(err)
	}
	params := NewParams()

	db, err := newDB(params, f)
	if err != nil {
		t.Fatal(err)
	}
	err = db.pt.Init()
	if err != nil {
		t.Fatal(err)
	}

	pid, err := db.pt.Get(nil, NewLogicalID(0, 0, uint64(db.pt.numPages)))
	if err == nil {
		t.Fatalf("got invalid page")
	}
	_ = pid

	db2, err := Open(params, f)
	if err != nil {
		t.Fatal(err)
	}
	_ = db2
}
