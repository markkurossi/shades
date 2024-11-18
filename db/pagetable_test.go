//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
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

func TestPageTableGet(t *testing.T) {
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

	pid, err := db.pt.Get(nil, NewLogicalID(0, 0, uint64(db.pt.numPages)))
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
