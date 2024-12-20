//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"fmt"
)

// Cache implements page cache.
type Cache struct {
	db     *DB
	buffer []byte
	lru    []PageRef
	clock  int
	cached map[PhysicalID]*PageRef
}

// NewCache creates a new cache for the database.
func NewCache(db *DB) (*Cache, error) {
	mem := 128 * 1024 * 1024
	pageSize := db.params.PageSize
	numRefs := mem / pageSize

	cache := &Cache{
		db:     db,
		buffer: make([]byte, mem),
		lru:    make([]PageRef, numRefs),
		cached: make(map[PhysicalID]*PageRef),
	}
	for i := 0; i < numRefs; i++ {
		cache.lru[i].db = db
		cache.lru[i].data = cache.buffer[i*pageSize : (i+1)*pageSize]
	}
	return cache, nil
}

// Get gets a page reference for the physical page.
func (cache *Cache) Get(pid PhysicalID) (*PageRef, error) {
	var err error

	ref, ok := cache.cached[pid]
	if !ok {
		ref, err = cache.newRef()
		if err != nil {
			return nil, err
		}
		cache.cached[pid] = ref
		ref.pid = pid

		err = ref.read()
		if err != nil {
			return nil, err
		}
	}
	if ref.pid != pid {
		panic("cached PageRef has invalid PhysicalID")
	}
	ref.refcount++

	return ref, nil
}

// New gets an empty page reference for the new physical page.
func (cache *Cache) New(pid PhysicalID, init []byte) (*PageRef, error) {
	_, ok := cache.cached[pid]
	if ok {
		return nil, fmt.Errorf("page %v is not new", pid)
	}
	ref, err := cache.newRef()
	if err != nil {
		return nil, err
	}
	cache.cached[pid] = ref
	ref.pid = pid

	n := copy(ref.data, init)
	for i := n; i < len(ref.data); i++ {
		ref.data[i] = 0
	}
	ref.refcount++

	return ref, nil
}

func (cache *Cache) flush() error {
	for _, ref := range cache.cached {
		err := ref.flush()
		if err != nil {
			return err
		}
	}

	return nil
}

func (cache *Cache) newRef() (*PageRef, error) {
	start := cache.clock
	for {
		ref := &cache.lru[cache.clock]
		if ref.refcount == 0 {
			// Don't flush and uncache zero pids since they mark an
			// unallocated page, but the zero pid is also used for the
			// root pointer.
			if ref.pid != 0 {
				err := ref.flush()
				if err != nil {
					return nil, err
				}
				delete(cache.cached, ref.pid)
			}
			return ref, nil
		}
		cache.clock++
		cache.clock %= len(cache.lru)
		if cache.clock == start {
			return nil, fmt.Errorf("working set too big")
		}
	}
}

// PageRef implements a reference to physical page.
type PageRef struct {
	db       *DB
	pid      PhysicalID
	data     []byte
	refcount int32
	dirty    bool
}

func (ref *PageRef) String() string {
	return fmt.Sprintf("pid=%v, refcount=%v, dirty=%v",
		ref.pid, ref.refcount, ref.dirty)
}

// Release releases the page reference.
func (ref *PageRef) Release() {
	if ref.refcount <= 0 {
		panic("releasing unreferenced page")
	}
	ref.refcount--
}

// Read returns the page data in read-only mode.
func (ref *PageRef) Read() []byte {
	return ref.data
}

// Data returns the page data in read-write mode i.e. the page is
// marked dirty and it will be flushed to storage when the transaction
// commits.
func (ref *PageRef) Data() []byte {
	ref.dirty = true
	return ref.Read()
}

func (ref *PageRef) read() error {
	if ref.dirty {
		panic("loading dirty page reference")
	}
	off := int64(ref.pid.Pagenum() * uint64(ref.db.params.PageSize))
	_, err := ref.db.device.ReadAt(ref.data, off)

	return err
}

func (ref *PageRef) flush() error {
	if !ref.dirty {
		return nil
	}
	off := int64(ref.pid.Pagenum() * uint64(ref.db.params.PageSize))
	_, err := ref.db.device.WriteAt(ref.data, off)
	if err != nil {
		return err
	}
	ref.dirty = false
	return nil
}
