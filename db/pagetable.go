//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/markkurossi/shades/crypto"
)

var (
	bo = binary.BigEndian
)

// PhysicalID defines a physical page ID.
type PhysicalID uint64

// PhysicalID component masks.
const (
	PIDMetaMask    uint64 = 0xffff000000000000
	PIDPagenumMask uint64 = 0x0000ffffffffffff
)

// NewPhysicalID creates a new physical page ID from the arguments.
func NewPhysicalID(meta uint16, pagenum uint64) PhysicalID {
	if pagenum&PIDMetaMask != 0 {
		panic("physical page number too big")
	}
	return PhysicalID(uint64(meta)<<48 | pagenum&PIDPagenumMask)
}

// Meta returns the meta field of the physical page ID.
func (pid PhysicalID) Meta() uint16 {
	return uint16(pid >> 48)
}

// Pagenum returns the page number of the physical page ID.
func (pid PhysicalID) Pagenum() uint64 {
	return uint64(pid) & PIDPagenumMask
}

func (pid PhysicalID) String() string {
	return fmt.Sprintf("%04x:%012x", pid.Meta(), pid.Pagenum())
}

// LogicalID defines a logical page ID.
type LogicalID uint64

// LogicalID component masks.
const (
	IDMetaMask     uint64 = 0xc000000000000000
	IDObjectIDMask uint64 = 0x3fff000000000000
	IDPagenumMask  uint64 = 0x0000ffffffffffff
)

// NewLogicalID creates a new logical page ID from the arguments.
func NewLogicalID(meta byte, objectID uint16, pagenum uint64) LogicalID {
	if meta&0xfc != 0 {
		panic("logical page meta too big")
	}
	if objectID&0xc000 != 0 {
		panic("logical page object ID too big")
	}
	if pagenum&(IDMetaMask|IDObjectIDMask) != 0 {
		panic("logical page number too big")
	}
	return LogicalID(uint64(meta)<<62 | uint64(objectID)<<48 | pagenum)
}

// Meta returns the meta field of the logical page ID.
func (id LogicalID) Meta() byte {
	return byte(id >> 62)
}

// ObjectID returns the object ID of the logical page ID.
func (id LogicalID) ObjectID() uint16 {
	return uint16((uint64(id) & IDObjectIDMask) >> 48)
}

// Pagenum returns the page number of the logical page ID.
func (id LogicalID) Pagenum() uint64 {
	return uint64(id) & IDPagenumMask
}

func (id LogicalID) String() string {
	return fmt.Sprintf("%d:%04x:%012x",
		id.Meta(), id.ObjectID(), id.Pagenum())
}

const (
	// RootBlock defines the physical ID of the database root block.
	RootBlock PhysicalID = 0

	// RootPtrMagic defines the root pointer magic number.
	RootPtrMagic = uint64(0x7b5368616465737d)
)

// RootPtrPadding defines the padding data, which is used to pad the
// root block into page boundary.
var RootPtrPadding = []rune("mtr@iki.fi~")

// PageTable maps logical page numbers to physical page numbers. This
// mapping is based on LogicalID.Pagenum(), meaning that the Meta and
// ObjectID fields are not stored in the page table; instead, they
// must be managed by higher-level objects and data structures.
type PageTable struct {
	db         *DB
	perPage    int
	depth      int
	numPages   int
	root       PhysicalID
	freelist   PhysicalID
	generation uint64
	rootBlock  *PageRef
	hash       *crypto.PRF
}

// NewPageTable creates a new page table for the database.
func NewPageTable(db *DB) (*PageTable, error) {
	var err error

	pt := &PageTable{
		db:      db,
		perPage: db.params.PageSize / 8,
	}

	var hashKey [16]byte
	pt.hash, err = crypto.NewPRF(hashKey[:])
	if err != nil {
		return nil, err
	}

	pt.setDepth(1)

	pt.rootBlock, err = db.cache.Get(RootBlock)
	if err != nil {
		return nil, err
	}

	return pt, nil
}

func (pt *PageTable) setDepth(depth int) {
	pt.depth = depth
	pt.numPages = 1
	for ; depth > 0; depth-- {
		pt.numPages *= pt.perPage
	}
}

// Init initializes a new page table for the database.
func (pt *PageTable) Init() error {
	pt.formatRootBlock()
	return pt.rootBlock.flush()
}

func (pt *PageTable) formatRootBlock() {
	buf := pt.rootBlock.Data()
	now := time.Now()

	// Format the first root pointer.
	bo.PutUint64(buf[0:], RootPtrMagic)
	bo.PutUint64(buf[8:], 0)
	bo.PutUint64(buf[16:], uint64(pt.root))
	bo.PutUint64(buf[24:], uint64(pt.freelist))
	bo.PutUint64(buf[32:], 0) // Snapshots
	bo.PutUint64(buf[40:], uint64(now.UnixNano()))
	bo.PutUint64(buf[48:], pt.generation)
	bo.PutUint64(buf[56:], 0) // UserData

	pt.hash.Data(buf[0:64], buf[:64])

	var i int
	for i = 80; i+80 < pt.db.params.PageSize; i += 80 {
		copy(buf[i:], buf[0:80])
	}
	for ; i < pt.db.params.PageSize; i++ {
		buf[i] = byte(RootPtrPadding[i%len(RootPtrPadding)])
	}

	if false {
		fmt.Printf("RootBlock:\n%s", hex.Dump(buf))
	}
}

// Get maps the logical ID to its current physical ID.
func (pt *PageTable) Get(tr *Transaction, id LogicalID) (PhysicalID, error) {
	pagenum := id.Pagenum()

	if pagenum >= uint64(pt.numPages) {
		return 0, fmt.Errorf("unmapped page %s", id)
	}

	return 0, fmt.Errorf("PageTable.Get not implemented yet")
}

// Set updates the mapping from the logical ID id to the physical ID
// pid.
func (pt *PageTable) Set(tr *Transaction, id LogicalID, pid PhysicalID) error {
	return fmt.Errorf("PageTable.Set not implemented yet")
}

// RootPointer implements the database root, which contains
// information about the database state, snapshots, and high-level
// data. It is written atomically to the first storage page.
type RootPointer struct {
	Magic      uint64
	Flags      uint64
	PageTable  PhysicalID
	Freelist   PhysicalID
	Snapshots  PhysicalID
	Timestamp  uint64
	Generation uint64
	UserData   uint64
	Checksum   [16]byte
}
