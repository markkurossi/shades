//
// Copyright (c) 2024 Markku Rossi
//
// All rights reserved.
//

package db

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/markkurossi/shades/crypto"
	"github.com/markkurossi/tabulate"
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

// Root pointer offsets.
const (
	RootPtrOfsMagic       = 0
	RootPtrOfsFlags       = 8
	RootPtrOfsDepth       = 10
	RootPtrOfsPageSize    = 12
	RootPtrOfsTimestamp   = 16
	RootPtrOfsGeneration  = 24
	RootPtrOfsNextPhysial = 32
	RootPtrOfsNextLogical = 40
	RootPtrOfsPageTable   = 48
	RootPtrOfsFreelist    = 56
	RootPtrOfsSnapshots   = 64
	RootPtrOfsUserData    = 72
	RootPtrOfsChecksum    = 80
	RootPtrSize           = 96
)

// RootPtrPadding defines the padding data, which is used to pad the
// root block into page boundary.
var RootPtrPadding = []rune("mtr@iki.fi~")

// PageTable maps logical page numbers to physical page numbers. This
// mapping is based on LogicalID.Pagenum(), meaning that the Meta and
// ObjectID fields are not stored in the page table; instead, they
// must be managed by higher-level objects and data structures.
type PageTable struct {
	db        *DB
	numPages  int
	root      RootPointer
	rootBlock *PageRef
	hash      *crypto.PRF
}

// NewPageTable creates a new page table for the database.
func NewPageTable(db *DB) (*PageTable, error) {
	var err error

	pt := &PageTable{
		db: db,
	}

	var hashKey [16]byte
	pt.hash, err = crypto.NewPRF(hashKey[:])
	if err != nil {
		return nil, err
	}

	return pt, nil
}

// Init initializes a new page table for the database.
func (pt *PageTable) Init() error {
	pt.root.Magic = RootPtrMagic
	pt.root.Depth = 1
	pt.root.PageSize = uint32(pt.db.params.PageSize)
	pt.root.Generation = 1
	pt.root.NextPhysical = 2 // 0=RootBlock, 1=PageTable
	pt.root.NextLogical = 1  // 0 is reserved for unallocated pages
	pt.root.PageTable = NewPhysicalID(0, 1)
	pt.root.Freelist = 0

	pt.init()

	buf := make([]byte, pt.db.params.PageSize)

	pt.formatRootBlock(buf)
	_, err := pt.db.device.WriteAt(buf, 0)
	if err != nil {
		return err
	}
	err = pt.db.device.Sync()
	if err != nil {
		return err
	}
	pt.rootBlock, err = pt.db.cache.Get(RootBlock)
	if err != nil {
		return err
	}

	return nil
}

// Open reads the page table table from the device.
func (pt *PageTable) Open() error {
	var err error

	pt.rootBlock, err = pt.db.cache.Get(RootBlock)
	if err != nil {
		return err
	}

	err = pt.parseRootBlock(pt.rootBlock.Read())
	if err != nil {
		return err
	}
	return nil
}

func (pt *PageTable) formatRootBlock(buf []byte) {

	pt.root.Timestamp = uint64(time.Now().UnixNano())

	// Format the first root pointer.
	bo.PutUint64(buf[RootPtrOfsMagic:], pt.root.Magic)
	bo.PutUint16(buf[RootPtrOfsFlags:], pt.root.Flags)
	bo.PutUint16(buf[RootPtrOfsDepth:], pt.root.Depth)
	bo.PutUint32(buf[RootPtrOfsPageSize:], pt.root.PageSize)
	bo.PutUint64(buf[RootPtrOfsTimestamp:], pt.root.Timestamp)
	bo.PutUint64(buf[RootPtrOfsGeneration:], pt.root.Generation)
	bo.PutUint64(buf[RootPtrOfsNextPhysial:], pt.root.NextPhysical)
	bo.PutUint64(buf[RootPtrOfsNextLogical:], pt.root.NextLogical)
	bo.PutUint64(buf[RootPtrOfsPageTable:], uint64(pt.root.PageTable))
	bo.PutUint64(buf[RootPtrOfsFreelist:], uint64(pt.root.Freelist))
	bo.PutUint64(buf[RootPtrOfsSnapshots:], uint64(pt.root.Snapshots))
	bo.PutUint64(buf[RootPtrOfsUserData:], pt.root.UserData)

	pt.hash.Data(buf[0:RootPtrOfsChecksum], buf[:RootPtrOfsChecksum])

	var i int = RootPtrSize
	for ; i+RootPtrSize < pt.db.params.PageSize; i += RootPtrSize {
		copy(buf[i:], buf[0:RootPtrSize])
	}
	for ; i < pt.db.params.PageSize; i++ {
		buf[i] = byte(RootPtrPadding[i%len(RootPtrPadding)])
	}
}

func (pt *PageTable) parseRootBlock(buf []byte) error {
	if false {
		fmt.Printf("RootBlock:\n%s", hex.Dump(buf))
	}

	for i := 0; i+RootPtrSize < len(buf); i += RootPtrSize {
		gen := bo.Uint64(buf[i+RootPtrOfsGeneration:])
		if gen <= pt.root.Generation {
			continue
		}
		rp, err := pt.parseRootPointer(buf[i : i+RootPtrSize])
		if err != nil {
			continue
		}
		pt.root = rp
	}
	if pt.root.Generation == 0 {
		return fmt.Errorf("no valid root pointer found")
	}

	if false {
		fmt.Printf("%v\n", pt.root)
	}
	pt.init()

	return nil
}

func (pt *PageTable) init() {
	pt.numPages = 1

	perPage := int(pt.root.PageSize / 8)

	for depth := pt.root.Depth; depth > 0; depth-- {
		pt.numPages *= perPage
	}
}

func (pt *PageTable) parseRootPointer(buf []byte) (RootPointer, error) {
	var checksum [16]byte

	pt.hash.Data(buf[0:RootPtrOfsChecksum], checksum[:0])
	if bytes.Compare(checksum[:], buf[RootPtrOfsChecksum:]) != 0 {
		return RootPointer{}, fmt.Errorf("invalid root pointer checksum")
	}
	return RootPointer{
		Magic:        bo.Uint64(buf[RootPtrOfsMagic:]),
		Flags:        bo.Uint16(buf[RootPtrOfsFlags:]),
		Depth:        bo.Uint16(buf[RootPtrOfsDepth:]),
		PageSize:     bo.Uint32(buf[RootPtrOfsPageSize:]),
		Timestamp:    bo.Uint64(buf[RootPtrOfsTimestamp:]),
		Generation:   bo.Uint64(buf[RootPtrOfsGeneration:]),
		NextPhysical: bo.Uint64(buf[RootPtrOfsNextPhysial:]),
		NextLogical:  bo.Uint64(buf[RootPtrOfsNextLogical:]),
		PageTable:    PhysicalID(bo.Uint64(buf[RootPtrOfsPageTable:])),
		Freelist:     PhysicalID(bo.Uint64(buf[RootPtrOfsFreelist:])),
		Snapshots:    PhysicalID(bo.Uint64(buf[RootPtrOfsSnapshots:])),
		UserData:     bo.Uint64(buf[RootPtrOfsUserData:]),
	}, nil
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
	Magic        uint64
	Flags        uint16
	Depth        uint16
	PageSize     uint32
	Timestamp    uint64
	Generation   uint64
	NextPhysical uint64
	NextLogical  uint64
	PageTable    PhysicalID
	Freelist     PhysicalID
	Snapshots    PhysicalID
	UserData     uint64
	Checksum     [16]byte
}

func (rp RootPointer) String() string {
	tab := tabulate.New(tabulate.UnicodeLight)
	tab.Header("Field")
	tab.Header("Value").SetAlign(tabulate.MR)

	row := tab.Row()
	row.Column("Magic")
	row.Column(fmt.Sprintf("%x", rp.Magic))

	row = tab.Row()
	row.Column("Flags")
	row.Column(fmt.Sprintf("%016b", rp.Flags))

	row = tab.Row()
	row.Column("Depth")
	row.Column(fmt.Sprintf("%v", rp.Depth))

	row = tab.Row()
	row.Column("PageSize")
	row.Column(fmt.Sprintf("%v", rp.PageSize))

	row = tab.Row()
	row.Column("Timestamp")
	row.Column(fmt.Sprintf("%v", rp.Timestamp))

	row = tab.Row()
	row.Column("Generation")
	row.Column(fmt.Sprintf("%v", rp.Generation))

	row = tab.Row()
	row.Column("NextPhysical")
	row.Column(fmt.Sprintf("%v", rp.NextPhysical))

	row = tab.Row()
	row.Column("NextLogical")
	row.Column(fmt.Sprintf("%v", rp.NextLogical))

	row = tab.Row()
	row.Column("PageTable")
	row.Column(fmt.Sprintf("%v", rp.PageTable))

	row = tab.Row()
	row.Column("Freelist")
	row.Column(fmt.Sprintf("%v", rp.Freelist))

	row = tab.Row()
	row.Column("Snapshots")
	row.Column(fmt.Sprintf("%v", rp.Snapshots))

	row = tab.Row()
	row.Column("UserData")
	row.Column(fmt.Sprintf("%v", rp.UserData))

	return tab.String()
}
