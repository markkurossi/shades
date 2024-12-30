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
	root0     RootPointer
	root1     RootPointer
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
	var err error

	pt.rootBlock, err = pt.db.cache.New(RootBlock, nil)
	if err != nil {
		return err
	}
	_ = pt.rootBlock.Data()

	pageTable := NewPhysicalID(0, 1)
	ref, err := pt.db.cache.New(pageTable, nil)
	if err != nil {
		return err
	}
	_ = ref.Data()
	ref.Release()

	pt.root0 = RootPointer{
		Magic:        RootPtrMagic,
		Depth:        1,
		PageSize:     uint32(pt.db.params.PageSize),
		Generation:   1,
		NextPhysical: 2, // 0=RootBlock, 1=PageTable
		NextLogical:  1, // 0 is reserved for unallocated pages
		PageTable:    pageTable,
		Freelist:     0,
	}

	pt.formatRootBlock(&pt.root0, pt.rootBlock.Data())

	err = pt.db.cache.flush()
	if err != nil {
		return err
	}
	err = pt.db.device.Sync()
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

func (pt *PageTable) formatRootBlock(root *RootPointer, buf []byte) {

	root.Timestamp = uint64(time.Now().UnixNano())

	// Format the first root pointer.
	bo.PutUint64(buf[RootPtrOfsMagic:], root.Magic)
	bo.PutUint16(buf[RootPtrOfsFlags:], root.Flags)
	bo.PutUint16(buf[RootPtrOfsDepth:], root.Depth)
	bo.PutUint32(buf[RootPtrOfsPageSize:], root.PageSize)
	bo.PutUint64(buf[RootPtrOfsTimestamp:], root.Timestamp)
	bo.PutUint64(buf[RootPtrOfsGeneration:], root.Generation)
	bo.PutUint64(buf[RootPtrOfsNextPhysial:], root.NextPhysical)
	bo.PutUint64(buf[RootPtrOfsNextLogical:], root.NextLogical)
	bo.PutUint64(buf[RootPtrOfsPageTable:], uint64(root.PageTable))
	bo.PutUint64(buf[RootPtrOfsFreelist:], uint64(root.Freelist))
	bo.PutUint64(buf[RootPtrOfsSnapshots:], uint64(root.Snapshots))
	bo.PutUint64(buf[RootPtrOfsUserData:], root.UserData)

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
	var root RootPointer

	for i := 0; i+RootPtrSize < len(buf); i += RootPtrSize {
		gen := bo.Uint64(buf[i+RootPtrOfsGeneration:])
		if gen <= root.Generation {
			continue
		}
		rp, err := pt.parseRootPointer(buf[i : i+RootPtrSize])
		if err != nil {
			continue
		}
		root = rp
	}
	if root.Generation == 0 {
		return fmt.Errorf("no valid root pointer found")
	}
	pt.root0 = root
	if false {
		fmt.Printf("%v\n", pt.root0)
	}

	return nil
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

func (pt *PageTable) newTransaction(rw bool) (*BaseTransaction, error) {
	if pt.root1.Generation > pt.root0.Generation {
		return nil, fmt.Errorf("base transaction already started")
	}
	pt.root1 = pt.root0
	pt.root1.Generation++

	tr := &BaseTransaction{
		pt: pt,
		rw: rw,
	}
	if rw {
		tr.writable = make(map[PhysicalID]PhysicalID)
	}
	return tr, nil
}

func (pt *PageTable) commit(tr *BaseTransaction) error {
	if !tr.rw {
		pt.root1.Generation = pt.root0.Generation
		return nil
	}

	fmt.Printf("PageTable.commit: root0:\n%v\n", pt.root0)
	fmt.Printf("root1:\n%v\n", pt.root1)

	buf := pt.rootBlock.Data()
	pt.formatRootBlock(&pt.root1, buf)

	err := pt.db.cache.flush()
	if err != nil {
		return err
	}
	err = pt.db.device.Sync()
	if err != nil {
		return err
	}

	pt.root0 = pt.root1

	return nil
}

func (pt *PageTable) abort(tr *BaseTransaction) error {
	pt.root1.Generation = pt.root0.Generation
	return nil
}

func (pt *PageTable) allocLogicalID() (LogicalID, error) {
	// XXX LogicalID freelist.

	pagenum := pt.root1.NextLogical
	pt.root1.NextLogical++

	return NewLogicalID(0, 0, pagenum), nil
}

func (pt *PageTable) freeLogicalID(id LogicalID) error {
	return fmt.Errorf("PageTable.freeLogicalID not implemented yet")
}

func (pt *PageTable) allocPhysicalID() (PhysicalID, error) {
	// XXX PhysicalID freelist

	pagenum := pt.root1.NextPhysical
	pt.root1.NextPhysical++

	return NewPhysicalID(0, pagenum), nil
}

func (pt *PageTable) freePhysicalID(pid PhysicalID) error {
	return fmt.Errorf("PageTable.freePhysicalID not implemented yet")
}

// Get maps the logical ID to its current physical ID.
func (pt *PageTable) get(tr *BaseTransaction, id LogicalID) (
	PhysicalID, error) {

	pagenum := id.Pagenum()

	if pagenum >= uint64(pt.root1.numPages()) {
		return 0, fmt.Errorf("unmapped page %v", id)
	}

	perPage := uint64(pt.root1.idsPerPage())

	var perID uint64 = 1
	var depth int
	for depth = int(pt.root1.Depth); depth > 1; depth-- {
		perID *= perPage
	}

	// Traverse page table.

	pageTable := pt.root1.PageTable
	ref, err := pt.db.cache.Get(pageTable)
	if err != nil {
		return 0, err
	}

	for depth = int(pt.root1.Depth); depth > 1; depth-- {
		idx := pagenum / perID
		pagenum = pagenum % perID

		perID /= perPage

		buf := ref.Read()
		pageTable = PhysicalID(bo.Uint64(buf[idx*8:]))
		ref.Release()

		if pageTable.Pagenum() == 0 {
			return 0, fmt.Errorf("unmapped page %v", id)
		}

		ref, err = pt.db.cache.Get(pageTable)
		if err != nil {
			return 0, err
		}
	}

	buf := ref.Read()
	pid := PhysicalID(bo.Uint64(buf[pagenum*8:]))
	ref.Release()

	if pid.Pagenum() == 0 {
		return 0, fmt.Errorf("unmapped page %v", id)
	}

	return pid, nil
}

// Set updates the mapping from the logical ID id to the physical ID
// pid.
func (pt *PageTable) set(tr *BaseTransaction, id LogicalID,
	pid PhysicalID) error {

	pagenum := id.Pagenum()

	for pagenum >= uint64(pt.root1.numPages()) {
		// Increase page table depth.
		pageTable, err := pt.allocPhysicalID()
		if err != nil {
			return err
		}
		ref, err := pt.db.cache.New(pageTable, nil)
		if err != nil {
			pt.freePhysicalID(pageTable)
			return err
		}
		tr.writable[pageTable] = 0
		buf := ref.Data()
		bo.PutUint64(buf, uint64(pt.root1.PageTable))
		ref.Release()

		pt.root1.PageTable = pageTable
		pt.root1.Depth++
	}

	perPage := uint64(pt.root1.idsPerPage())

	var perID uint64 = 1
	var depth int
	for depth = int(pt.root1.Depth); depth > 1; depth-- {
		perID *= perPage
	}

	// Traverse page table.

	pageTable := pt.root1.PageTable
	ref, pageTable, err := pt.writable(tr, pageTable)
	if err != nil {
		return err
	}
	pt.root1.PageTable = pageTable

	for depth = int(pt.root1.Depth); depth > 1; depth-- {
		idx := pagenum / perID
		pagenum = pagenum % perID

		perID /= perPage

		buf := ref.Data()
		pageTable = PhysicalID(bo.Uint64(buf[idx*8:]))

		var nref *PageRef
		if pageTable.Pagenum() == 0 {
			// On-demand allocate missing page table pages.
			pageTable, err = pt.allocPhysicalID()
			if err != nil {
				ref.Release()
				return err
			}
			nref, err = pt.db.cache.New(pageTable, nil)
			if err != nil {
				pt.freePhysicalID(pageTable)
				ref.Release()
				return err
			}
			tr.writable[pageTable] = 0

		} else {
			nref, pageTable, err = pt.writable(tr, pageTable)
			if err != nil {
				ref.Release()
				return err
			}
		}
		bo.PutUint64(buf[idx*8:], uint64(pageTable))
		ref.Release()

		ref = nref
	}

	buf := ref.Data()
	bo.PutUint64(buf[pagenum*8:], uint64(pid))
	ref.Release()

	return nil
}

func (pt *PageTable) writable(tr *BaseTransaction, pid PhysicalID) (
	*PageRef, PhysicalID, error) {
	_, ok := tr.writable[pid]
	if ok {
		ref, err := pt.db.cache.Get(pid)
		if err != nil {
			return nil, 0, err
		}
		return ref, pid, nil
	}

	// Copy page table page.

	newPid, err := pt.allocPhysicalID()
	if err != nil {
		return nil, 0, err
	}
	oldRef, err := tr.cache.Get(pid)
	if err != nil {
		pt.freePhysicalID(newPid)
		return nil, 0, err
	}
	defer oldRef.Release()

	newRef, err := pt.db.cache.New(newPid, oldRef.Read())
	if err != nil {
		pt.freePhysicalID(newPid)
		return nil, 0, err
	}
	tr.writable[newPid] = pid

	return newRef, newPid, nil
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

func (rp RootPointer) idsPerPage() int {
	return int(rp.PageSize / 8)
}

func (rp RootPointer) numPages() int {
	perPage := rp.idsPerPage()
	numPages := 1

	for depth := rp.Depth; depth > 0; depth-- {
		numPages *= perPage
	}

	return numPages
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
