// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nbs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"fmt"

	"github.com/fatih/color"
	"github.com/dolthub/mmap-go"
	"github.com/golang/snappy"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

// CompressedChunk represents a chunk of data in a table file which is still compressed via snappy.
type CompressedChunk struct {
	// H is the hash of the chunk
	H hash.Hash

	// FullCompressedChunk is the entirety of the compressed chunk data including the crc
	FullCompressedChunk []byte

	// CompressedData is just the snappy encoded byte buffer that stores the chunk data
	CompressedData []byte
}

// NewCompressedChunk creates a CompressedChunk
func NewCompressedChunk(h hash.Hash, buff []byte) (CompressedChunk, error) {
	dataLen := uint64(len(buff)) - checksumSize

	chksum := binary.BigEndian.Uint32(buff[dataLen:])
	compressedData := buff[:dataLen]

	if chksum != crc(compressedData) {
		return CompressedChunk{}, errors.New("checksum error")
	}

	return CompressedChunk{H: h, FullCompressedChunk: buff, CompressedData: compressedData}, nil
}

// ToChunk snappy decodes the compressed data and returns a chunks.Chunk
func (cmp CompressedChunk) ToChunk() (chunks.Chunk, error) {
	data, err := snappy.Decode(nil, cmp.CompressedData)

	if err != nil {
		return chunks.Chunk{}, err
	}

	return chunks.NewChunkWithHash(cmp.H, data), nil
}

func ChunkToCompressedChunk(chunk chunks.Chunk) CompressedChunk {
	compressed := snappy.Encode(nil, chunk.Data())
	length := len(compressed)
	compressed = append(compressed, []byte{0, 0, 0, 0}...)
	binary.BigEndian.PutUint32(compressed[length:], crc(compressed[:length]))
	return CompressedChunk{H: chunk.Hash(), FullCompressedChunk: compressed, CompressedData: compressed[:length]}
}

// Hash returns the hash of the data
func (cmp CompressedChunk) Hash() hash.Hash {
	return cmp.H
}

// IsEmpty returns true if the chunk contains no data.
func (cmp CompressedChunk) IsEmpty() bool {
	return len(cmp.CompressedData) == 0 || (len(cmp.CompressedData) == 1 && cmp.CompressedData[0] == 0)
}

var EmptyCompressedChunk CompressedChunk

func init() {
	EmptyCompressedChunk = ChunkToCompressedChunk(chunks.EmptyChunk)
}

// ErrInvalidTableFile is an error returned when a table file is corrupt or invalid.
var ErrInvalidTableFile = errors.New("invalid or corrupt table file")

type onHeapTableIndex struct {
	chunkCount            uint32
	totalUncompressedData uint64
	prefixes, offsets     []uint64
	lengths, ordinals     []uint32
	suffixes              []byte
}

type indexEntry interface {
	Offset() uint64
	Length() uint32
}

type indexResult struct {
	o uint64
	l uint32
}

func (ir indexResult) Offset() uint64 {
	return ir.o
}

func (ir indexResult) Length() uint32 {
	return ir.l
}

// An mmapIndexEntry is an addrSuffix, a BigEndian uint64 for the offset and a
// BigEnding uint32 for the chunk size.
const mmapIndexEntrySize = addrSuffixSize + uint64Size + lengthSize

type mmapOrdinalSlice []mmapOrdinal

func (s mmapOrdinalSlice) Len() int           { return len(s) }
func (s mmapOrdinalSlice) Less(i, j int) bool { return s[i].offset < s[j].offset }
func (s mmapOrdinalSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (i mmapTableIndex) Ordinals() []uint32 {
	s := mmapOrdinalSlice(make([]mmapOrdinal, i.chunkCount))
	for idx := 0; uint32(idx) < i.chunkCount; idx++ {
		mi := idx * mmapIndexEntrySize
		e := mmapIndexEntry(i.data[mi : mi+mmapIndexEntrySize])
		s[idx] = mmapOrdinal{idx, e.Offset()}
	}
	sort.Sort(s)
	res := make([]uint32, i.chunkCount)
	for j, r := range s {
		res[r.idx] = uint32(j)
	}
	return res
}

type mmapTableIndex struct {
	chunkCount            uint32
	totalUncompressedData uint64
	fileSz                uint64
	prefixes              []uint64
	data                  mmap.MMap
	refCnt                *int32
}

func (i mmapTableIndex) Prefixes() []uint64 {
	return i.prefixes
}

type mmapOrdinal struct {
	idx    int
	offset uint64
}

func (i mmapTableIndex) TableFileSize() uint64 {
	return i.fileSz
}

func (i mmapTableIndex) ChunkCount() uint32 {
	return i.chunkCount
}

func (i mmapTableIndex) TotalUncompressedData() uint64 {
	return i.totalUncompressedData
}

func (i mmapTableIndex) Close() error {
	cnt := atomic.AddInt32(i.refCnt, -1)
	if cnt == 0 {
		return i.data.Unmap()
	}
	if cnt < 0 {
		panic("Close() called and reduced ref count to < 0.")
	}
	return nil
}

func (i mmapTableIndex) Clone() tableIndex {
	cnt := atomic.AddInt32(i.refCnt, 1)
	if cnt == 1 {
		panic("Clone() called after last Close(). This index is no longer valid.")
	}
	return i
}

func (i mmapTableIndex) prefixIdx(prefix uint64) (idx uint32) {
	// NOTE: The golang impl of sort.Search is basically inlined here. This method can be called in
	// an extremely tight loop and inlining the code was a significant perf improvement.
	idx, j := 0, i.chunkCount
	for idx < j {
		h := idx + (j-idx)/2 // avoid overflow when computing h
		// i ≤ h < j
		if i.prefixes[h] < prefix {
			idx = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	return
}

func (i mmapTableIndex) Lookup(h *addr) (indexEntry, bool) {
	prefix := binary.BigEndian.Uint64(h[:])
	for idx := i.prefixIdx(prefix); idx < i.chunkCount && i.prefixes[idx] == prefix; idx++ {
		mi := idx * mmapIndexEntrySize
		e := mmapIndexEntry(i.data[mi : mi+mmapIndexEntrySize])
		if bytes.Equal(e.suffix(), h[addrPrefixSize:]) {
			return e, true
		}
	}
	return mmapIndexEntry{}, false
}

func (i mmapTableIndex) EntrySuffixMatches(idx uint32, h *addr) bool {
	mi := idx * mmapIndexEntrySize
	e := mmapIndexEntry(i.data[mi : mi+mmapIndexEntrySize])
	return bytes.Equal(e.suffix(), h[addrPrefixSize:])
}

func (i mmapTableIndex) IndexEntry(idx uint32, a *addr) indexEntry {
	mi := idx * mmapIndexEntrySize
	e := mmapIndexEntry(i.data[mi : mi+mmapIndexEntrySize])
	if a != nil {
		binary.BigEndian.PutUint64(a[:], i.prefixes[idx])
		copy(a[addrPrefixSize:], e.suffix())
	}
	return e
}

type mmapIndexEntry []byte

const mmapIndexEntryOffsetStart = addrSuffixSize
const mmapIndexEntryLengthStart = addrSuffixSize + uint64Size

func (e mmapIndexEntry) suffix() []byte {
	return e[:addrSuffixSize]
}

func (e mmapIndexEntry) Offset() uint64 {
	return binary.BigEndian.Uint64(e[mmapIndexEntryOffsetStart:])
}

func (e mmapIndexEntry) Length() uint32 {
	return binary.BigEndian.Uint32(e[mmapIndexEntryLengthStart:])
}

func mmapOffheapSize(chunks int) int {
	pageSize := 4096
	esz := addrSuffixSize + uint64Size + lengthSize
	min := esz * chunks
	if min%pageSize == 0 {
		return min
	} else {
		return (min/pageSize + 1) * pageSize
	}
}

func newMmapTableIndex(ti onHeapTableIndex, f *os.File) (mmapTableIndex, error) {
	flags := 0
	if f == nil {
		flags = mmap.ANON
	}
	arr, err := mmap.MapRegion(f, mmapOffheapSize(len(ti.ordinals)), mmap.RDWR, flags, 0)
	if err != nil {
		return mmapTableIndex{}, err
	}
	for i := range ti.ordinals {
		idx := i * mmapIndexEntrySize
		si := addrSuffixSize * ti.ordinals[i]
		copy(arr[idx:], ti.suffixes[si:si+addrSuffixSize])
		binary.BigEndian.PutUint64(arr[idx+mmapIndexEntryOffsetStart:], ti.offsets[ti.ordinals[i]])
		binary.BigEndian.PutUint32(arr[idx+mmapIndexEntryLengthStart:], ti.lengths[ti.ordinals[i]])
	}

	refCnt := new(int32)
	*refCnt = 1
	return mmapTableIndex{
		ti.chunkCount,
		ti.totalUncompressedData,
		ti.TableFileSize(),
		ti.Prefixes(),
		arr,
		refCnt,
	}, nil
}

type tableReaderAt interface {
	ReadAtWithStats(ctx context.Context, p []byte, off int64, stats *Stats) (n int, err error)
}

// tableReader implements get & has queries against a single nbs table. goroutine safe.
// |blockSize| refers to the block-size of the underlying storage. We assume that, each
// time we read data, we actually have to read in blocks of this size. So, we're willing
// to tolerate up to |blockSize| overhead each time we read a chunk, if it helps us group
// more chunks together into a single read request to backing storage.
type tableReader struct {
	tableIndex
	prefixes              []uint64
	chunkCount            uint32
	totalUncompressedData uint64
	r                     tableReaderAt
	blockSize             uint64
}

type tableIndex interface {
	// ChunkCount returns the total number of chunks in the indexed file.
	ChunkCount() uint32
	// EntrySuffixMatches returns true if the entry at index |idx| matches
	// the suffix of the address |h|. Used by |Lookup| after finding
	// matching indexes based on |Prefixes|.
	EntrySuffixMatches(idx uint32, h *addr) bool
	// IndexEntry returns the |indexEntry| at |idx|. Optionally puts the
	// full address of that entry in |a| if |a| is not |nil|.
	IndexEntry(idx uint32, a *addr) indexEntry
	// Lookup returns an |indexEntry| for the chunk corresponding to the
	// provided address |h|. Second returns is |true| if an entry exists
	// and |false| otherwise.
	Lookup(h *addr) (indexEntry, bool)
	// Ordinals returns a slice of indexes which maps the |i|th chunk in
	// the indexed file to its corresponding entry in index. The |i|th
	// entry in the result is the |i|th chunk in the indexed file, and its
	// corresponding value in the slice is the index entry that maps to it.
	Ordinals() []uint32
	// Prefixes returns the sorted slice of |uint64| |addr| prefixes; each
	// entry corresponds to an indexed chunk address.
	Prefixes() []uint64
	// TableFileSize returns the total size of the indexed table file, in bytes.
	TableFileSize() uint64
	// TotalUncompressedData returns the total uncompressed data size of
	// the table file. Used for informational statistics only.
	TotalUncompressedData() uint64

	// Close releases any resources used by this tableIndex.
	Close() error

	// Clone returns a |tableIndex| with the same contents which can be
	// |Close|d independently.
	Clone() tableIndex
}

var _ tableIndex = mmapTableIndex{}

// parses a valid nbs tableIndex from a byte stream. |buff| must end with an NBS index
// and footer, though it may contain an unspecified number of bytes before that data.
// |tableIndex| doesn't keep alive any references to |buff|.
func parseTableIndex(buff []byte) (onHeapTableIndex, error) {
	pos := int64(len(buff))

	// footer
	pos -= magicNumberSize

	if string(buff[pos:]) != magicNumber {
		return onHeapTableIndex{}, ErrInvalidTableFile
	}

	// total uncompressed chunk data
	pos -= uint64Size

	if pos < 0 {
		return onHeapTableIndex{}, ErrInvalidTableFile
	}

	totalUncompressedData := binary.BigEndian.Uint64(buff[pos:])

	pos -= uint32Size

	if pos < 0 {
		return onHeapTableIndex{}, ErrInvalidTableFile
	}

	chunkCount := binary.BigEndian.Uint32(buff[pos:])

	// index
	suffixesSize := int64(chunkCount) * addrSuffixSize
	pos -= suffixesSize

	if pos < 0 {
		return onHeapTableIndex{}, ErrInvalidTableFile
	}

	suffixes := make([]byte, suffixesSize)
	copy(suffixes, buff[pos:])

	lengthsSize := int64(chunkCount) * lengthSize
	pos -= lengthsSize

	if pos < 0 {
		return onHeapTableIndex{}, ErrInvalidTableFile
	}

	lengths, offsets := computeOffsets(chunkCount, buff[pos:pos+lengthsSize])

	tuplesSize := int64(chunkCount) * prefixTupleSize
	pos -= tuplesSize

	if pos < 0 {
		return onHeapTableIndex{}, ErrInvalidTableFile
	}

	prefixes, ordinals := computePrefixes(chunkCount, buff[pos:pos+tuplesSize])

	return onHeapTableIndex{
		chunkCount, totalUncompressedData,
		prefixes, offsets,
		lengths, ordinals,
		suffixes,
	}, nil
}

func computeOffsets(count uint32, buff []byte) (lengths []uint32, offsets []uint64) {
	lengths = make([]uint32, count)
	offsets = make([]uint64, count)

	lengths[0] = binary.BigEndian.Uint32(buff)

	for i := uint64(1); i < uint64(count); i++ {
		lengths[i] = binary.BigEndian.Uint32(buff[i*lengthSize:])
		offsets[i] = offsets[i-1] + uint64(lengths[i-1])
	}
	return
}

func computePrefixes(count uint32, buff []byte) (prefixes []uint64, ordinals []uint32) {
	prefixes = make([]uint64, count)
	ordinals = make([]uint32, count)

	for i := uint64(0); i < uint64(count); i++ {
		idx := i * prefixTupleSize
		prefixes[i] = binary.BigEndian.Uint64(buff[idx:])
		ordinals[i] = binary.BigEndian.Uint32(buff[idx+addrPrefixSize:])
	}
	return
}

func (ti onHeapTableIndex) prefixIdxToOrdinal(idx uint32) uint32 {
	return ti.ordinals[idx]
}

// TableFileSize returns the size of the table file that this index references.
// This assumes that the index follows immediately after the last chunk in the
// file and that the last chunk in the file is in the index.
func (ti onHeapTableIndex) TableFileSize() uint64 {
	if ti.chunkCount == 0 {
		return footerSize
	}
	len, offset := ti.offsets[ti.chunkCount-1], uint64(ti.lengths[ti.chunkCount-1])
	return offset + len + indexSize(ti.chunkCount) + footerSize
}

// prefixIdx returns the first position in |tr.prefixes| whose value ==
// |prefix|. Returns |tr.chunkCount| if absent
func (ti onHeapTableIndex) prefixIdx(prefix uint64) (idx uint32) {
	// NOTE: The golang impl of sort.Search is basically inlined here. This method can be called in
	// an extremely tight loop and inlining the code was a significant perf improvement.
	idx, j := 0, ti.chunkCount
	for idx < j {
		h := idx + (j-idx)/2 // avoid overflow when computing h
		// i ≤ h < j
		if ti.prefixes[h] < prefix {
			idx = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}

	return
}

// EntrySuffixMatches returns true IFF the suffix for prefix entry |idx|
// matches the address |a|.
func (ti onHeapTableIndex) EntrySuffixMatches(idx uint32, h *addr) bool {
	li := uint64(ti.ordinals[idx]) * addrSuffixSize
	return bytes.Equal(h[addrPrefixSize:], ti.suffixes[li:li+addrSuffixSize])
}

// lookupOrdinal returns the ordinal of |h| if present. Returns |ti.chunkCount|
// if absent.
func (ti onHeapTableIndex) lookupOrdinal(h *addr) uint32 {
	prefix := h.Prefix()

	for idx := ti.prefixIdx(prefix); idx < ti.chunkCount && ti.prefixes[idx] == prefix; idx++ {
		if ti.EntrySuffixMatches(idx, h) {
			return ti.ordinals[idx]
		}
	}

	return ti.chunkCount
}

func (ti onHeapTableIndex) IndexEntry(idx uint32, a *addr) indexEntry {
	ord := ti.ordinals[idx]
	if a != nil {
		binary.BigEndian.PutUint64(a[:], ti.prefixes[idx])
		li := uint64(ord) * addrSuffixSize
		copy(a[addrPrefixSize:], ti.suffixes[li:li+addrSuffixSize])
	}
	return indexResult{ti.offsets[ord], ti.lengths[ord]}
}

func (ti onHeapTableIndex) Lookup(h *addr) (indexEntry, bool) {
	ord := ti.lookupOrdinal(h)
	if ord == ti.chunkCount {
		return indexResult{}, false
	}
	return indexResult{ti.offsets[ord], ti.lengths[ord]}, true
}

func (ti onHeapTableIndex) Prefixes() []uint64 {
	return ti.prefixes
}

func (ti onHeapTableIndex) Ordinals() []uint32 {
	return ti.ordinals
}

func (i onHeapTableIndex) ChunkCount() uint32 {
	return i.chunkCount
}

func (i onHeapTableIndex) TotalUncompressedData() uint64 {
	return i.totalUncompressedData
}

func (i onHeapTableIndex) Close() error {
	return nil
}

func (i onHeapTableIndex) Clone() tableIndex {
	return i
}

// newTableReader parses a valid nbs table byte stream and returns a reader. buff must end with an NBS index
// and footer, though it may contain an unspecified number of bytes before that data. r should allow
// retrieving any desired range of bytes from the table.
func newTableReader(index tableIndex, r tableReaderAt, blockSize uint64) tableReader {
	return tableReader{
		index,
		index.Prefixes(),
		index.ChunkCount(),
		index.TotalUncompressedData(),
		r,
		blockSize,
	}
}

// Scan across (logically) two ordered slices of address prefixes.
func (tr tableReader) hasMany(addrs []hasRecord) (bool, error) {
	// TODO: Use findInIndex if (tr.chunkCount - len(addrs)*Log2(tr.chunkCount)) > (tr.chunkCount - len(addrs))

	filterIdx := uint32(0)
	filterLen := uint32(tr.chunkCount)

	var remaining bool
	for i, addr := range addrs {
		if addr.has {
			continue
		}

		for filterIdx < filterLen && addr.prefix > tr.prefixes[filterIdx] {
			filterIdx++
		}

		if filterIdx >= filterLen {
			return true, nil
		}

		if addr.prefix != tr.prefixes[filterIdx] {
			remaining = true
			continue
		}

		// prefixes are equal, so locate and compare against the corresponding suffix
		for j := filterIdx; j < filterLen && addr.prefix == tr.prefixes[j]; j++ {
			if tr.EntrySuffixMatches(j, addr.a) {
				addrs[i].has = true
				break
			}
		}

		if !addrs[i].has {
			remaining = true
		}
	}

	return remaining, nil
}

func (tr tableReader) count() (uint32, error) {
	return tr.chunkCount, nil
}

func (tr tableReader) uncompressedLen() (uint64, error) {
	return tr.totalUncompressedData, nil
}

func (tr tableReader) index() (tableIndex, error) {
	return tr.tableIndex, nil
}

// returns true iff |h| can be found in this table.
func (tr tableReader) has(h addr) (bool, error) {
	_, ok := tr.Lookup(&h)
	return ok, nil
}

var DebugGetLocations bool = false

// returns the storage associated with |h|, iff present. Returns nil if absent. On success,
// the returned byte slice directly references the underlying storage.
func (tr tableReader) get(ctx context.Context, h addr, stats *Stats) ([]byte, error) {
	e, found := tr.Lookup(&h)
	if !found {
		return nil, nil
	}

	offset := e.Offset()
	length := uint64(e.Length())
	buff := make([]byte, length) // TODO: Avoid this allocation for every get

	if DebugGetLocations {
		fmt.Fprintf(color.Error, "get %v at offset %d, length %d\n", h, offset, length)
	}

	n, err := tr.r.ReadAtWithStats(ctx, buff, int64(offset), stats)

	if err != nil {
		return nil, err
	}

	if n != int(length) {
		return nil, errors.New("failed to read all data")
	}

	cmp, err := NewCompressedChunk(hash.Hash(h), buff)

	if err != nil {
		return nil, err
	}

	if len(cmp.CompressedData) == 0 {
		return nil, errors.New("failed to get data")
	}

	chnk, err := cmp.ToChunk()

	if err != nil {
		return nil, err
	}

	return chnk.Data(), nil
}

type offsetRec struct {
	a      *addr
	offset uint64
	length uint32
}

type offsetRecSlice []offsetRec

func (hs offsetRecSlice) Len() int           { return len(hs) }
func (hs offsetRecSlice) Less(i, j int) bool { return hs[i].offset < hs[j].offset }
func (hs offsetRecSlice) Swap(i, j int)      { hs[i], hs[j] = hs[j], hs[i] }

var _ chunkReadPlanner = tableReader{}
var _ chunkReader = tableReader{}

func (tr tableReader) readCompressedAtOffsets(
	ctx context.Context,
	rb readBatch,
	found func(CompressedChunk),
	stats *Stats,
) error {
	return tr.readAtOffsetsWithCB(ctx, rb, stats, func(cmp CompressedChunk) error {
		found(cmp)
		return nil
	})
}

func (tr tableReader) readAtOffsets(
	ctx context.Context,
	rb readBatch,
	found func(*chunks.Chunk),
	stats *Stats,
) error {
	return tr.readAtOffsetsWithCB(ctx, rb, stats, func(cmp CompressedChunk) error {
		chk, err := cmp.ToChunk()

		if err != nil {
			return err
		}

		found(&chk)
		return nil
	})
}

func (tr tableReader) readAtOffsetsWithCB(
	ctx context.Context,
	rb readBatch,
	stats *Stats,
	cb func(cmp CompressedChunk) error,
) error {
	readLength := rb.End() - rb.Start()
	buff := make([]byte, readLength)

	n, err := tr.r.ReadAtWithStats(ctx, buff, int64(rb.Start()), stats)
	if err != nil {
		return err
	}

	if uint64(n) != readLength {
		return errors.New("failed to read all data")
	}

	for i := range rb {
		cmp, err := rb.ExtractChunkFromRead(buff, i)
		if err != nil {
			return err
		}

		err = cb(cmp)
		if err != nil {
			return err
		}
	}

	return nil
}

// getMany retrieves multiple stored blocks and optimizes by attempting to read in larger physical
// blocks which contain multiple stored blocks. |reqs| must be sorted by address prefix.
func (tr tableReader) getMany(
	ctx context.Context,
	eg *errgroup.Group,
	reqs []getRecord,
	found func(*chunks.Chunk),
	stats *Stats) (bool, error) {

	// Pass #1: Iterate over |reqs| and |tr.prefixes| (both sorted by address) and build the set
	// of table locations which must be read in order to satisfy the getMany operation.
	offsetRecords, remaining := tr.findOffsets(reqs)
	err := tr.getManyAtOffsets(ctx, eg, offsetRecords, found, stats)
	return remaining, err
}
func (tr tableReader) getManyCompressed(ctx context.Context, eg *errgroup.Group, reqs []getRecord, found func(CompressedChunk), stats *Stats) (bool, error) {
	// Pass #1: Iterate over |reqs| and |tr.prefixes| (both sorted by address) and build the set
	// of table locations which must be read in order to satisfy the getMany operation.
	offsetRecords, remaining := tr.findOffsets(reqs)
	err := tr.getManyCompressedAtOffsets(ctx, eg, offsetRecords, found, stats)
	return remaining, err
}

func (tr tableReader) getManyCompressedAtOffsets(ctx context.Context, eg *errgroup.Group, offsetRecords offsetRecSlice, found func(CompressedChunk), stats *Stats) error {
	return tr.getManyAtOffsetsWithReadFunc(ctx, eg, offsetRecords, stats, func(
		ctx context.Context,
		rb readBatch,
		stats *Stats) error {
		return tr.readCompressedAtOffsets(ctx, rb, found, stats)
	})
}

func (tr tableReader) getManyAtOffsets(
	ctx context.Context,
	eg *errgroup.Group,
	offsetRecords offsetRecSlice,
	found func(*chunks.Chunk),
	stats *Stats,
) error {
	return tr.getManyAtOffsetsWithReadFunc(ctx, eg, offsetRecords, stats, func(
		ctx context.Context,
		rb readBatch,
		stats *Stats) error {
		return tr.readAtOffsets(ctx, rb, found, stats)
	})
}

type readBatch offsetRecSlice

func (r readBatch) Start() uint64 {
	return r[0].offset
}

func (r readBatch) End() uint64 {
	last := r[len(r)-1]
	return last.offset + uint64(last.length)
}

func (s readBatch) ExtractChunkFromRead(buff []byte, idx int) (CompressedChunk, error) {
	rec := s[idx]
	chunkStart := rec.offset - s.Start()
	return NewCompressedChunk(hash.Hash(*rec.a), buff[chunkStart:chunkStart+uint64(rec.length)])
}

func toReadBatches(offsets offsetRecSlice, blockSize uint64) []readBatch {
	res := make([]readBatch, 0)
	var batch readBatch
	for i := 0; i < len(offsets); {
		rec := offsets[i]
		if batch == nil {
			batch = readBatch{rec}
			i++
			continue
		}

		if _, canRead := canReadAhead(rec, batch.End(), blockSize); canRead {
			batch = append(batch, rec)
			i++
			continue
		}

		res = append(res, batch)
		batch = nil
	}
	if batch != nil {
		res = append(res, batch)
	}
	return res
}

func (tr tableReader) getManyAtOffsetsWithReadFunc(
	ctx context.Context,
	eg *errgroup.Group,
	offsetRecords offsetRecSlice,
	stats *Stats,
	readAtOffsets func(
		ctx context.Context,
		rb readBatch,
		stats *Stats) error,
) error {
	batches := toReadBatches(offsetRecords, tr.blockSize)
	var idx int32
	readBatches := func() error {
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			i := atomic.AddInt32(&idx, 1) - 1
			if int(i) >= len(batches) {
				return nil
			}
			rb := batches[i]
			err := readAtOffsets(ctx, rb, stats)
			if err != nil {
				return err
			}
		}
	}
	ioParallelism := 4
	for i := 0; i < ioParallelism; i++ {
		eg.Go(readBatches)
	}

	return nil
}

// findOffsets iterates over |reqs| and |tr.prefixes| (both sorted by
// address) to build the set of table locations which must be read in order to
// find each chunk specified by |reqs|. If this table contains all requested
// chunks remaining will be set to false upon return. If some are not here,
// then remaining will be true. The result offsetRecSlice is sorted in offset
// order.
func (tr tableReader) findOffsets(reqs []getRecord) (ors offsetRecSlice, remaining bool) {
	filterIdx := uint32(0)
	filterLen := uint32(len(tr.prefixes))
	ors = make(offsetRecSlice, 0, len(reqs))

	// Iterate over |reqs| and |tr.prefixes| (both sorted by address) and build the set
	// of table locations which must be read in order to satisfy |reqs|.
	for i, req := range reqs {
		if req.found {
			continue
		}

		// advance within the prefixes until we reach one which is >= req.prefix
		for filterIdx < filterLen && tr.prefixes[filterIdx] < req.prefix {
			filterIdx++
		}

		if filterIdx >= filterLen {
			remaining = true // last prefix visited.
			break
		}

		if req.prefix != tr.prefixes[filterIdx] {
			remaining = true
			continue
		}

		// record all offsets within the table which contain the data required.
		for j := filterIdx; j < filterLen && req.prefix == tr.prefixes[j]; j++ {
			if tr.EntrySuffixMatches(j, req.a) {
				reqs[i].found = true
				entry := tr.IndexEntry(j, nil)
				ors = append(ors, offsetRec{req.a, entry.Offset(), entry.Length()})
				break
			}
		}
	}

	sort.Sort(ors)
	return ors, remaining
}

func canReadAhead(fRec offsetRec, readEnd, blockSize uint64) (newEnd uint64, canRead bool) {
	if fRec.offset < readEnd {
		// |offsetRecords| will contain an offsetRecord for *every* chunkRecord whose address
		// prefix matches the prefix of a requested address. If the set of requests contains
		// addresses which share a common prefix, then it's possible for multiple offsetRecords
		// to reference the same table offset position. In that case, we'll see sequential
		// offsetRecords with the same fRec.offset.
		return readEnd, true
	}

	if fRec.offset-readEnd > blockSize {
		return readEnd, false
	}

	return fRec.offset + uint64(fRec.length), true
}

func (tr tableReader) calcReads(reqs []getRecord, blockSize uint64) (reads int, remaining bool, err error) {
	var offsetRecords offsetRecSlice
	// Pass #1: Build the set of table locations which must be read in order to find all the elements of |reqs| which are present in this table.
	offsetRecords, remaining = tr.findOffsets(reqs)

	// Now |offsetRecords| contains all locations within the table which must
	// be searched (note that there may be duplicates of a particular
	// location). Scan forward, grouping sequences of reads into large physical
	// reads.

	var readStart, readEnd uint64
	readStarted := false

	for i := 0; i < len(offsetRecords); {
		rec := offsetRecords[i]
		length := rec.length

		if !readStarted {
			readStarted = true
			reads++
			readStart = rec.offset
			readEnd = readStart + uint64(length)
			i++
			continue
		}

		if newReadEnd, canRead := canReadAhead(rec, readEnd, tr.blockSize); canRead {
			readEnd = newReadEnd
			i++
			continue
		}

		readStarted = false
	}

	return
}

func (tr tableReader) extract(ctx context.Context, chunks chan<- extractRecord) error {
	sendChunk := func(or offsetRec) error {
		buff := make([]byte, or.length)
		n, err := tr.r.ReadAtWithStats(ctx, buff, int64(or.offset), &Stats{})
		if err != nil {
			return err
		}
		if uint32(n) != or.length {
			return errors.New("did not read all data")
		}
		cmp, err := NewCompressedChunk(hash.Hash(*or.a), buff)

		if err != nil {
			return err
		}

		chnk, err := cmp.ToChunk()

		if err != nil {
			return err
		}

		chunks <- extractRecord{a: *or.a, data: chnk.Data()}
		return nil
	}

	var ors offsetRecSlice
	for i := uint32(0); i < tr.chunkCount; i++ {
		a := new(addr)
		e := tr.IndexEntry(i, a)
		ors = append(ors, offsetRec{a, e.Offset(), e.Length()})
	}
	sort.Sort(ors)
	for _, or := range ors {
		err := sendChunk(or)
		if err != nil {
			return err
		}
	}

	return nil
}

func (tr tableReader) reader(ctx context.Context) (io.Reader, error) {
	i, _ := tr.index()
	return io.LimitReader(&readerAdapter{tr.r, 0, ctx}, int64(i.TableFileSize())), nil
}

func (tr tableReader) Close() error {
	return tr.tableIndex.Close()
}

func (tr tableReader) Clone() tableReader {
	return tableReader{tr.tableIndex.Clone(), tr.prefixes, tr.chunkCount, tr.totalUncompressedData, tr.r, tr.blockSize}
}

type readerAdapter struct {
	rat tableReaderAt
	off int64
	ctx context.Context
}

func (ra *readerAdapter) Read(p []byte) (n int, err error) {
	n, err = ra.rat.ReadAtWithStats(ra.ctx, p, ra.off, &Stats{})
	ra.off += int64(n)
	return
}
