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

package types

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"

	"github.com/dolthub/dolt/go/store/atomicerr"

	"github.com/dolthub/dolt/go/store/d"
)

// Blob represents a list of Blobs.
type Blob struct {
	sequence
}

func newBlob(seq sequence) Blob {
	return Blob{seq}
}

func NewEmptyBlob(vrw ValueReadWriter) (Blob, error) {
	seq, err := newBlobLeafSequence(vrw, []byte{})

	if err != nil {
		return Blob{}, err
	}

	return Blob{seq}, nil
}

// ReadAt implements the ReaderAt interface. Eagerly loads requested byte-range from the blob p-tree.
func (b Blob) ReadAt(ctx context.Context, p []byte, off int64) (n int, err error) {
	// TODO: Support negative off?
	d.PanicIfTrue(off < 0)

	startIdx := uint64(off)
	if startIdx >= b.Len() {
		return 0, io.EOF
	}

	endIdx := startIdx + uint64(len(p))
	if endIdx > b.Len() {
		endIdx = b.Len()
	}

	var isEOF bool
	if endIdx == b.Len() {
		isEOF = true
	}

	if startIdx == endIdx {
		return
	}

	leaves, localStart, err := LoadLeafNodes(ctx, []Collection{b}, startIdx, endIdx)

	if err != nil {
		return 0, err
	}

	endIdx = localStart + endIdx - startIdx
	startIdx = localStart

	for _, leaf := range leaves {
		bl := leaf.asSequence().(blobLeafSequence)

		localEnd := endIdx
		data := bl.data()
		leafLength := uint64(len(data))
		if localEnd > leafLength {
			localEnd = leafLength
		}
		src := data[startIdx:localEnd]

		copy(p[n:], src)
		n += len(src)
		endIdx -= localEnd
		startIdx = 0
	}

	if isEOF {
		err = io.EOF
	}

	return n, err
}

func (b Blob) Reader(ctx context.Context) *BlobReader {
	return &BlobReader{b, 0, ctx}
}

func (b Blob) Copy(ctx context.Context, w io.Writer) (int64, error) {
	return b.CopyReadAhead(ctx, w, 1<<23 /* 8MB */, 6)
}

// CopyReadAhead copies the entire contents of |b| to |w|, and attempts to stay
// |concurrency| |chunkSize| blocks of bytes ahead of the last byte written to
// |w|.
func (b Blob) CopyReadAhead(ctx context.Context, w io.Writer, chunkSize uint64, concurrency int) (int64, error) {
	ae := atomicerr.New()
	bChan := make(chan chan []byte, concurrency)

	go func() {
		defer close(bChan)
		for idx, l := uint64(0), b.Len(); idx < l; {
			if ae.IsSet() {
				break
			}

			bc := make(chan []byte)
			bChan <- bc

			start := idx
			blockLength := b.Len() - start
			if blockLength > chunkSize {
				blockLength = chunkSize
			}
			idx += blockLength

			go func() {
				defer close(bc)
				buff := make([]byte, blockLength)
				n, err := b.ReadAt(ctx, buff, int64(start))

				if err != nil && err != io.EOF {
					ae.SetIfError(err)
				} else if n > 0 {
					bc <- buff
				}
			}()
		}
	}()

	// Ensure read-ahead goroutines can exit
	defer func() {
		for range bChan {
		}
	}()

	var n int64
	for b := range bChan {
		if ae.IsSet() {
			break
		}

		bytes, ok := <-b

		if !ok {
			continue
		}

		ln, err := w.Write(bytes)
		n += int64(ln)
		if err != nil {
			ae.SetIfError(err)
		}
	}

	return n, ae.Get()
}

// Concat returns a new Blob comprised of this joined with other. It only needs
// to visit the rightmost prolly tree chunks of this Blob, and the leftmost
// prolly tree chunks of other, so it's efficient.
func (b Blob) Concat(ctx context.Context, other Blob) (Blob, error) {
	seq, err := concat(ctx, b.sequence, other.sequence, func(cur *sequenceCursor, vrw ValueReadWriter) (*sequenceChunker, error) {
		return b.newChunker(ctx, cur, vrw)
	})

	if err != nil {
		return Blob{}, err
	}

	return newBlob(seq), nil
}

func (b Blob) newChunker(ctx context.Context, cur *sequenceCursor, vrw ValueReadWriter) (*sequenceChunker, error) {
	return newSequenceChunker(ctx, cur, 0, vrw, makeBlobLeafChunkFn(vrw), newIndexedMetaSequenceChunkFn(BlobKind, vrw), hashValueByte)
}

func (b Blob) asSequence() sequence {
	return b.sequence
}

// Value interface
func (b Blob) Value(ctx context.Context) (Value, error) {
	return b, nil
}

func (b Blob) isPrimitive() bool {
	return true
}

func (b Blob) Kind() NomsKind {
	if b.sequence == nil {
		return BlobKind
	}
	return b.sequence.Kind()
}

func (b Blob) WalkValues(ctx context.Context, cb ValueCallback) error {
	return nil
}

type BlobReader struct {
	b   Blob
	pos int64
	ctx context.Context
}

func (cbr *BlobReader) Read(p []byte) (n int, err error) {
	n, err = cbr.b.ReadAt(cbr.ctx, p, cbr.pos)
	cbr.pos += int64(n)
	return
}

func (cbr *BlobReader) Seek(offset int64, whence int) (int64, error) {
	abs := int64(cbr.pos)

	switch whence {
	case 0:
		abs = offset
	case 1:
		abs += offset
	case 2:
		abs = int64(cbr.b.Len()) + offset
	default:
		return 0, errors.New("Blob.Reader.Seek: invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("Blob.Reader.Seek: negative position")
	}

	cbr.pos = int64(abs)
	return abs, nil
}

func makeBlobLeafChunkFn(vrw ValueReadWriter) makeChunkFn {
	return func(level uint64, items []sequenceItem) (Collection, orderedKey, uint64, error) {
		d.PanicIfFalse(level == 0)
		buff := make([]byte, len(items))

		for i, v := range items {
			buff[i] = v.(byte)
		}

		return chunkBlobLeaf(vrw, buff)
	}
}

func chunkBlobLeaf(vrw ValueReadWriter, buff []byte) (Collection, orderedKey, uint64, error) {
	seq, err := newBlobLeafSequence(vrw, buff)

	if err != nil {
		return nil, orderedKey{}, 0, err
	}

	blob := newBlob(seq)

	ordKey, err := orderedKeyFromInt(len(buff), vrw.Format())

	if err != nil {
		return nil, orderedKey{}, 0, err
	}

	return blob, ordKey, uint64(len(buff)), nil
}

// NewBlob creates a Blob by reading from every Reader in rs and
// concatenating the result. NewBlob uses one goroutine per Reader.
func NewBlob(ctx context.Context, vrw ValueReadWriter, rs ...io.Reader) (Blob, error) {
	return readBlobsP(ctx, vrw, rs...)
}

func readBlobsP(ctx context.Context, vrw ValueReadWriter, rs ...io.Reader) (Blob, error) {
	switch len(rs) {
	case 0:
		return NewEmptyBlob(vrw)
	case 1:
		return readBlob(ctx, rs[0], vrw)
	}

	blobs := make([]Blob, len(rs))

	ae := atomicerr.New()
	wg := &sync.WaitGroup{}
	wg.Add(len(rs))

	for i, r := range rs {
		if ae.IsSet() {
			break
		}

		i2, r2 := i, r
		go func() {
			defer wg.Done()

			if !ae.IsSet() {
				var err error
				blobs[i2], err = readBlob(ctx, r2, vrw)
				ae.SetIfError(err)
			}
		}()
	}

	wg.Wait()

	if ae.IsSet() {
		return Blob{}, ae.Get()
	}

	b := blobs[0]
	for i := 1; i < len(blobs); i++ {
		var err error
		b, err = b.Concat(ctx, blobs[i])

		if err != nil {
			return Blob{}, err
		}
	}
	return b, nil
}

func readBlob(ctx context.Context, r io.Reader, vrw ValueReadWriter) (Blob, error) {
	sc, err := newEmptySequenceChunker(ctx, vrw, makeBlobLeafChunkFn(vrw), newIndexedMetaSequenceChunkFn(BlobKind, vrw), func(item sequenceItem, rv *rollingValueHasher) error {
		rv.HashByte(item.(byte))
		return nil
	})

	if err != nil {
		return Blob{}, err
	}

	// TODO: The code below is temporary. It's basically a custom leaf-level chunker for blobs. There are substational
	// perf gains by doing it this way as it avoids the cost of boxing every single byte which is chunked.
	chunkBuff := [8192]byte{}
	chunkBytes := chunkBuff[:]
	rv := newRollingValueHasher(vrw.Format(), 0)
	offset := 0
	addByte := func(b byte) bool {
		if offset >= len(chunkBytes) {
			tmp := make([]byte, len(chunkBytes)*2)
			copy(tmp, chunkBytes)
			chunkBytes = tmp
		}
		chunkBytes[offset] = b
		offset++
		rv.hashByte(b, uint32(offset))
		return rv.crossedBoundary
	}

	ae := atomicerr.New()
	mtChan := make(chan chan metaTuple, runtime.NumCPU())

	makeChunk := func() {
		rv.Reset()
		cp := make([]byte, offset)
		copy(cp, chunkBytes[0:offset])

		ch := make(chan metaTuple)
		mtChan <- ch

		go func(ch chan metaTuple, cp []byte) {
			defer close(ch)

			col, key, numLeaves, err := chunkBlobLeaf(vrw, cp)

			if err != nil {
				ae.SetIfError(err)
				return
			}

			val, err := vrw.WriteValue(ctx, col)

			if ae.SetIfError(err) {
				return
			}

			mt, err := newMetaTuple(val, key, numLeaves)

			if ae.SetIfError(err) {
				return
			}

			ch <- mt
		}(ch, cp)

		offset = 0
	}

	go func() {
		defer close(mtChan)
		readBuff := [8192]byte{}
		for {
			if ae.IsSet() {
				break
			}

			n, err := r.Read(readBuff[:])

			isEOF := err == io.EOF
			if err != nil && err != io.EOF {
				ae.SetIfError(err)
				break
			}

			for i := 0; i < n; i++ {
				if addByte(readBuff[i]) {
					makeChunk()
				}
			}

			if isEOF {
				if offset > 0 {
					makeChunk()
				}
				break
			}
		}
	}()

	for ch := range mtChan {
		if ae.IsSet() {
			break
		}

		mt, ok := <-ch

		if !ok {
			continue
		}

		if sc.parent == nil {
			err := sc.createParent(ctx)

			if ae.SetIfError(err) {
				continue
			}
		}

		_, err := sc.parent.Append(ctx, mt)
		ae.SetIfError(err)
	}

	seq, err := sc.Done(ctx)

	if err != nil {
		return Blob{}, err
	}

	return newBlob(seq), nil
}

func (b Blob) readFrom(nbf *NomsBinFormat, bnr *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

func (b Blob) skip(nbf *NomsBinFormat, bnr *binaryNomsReader) {
	panic("unreachable")
}

func (b Blob) String() string {
	panic("unreachable")
}

func (b Blob) HumanReadableString() string {
	panic("unreachable")
}
