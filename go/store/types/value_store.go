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
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/sizecache"
)

// ValueReader is an interface that knows how to read Noms Values, e.g.
// datas/Database. Required to avoid import cycle between this package and the
// package that implements Value reading.
type ValueReader interface {
	Format() *NomsBinFormat
	ReadValue(ctx context.Context, h hash.Hash) (Value, error)
	ReadManyValues(ctx context.Context, hashes hash.HashSlice) (ValueSlice, error)
}

// ValueWriter is an interface that knows how to write Noms Values, e.g.
// datas/Database. Required to avoid import cycle between this package and the
// package that implements Value writing.
type ValueWriter interface {
	WriteValue(ctx context.Context, v Value) (Ref, error)
}

// ValueReadWriter is an interface that knows how to read and write Noms
// Values, e.g. datas/Database. Required to avoid import cycle between this
// package and the package that implements Value read/writing.
type ValueReadWriter interface {
	ValueReader
	ValueWriter
}

// ValueStore provides methods to read and write Noms Values to a ChunkStore.
// It minimally validates Values as they're written, but does not guarantee
// that these Values are persisted through the ChunkStore until a subsequent
// Flush.
// Currently, WriteValue validates the following properties of a Value v:
// - v can be correctly serialized and its Ref taken
type ValueStore struct {
	cs                   chunks.ChunkStore
	bufferMu             sync.RWMutex
	bufferedChunks       map[hash.Hash]chunks.Chunk
	bufferedChunksMax    uint64
	bufferedChunkSize    uint64
	withBufferedChildren map[hash.Hash]uint64 // chunk Hash -> ref height
	unresolvedRefs       hash.HashSet
	enforceCompleteness  bool
	decodedChunks        *sizecache.SizeCache
	nbf                  *NomsBinFormat

	versOnce sync.Once
}

func PanicIfDangling(ctx context.Context, unresolved hash.HashSet, cs chunks.ChunkStore) {
	absent, err := cs.HasMany(ctx, unresolved)

	// TODO: fix panics
	d.PanicIfError(err)

	if len(absent) != 0 {
		d.Panic("Found dangling references to %v", absent)
	}
}

const (
	defaultDecodedChunksSize = 1 << 25 // 32MB
	defaultPendingPutMax     = 1 << 28 // 256MB

	gcBuffSize = 16
)

// newTestValueStore creates a simple struct that satisfies ValueReadWriter
// and is backed by a chunks.TestStore. Used for testing noms.
func newTestValueStore() *ValueStore {
	ts := &chunks.TestStorage{}
	return NewValueStore(ts.NewView())
}

// NewInternalValueStore creates a simple struct that satisfies ValueReadWriter
// and is backed by a chunks.TestStore. Used for dolt operations outside of noms.
func NewInternalValueStore() *ValueStore {
	ts := &chunks.TestStorage{}
	return NewValueStore(ts.NewViewWithDefaultFormat())
}

// NewValueStore returns a ValueStore instance that owns the provided
// ChunkStore and manages its lifetime. Calling Close on the returned
// ValueStore will Close() cs.
func NewValueStore(cs chunks.ChunkStore) *ValueStore {
	return newValueStoreWithCacheAndPending(cs, defaultDecodedChunksSize, defaultPendingPutMax)
}

func newValueStoreWithCacheAndPending(cs chunks.ChunkStore, cacheSize, pendingMax uint64) *ValueStore {
	return &ValueStore{
		cs: cs,

		bufferMu:             sync.RWMutex{},
		bufferedChunks:       map[hash.Hash]chunks.Chunk{},
		bufferedChunksMax:    pendingMax,
		withBufferedChildren: map[hash.Hash]uint64{},
		decodedChunks:        sizecache.New(cacheSize),
		unresolvedRefs:       hash.HashSet{},
		enforceCompleteness:  true,
		versOnce:             sync.Once{},
	}
}

func (lvs *ValueStore) expectVersion() {
	dataVersion := lvs.cs.Version()
	nbf, err := GetFormatForVersionString(dataVersion)
	if err != nil {
		panic(err)
	}
	lvs.nbf = nbf
}

func (lvs *ValueStore) SetEnforceCompleteness(enforce bool) {
	lvs.enforceCompleteness = enforce
}

func (lvs *ValueStore) ChunkStore() chunks.ChunkStore {
	return lvs.cs
}

func (lvs *ValueStore) Format() *NomsBinFormat {
	lvs.versOnce.Do(lvs.expectVersion)
	return lvs.nbf
}

// ReadValue reads and decodes a value from lvs. It is not considered an error
// for the requested chunk to be empty; in this case, the function simply
// returns nil.
func (lvs *ValueStore) ReadValue(ctx context.Context, h hash.Hash) (Value, error) {
	lvs.versOnce.Do(lvs.expectVersion)
	if v, ok := lvs.decodedChunks.Get(h); ok {
		if v == nil {
			return nil, errors.New("value present but empty")
		}

		return v.(Value), nil
	}

	chunk := func() chunks.Chunk {
		lvs.bufferMu.RLock()
		defer lvs.bufferMu.RUnlock()
		if pending, ok := lvs.bufferedChunks[h]; ok {
			return pending
		}
		return chunks.EmptyChunk
	}()

	if chunk.IsEmpty() {
		var err error
		chunk, err = lvs.cs.Get(ctx, h)

		if err != nil {
			return nil, err
		}
	}
	if chunk.IsEmpty() {
		return nil, nil
	}

	v, err := DecodeValue(chunk, lvs)

	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, errors.New("decoded value is empty")
	}

	lvs.decodedChunks.Add(h, uint64(len(chunk.Data())), v)
	return v, nil
}

// ReadManyValues reads and decodes Values indicated by |hashes| from lvs and
// returns the found Values in the same order. Any non-present Values will be
// represented by nil.
func (lvs *ValueStore) ReadManyValues(ctx context.Context, hashes hash.HashSlice) (ValueSlice, error) {
	lvs.versOnce.Do(lvs.expectVersion)
	decode := func(h hash.Hash, chunk *chunks.Chunk) (Value, error) {
		v, ferr := DecodeValue(*chunk, lvs)

		if ferr != nil {
			return nil, ferr
		}

		if v == nil {
			return nil, errors.New("decoded value is empty")
		}

		lvs.decodedChunks.Add(h, uint64(len(chunk.Data())), v)
		return v, nil
	}

	foundValues := make(map[hash.Hash]Value, len(hashes))

	// First, see which hashes can be found in either the Value cache or bufferedChunks.
	// Put the rest into a new HashSet to be requested en masse from the ChunkStore.
	remaining := hash.HashSet{}
	for _, h := range hashes {
		if v, ok := lvs.decodedChunks.Get(h); ok {
			d.PanicIfTrue(v == nil)
			foundValues[h] = v.(Value)
			continue
		}

		chunk := func() chunks.Chunk {
			lvs.bufferMu.RLock()
			defer lvs.bufferMu.RUnlock()
			if pending, ok := lvs.bufferedChunks[h]; ok {
				return pending
			}
			return chunks.EmptyChunk
		}()
		if !chunk.IsEmpty() {
			var err error
			foundValues[h], err = decode(h, &chunk)

			if err != nil {
				return nil, err
			}

			continue
		}

		remaining.Insert(h)
	}

	if len(remaining) != 0 {
		mu := new(sync.Mutex)
		var decodeErr error
		err := lvs.cs.GetMany(ctx, remaining, func(c *chunks.Chunk) {
			mu.Lock()
			defer mu.Unlock()
			if decodeErr != nil {
				return
			}
			h := c.Hash()
			foundValues[h], decodeErr = decode(h, c)
		})
		if err != nil {
			return nil, err
		}
		if decodeErr != nil {
			return nil, decodeErr
		}
	}

	rv := make(ValueSlice, len(hashes))
	for i, h := range hashes {
		rv[i] = foundValues[h]
	}
	return rv, nil
}

// WriteValue takes a Value, schedules it to be written it to lvs, and returns
// an appropriately-typed types.Ref. v is not guaranteed to be actually
// written until after Flush().
func (lvs *ValueStore) WriteValue(ctx context.Context, v Value) (Ref, error) {
	lvs.versOnce.Do(lvs.expectVersion)
	d.PanicIfFalse(v != nil)

	c, err := EncodeValue(v, lvs.nbf)

	if err != nil {
		return Ref{}, err
	}

	if c.IsEmpty() {
		return Ref{}, errors.New("value encoded to empty chunk")
	}

	h := c.Hash()
	height, err := maxChunkHeight(lvs.nbf, v)

	if err != nil {
		return Ref{}, err
	}

	height++
	t, err := TypeOf(v)

	if err != nil {
		return Ref{}, err
	}

	r, err := constructRef(lvs.nbf, h, t, height)

	if err != nil {
		return Ref{}, err
	}

	lvs.bufferChunk(ctx, v, c, height)
	return r, nil
}

// bufferChunk enqueues c (which is the serialization of v) within this
// ValueStore. Buffered chunks are flushed progressively to the underlying
// ChunkStore in a way which attempts to locate children and grandchildren
// sequentially together. The following invariants are retained:
//
// 1. For any given chunk currently in the buffer, only direct children of the
//    chunk may also be presently buffered (any grandchildren will have been
//    flushed).
// 2. The total data occupied by buffered chunks does not exceed
//    lvs.bufferedChunksMax
func (lvs *ValueStore) bufferChunk(ctx context.Context, v Value, c chunks.Chunk, height uint64) {
	lvs.bufferMu.Lock()
	defer lvs.bufferMu.Unlock()

	d.PanicIfTrue(height == 0)
	h := c.Hash()
	if _, present := lvs.bufferedChunks[h]; !present {
		lvs.bufferedChunks[h] = c
		lvs.bufferedChunkSize += uint64(len(c.Data()))
	}

	put := func(h hash.Hash, c chunks.Chunk) error {
		err := lvs.cs.Put(ctx, c)

		if err != nil {
			return err
		}

		lvs.bufferedChunkSize -= uint64(len(c.Data()))
		delete(lvs.bufferedChunks, h)

		return nil
	}

	putChildren := func(parent hash.Hash) error {
		pending, isBuffered := lvs.bufferedChunks[parent]
		if !isBuffered {
			return nil
		}

		err := WalkRefs(pending, lvs.nbf, func(grandchildRef Ref) error {
			gch := grandchildRef.TargetHash()
			if pending, present := lvs.bufferedChunks[gch]; present {
				return put(gch, pending)
			}

			return nil
		})

		if err != nil {
			return err
		}

		delete(lvs.withBufferedChildren, parent)

		return nil
	}

	// Enforce invariant (1)
	if height > 1 {
		err := v.WalkRefs(lvs.nbf, func(childRef Ref) error {
			childHash := childRef.TargetHash()
			if _, isBuffered := lvs.bufferedChunks[childHash]; isBuffered {
				lvs.withBufferedChildren[h] = height
			} else if lvs.enforceCompleteness {
				// If the childRef isn't presently buffered, we must consider it an
				// unresolved ref.
				lvs.unresolvedRefs.Insert(childHash)
			}

			if _, hasBufferedChildren := lvs.withBufferedChildren[childHash]; hasBufferedChildren {
				return putChildren(childHash)
			}

			return nil
		})

		// TODO: fix panics
		d.PanicIfError(err)
	}

	// Enforce invariant (2)
	for lvs.bufferedChunkSize > lvs.bufferedChunksMax {
		var tallest hash.Hash
		var height uint64 = 0
		for parent, ht := range lvs.withBufferedChildren {
			if ht > height {
				tallest = parent
				height = ht
			}
		}
		if height == 0 { // This can happen if there are no pending parents
			var chunk chunks.Chunk
			for tallest, chunk = range lvs.bufferedChunks {
				// Any pendingPut is as good as another in this case, so take the first one
				break
			}

			err := put(tallest, chunk)

			// TODO: fix panics
			d.PanicIfError(err)

			continue
		}

		err := putChildren(tallest)

		// TODO: fix panics
		d.PanicIfError(err)
	}
}

func (lvs *ValueStore) Root(ctx context.Context) (hash.Hash, error) {
	root, err := lvs.cs.Root(ctx)

	if err != nil {
		return hash.Hash{}, err
	}

	return root, nil
}

func (lvs *ValueStore) Rebase(ctx context.Context) error {
	return lvs.cs.Rebase(ctx)
}

// Commit() flushes all bufferedChunks into the ChunkStore, with best-effort
// locality, and attempts to Commit, updating the root to |current| (or keeping
// it the same as Root()). If the root has moved since this ValueStore was
// opened, or last Rebased(), it will return false and will have internally
// rebased. Until Commit() succeeds, no work of the ValueStore will be visible
// to other readers of the underlying ChunkStore.
func (lvs *ValueStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	return func() (bool, error) {
		lvs.bufferMu.Lock()
		defer lvs.bufferMu.Unlock()

		put := func(h hash.Hash, chunk chunks.Chunk) error {
			err := lvs.cs.Put(ctx, chunk)

			if err != nil {
				return err
			}

			delete(lvs.bufferedChunks, h)
			lvs.bufferedChunkSize -= uint64(len(chunk.Data()))
			return nil
		}

		for parent := range lvs.withBufferedChildren {
			if pending, present := lvs.bufferedChunks[parent]; present {
				err := WalkRefs(pending, lvs.nbf, func(reachable Ref) error {
					if pending, present := lvs.bufferedChunks[reachable.TargetHash()]; present {
						return put(reachable.TargetHash(), pending)
					}

					return nil
				})

				if err != nil {
					return false, err
				}

				err = put(parent, pending)

				if err != nil {
					return false, err
				}
			}
		}
		for _, c := range lvs.bufferedChunks {
			// Can't use put() because it's wrong to delete from a lvs.bufferedChunks while iterating it.
			err := lvs.cs.Put(ctx, c)

			if err != nil {
				return false, err
			}

			lvs.bufferedChunkSize -= uint64(len(c.Data()))
		}

		d.PanicIfFalse(lvs.bufferedChunkSize == 0)
		lvs.withBufferedChildren = map[hash.Hash]uint64{}
		lvs.bufferedChunks = map[hash.Hash]chunks.Chunk{}

		if lvs.enforceCompleteness {
			root, err := lvs.Root(ctx)

			if err != nil {
				return false, err
			}

			if (current != hash.Hash{} && current != root) {
				if _, ok := lvs.bufferedChunks[current]; !ok {
					// If the client is attempting to move the root and the referenced
					// value isn't still buffered, we need to ensure that it is contained
					// in the ChunkStore.
					lvs.unresolvedRefs.Insert(current)
				}
			}

			PanicIfDangling(ctx, lvs.unresolvedRefs, lvs.cs)
		}

		success, err := lvs.cs.Commit(ctx, current, last)

		if err != nil {
			return false, err
		}

		if !success {
			return false, nil
		}

		if lvs.enforceCompleteness {
			lvs.unresolvedRefs = hash.HashSet{}
		}

		return true, nil
	}()
}

// GC traverses the ValueStore from the root and removes unreferenced chunks from the ChunkStore
func (lvs *ValueStore) GC(ctx context.Context) error {
	collector, ok := lvs.cs.(chunks.ChunkStoreGarbageCollector)

	if !ok {
		return chunks.ErrUnsupportedOperation
	}

	lvs.versOnce.Do(lvs.expectVersion)

	root, err := lvs.Root(ctx)

	if err != nil {
		return err
	}

	rootVal, err := lvs.ReadValue(ctx, root)
	if err != nil {
		return err
	}
	if rootVal == nil {
		// empty root
		return nil
	}

	keepChunks := make(chan []hash.Hash, gcBuffSize)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return collector.MarkAndSweepChunks(ctx, root, keepChunks)
	})

	keepHashes := func(hs []hash.Hash) error {
		select {
		case keepChunks <- hs:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	const batchSize = 16384
	batches := func(hs []hash.Hash) [][]hash.Hash {
		// Return subslices of a copy, because the caller may
		// mutate the parameter after the call.
		copied := make([]hash.Hash, len(hs))
		copy(copied, hs)
		var res [][]hash.Hash
		i := 0
		for ; i+batchSize < len(copied); i += batchSize {
			res = append(res, copied[i:i+batchSize])
		}
		if i < len(hs) {
			res = append(res, copied[i:len(hs)])
		}
		return res
	}

	concurrency := runtime.GOMAXPROCS(0) - 1
	if concurrency < 1 {
		concurrency = 1
	}
	walker := newParallelRefWalker(ctx, lvs.nbf, concurrency)

	eg.Go(func() error {
		toVisit := []hash.Hash{root}
		visited := hash.NewHashSet(root)
		for len(toVisit) > 0 {
			batches := batches(toVisit)
			toVisit = toVisit[0:0]
			for _, batch := range batches {
				if err := keepHashes(batch); err != nil {
					return err
				}
				vals, err := lvs.ReadManyValues(ctx, batch)
				if err != nil {
					return err
				}
				if len(vals) != len(batch) {
					return errors.New("dangling reference found in chunk store")
				}
				hashes, err := walker.GetRefs(visited, vals)
				if err != nil {
					return err
				}
				toVisit = append(toVisit, hashes...)
			}
		}
		close(keepChunks)
		walker.Close()
		return nil
	})

	err = eg.Wait()
	if err != nil {
		return err
	}

	// purge the cache
	lvs.decodedChunks = sizecache.New(lvs.decodedChunks.Size())
	lvs.bufferedChunks = make(map[hash.Hash]chunks.Chunk, lvs.bufferedChunkSize)
	lvs.bufferedChunkSize = 0
	lvs.withBufferedChildren = map[hash.Hash]uint64{}

	return nil
}

// Close closes the underlying ChunkStore
func (lvs *ValueStore) Close() error {
	return lvs.cs.Close()
}
