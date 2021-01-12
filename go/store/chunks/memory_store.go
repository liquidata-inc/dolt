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

package chunks

import (
	"context"
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
)

// MemoryStorage provides a "persistent" storage layer to back multiple
// MemoryStoreViews. A MemoryStorage instance holds the ground truth for the
// root and set of chunks that are visible to all MemoryStoreViews vended by
// NewView(), allowing them to implement the transaction-style semantics that
// ChunkStore requires.
type MemoryStorage struct {
	data     map[hash.Hash]Chunk
	rootHash hash.Hash
	mu       sync.RWMutex
	version  string
}

// NewView vends a MemoryStoreView backed by this MemoryStorage. It's
// initialized with the currently "persisted" root.
func (ms *MemoryStorage) NewView() ChunkStore {
	version := ms.version
	if version == "" {
		version = constants.NomsVersion
	}

	return &MemoryStoreView{storage: ms, rootHash: ms.rootHash, version: version}
}

// NewViewWithVersion vends a MemoryStoreView backed by this MemoryStorage. It's
// initialized with the currently "persisted" root. Uses the default format.
func (ms *MemoryStorage) NewViewWithDefaultFormat() ChunkStore {
	return &MemoryStoreView{storage: ms, rootHash: ms.rootHash, version: constants.FormatDefaultString}
}

// Get retrieves the Chunk with the Hash h, returning EmptyChunk if it's not
// present.
func (ms *MemoryStorage) Get(ctx context.Context, h hash.Hash) (Chunk, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if c, ok := ms.data[h]; ok {
		return c, nil
	}
	return EmptyChunk, nil
}

// Has returns true if the Chunk with the Hash h is present in ms.data, false
// if not.
func (ms *MemoryStorage) Has(ctx context.Context, r hash.Hash) (bool, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	_, ok := ms.data[r]
	return ok, nil
}

// Len returns the number of Chunks in ms.data.
func (ms *MemoryStorage) Len() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.data)
}

// Root returns the currently "persisted" root hash of this in-memory store.
func (ms *MemoryStorage) Root(ctx context.Context) hash.Hash {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.rootHash
}

// Update checks the "persisted" root against last and, iff it matches,
// updates the root to current, adds all of novel to ms.data, and returns
// true. Otherwise returns false.
func (ms *MemoryStorage) Update(current, last hash.Hash, novel map[hash.Hash]Chunk) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if last != ms.rootHash {
		return false
	}
	if ms.data == nil {
		ms.data = map[hash.Hash]Chunk{}
	}
	for h, c := range novel {
		ms.data[h] = c
	}
	ms.rootHash = current
	return true
}

// MemoryStoreView is an in-memory implementation of store.ChunkStore. Useful
// mainly for tests.
// The proper way to get one:
// storage := &MemoryStorage{}
// ms := storage.NewView()
type MemoryStoreView struct {
	pending  map[hash.Hash]Chunk
	rootHash hash.Hash
	mu       sync.RWMutex
	version  string

	storage *MemoryStorage
}

var _ ChunkStore = &MemoryStoreView{}
var _ ChunkStoreGarbageCollector = &MemoryStoreView{}

func (ms *MemoryStoreView) Get(ctx context.Context, h hash.Hash) (Chunk, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if c, ok := ms.pending[h]; ok {
		return c, nil
	}
	return ms.storage.Get(ctx, h)
}

func (ms *MemoryStoreView) GetMany(ctx context.Context, hashes hash.HashSet, found func(*Chunk)) error {
	for h := range hashes {
		c, err := ms.Get(ctx, h)

		if err != nil {
			return err
		}

		if !c.IsEmpty() {
			found(&c)
		}
	}

	return nil
}

func (ms *MemoryStoreView) Has(ctx context.Context, h hash.Hash) (bool, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if _, ok := ms.pending[h]; ok {
		return true, nil
	}
	return ms.storage.Has(ctx, h)
}

func (ms *MemoryStoreView) HasMany(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
	absent := hash.HashSet{}
	for h := range hashes {
		exists, err := ms.Has(ctx, h)
		if err != nil {
			return nil, err
		} else if !exists {
			absent.Insert(h)
		}
	}
	return absent, nil
}

func (ms *MemoryStoreView) Version() string {
	return ms.version
}

func (ms *MemoryStoreView) Put(ctx context.Context, c Chunk) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.pending == nil {
		ms.pending = map[hash.Hash]Chunk{}
	}
	ms.pending[c.Hash()] = c

	return nil
}

func (ms *MemoryStoreView) Len() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.pending) + ms.storage.Len()
}

func (ms *MemoryStoreView) Rebase(ctx context.Context) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.rootHash = ms.storage.Root(ctx)

	return nil
}

func (ms *MemoryStoreView) Root(ctx context.Context) (hash.Hash, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.rootHash, nil
}

func (ms *MemoryStoreView) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if last != ms.rootHash {
		return false, nil
	}

	success := ms.storage.Update(current, last, ms.pending)
	if success {
		ms.pending = nil
	}
	ms.rootHash = ms.storage.Root(ctx)
	return success, nil
}

func (ms *MemoryStoreView) MarkAndSweepChunks(ctx context.Context, last hash.Hash, keepChunks <-chan []hash.Hash) error {
	if last != ms.rootHash {
		return fmt.Errorf("last does not match ms.Root()")
	}

	keepers := make(map[hash.Hash]Chunk, ms.storage.Len())

LOOP:
	for {
		select {
		case hs, ok := <-keepChunks:
			if !ok {
				break LOOP
			}
			for _, h := range hs {
				c, err := ms.Get(ctx, h)
				if err != nil {
					return err
				}
				keepers[h] = c
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	ms.storage = &MemoryStorage{rootHash: ms.rootHash, data: keepers}
	ms.pending = map[hash.Hash]Chunk{}
	return nil
}

func (ms *MemoryStoreView) Stats() interface{} {
	return nil
}

func (ms *MemoryStoreView) StatsSummary() string {
	return "Unsupported"
}

func (ms *MemoryStoreView) Close() error {
	return nil
}

type memoryStoreFactory struct {
	stores map[string]*MemoryStorage
	mu     *sync.Mutex
}

func NewMemoryStoreFactory() *memoryStoreFactory {
	return &memoryStoreFactory{map[string]*MemoryStorage{}, &sync.Mutex{}}
}

func (f *memoryStoreFactory) CreateStoreFromCache(ctx context.Context, ns string) ChunkStore {
	return f.CreateStore(ctx, ns)
}

func (f *memoryStoreFactory) CreateStore(ctx context.Context, ns string) ChunkStore {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.stores == nil {
		d.Panic("Cannot use memoryStoreFactory after Shutter().")
	}
	if ms, present := f.stores[ns]; present {
		return ms.NewView()
	}
	f.stores[ns] = &MemoryStorage{}
	return f.stores[ns].NewView()
}

func (f *memoryStoreFactory) Shutter() {
	f.stores = nil
}
