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

package remotestorage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"golang.org/x/sync/errgroup"

	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/atomicerr"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/dolthub/dolt/go/store/types"
)

var DownloadHedger *Hedger

func init() {
	// TODO: This does not necessarily respond well to changes in network
	// conditions during the program's runtime.
	DownloadHedger = NewHedger(
		8,
		NewMinStrategy(
			1*time.Second,
			NewPercentileStrategy(0, 1*time.Hour, 95.0),
		),
	)
}

var ErrUploadFailed = errors.New("upload failed")
var ErrInvalidDoltSpecPath = errors.New("invalid dolt spec path")

var globalHttpFetcher HTTPFetcher = &http.Client{}

var _ nbs.TableFileStore = (*DoltChunkStore)(nil)
var _ datas.NBSCompressedChunkStore = (*DoltChunkStore)(nil)
var _ chunks.ChunkStore = (*DoltChunkStore)(nil)

// We may need this to be configurable for users with really bad internet
var downThroughputCheck = iohelp.MinThroughputCheckParams{
	MinBytesPerSec: 1024,
	CheckInterval:  1 * time.Second,
	NumIntervals:   5,
}

const (
	downRetryCount   = 5
	uploadRetryCount = 5
)

var uploadRetryParams = backoff.NewExponentialBackOff()
var downRetryParams = backoff.NewExponentialBackOff()

func init() {
	uploadRetryParams.MaxInterval = 5 * time.Second

	downRetryParams.MaxInterval = 5 * time.Second
}

type HTTPFetcher interface {
	Do(req *http.Request) (*http.Response, error)
}

type DoltChunkStore struct {
	org         string
	repoName    string
	host        string
	csClient    remotesapi.ChunkStoreServiceClient
	cache       chunkCache
	metadata    *remotesapi.GetRepoMetadataResponse
	nbf         *types.NomsBinFormat
	httpFetcher HTTPFetcher
}

func NewDoltChunkStoreFromPath(ctx context.Context, nbf *types.NomsBinFormat, path, host string, csClient remotesapi.ChunkStoreServiceClient) (*DoltChunkStore, error) {
	tokens := strings.Split(strings.Trim(path, "/"), "/")
	if len(tokens) != 2 {
		return nil, ErrInvalidDoltSpecPath
	}

	// todo:
	// this may just be a dolthub thing.  Need to revisit how we do this.
	org := tokens[0]
	repoName := tokens[1]

	return NewDoltChunkStore(ctx, nbf, org, repoName, host, csClient)
}

func NewDoltChunkStore(ctx context.Context, nbf *types.NomsBinFormat, org, repoName, host string, csClient remotesapi.ChunkStoreServiceClient) (*DoltChunkStore, error) {
	metadata, err := csClient.GetRepoMetadata(ctx, &remotesapi.GetRepoMetadataRequest{
		RepoId: &remotesapi.RepoId{
			Org:      org,
			RepoName: repoName,
		},
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: nbf.VersionString(),
			NbsVersion: nbs.StorageVersion,
		},
	})

	if err != nil {
		return nil, err
	}

	return &DoltChunkStore{org, repoName, host, csClient, newMapChunkCache(), metadata, nbf, globalHttpFetcher}, nil
}

func (dcs *DoltChunkStore) WithHTTPFetcher(fetcher HTTPFetcher) *DoltChunkStore {
	return &DoltChunkStore{dcs.org, dcs.repoName, dcs.host, dcs.csClient, dcs.cache, dcs.metadata, dcs.nbf, fetcher}
}

func (dcs *DoltChunkStore) WithNoopChunkCache() *DoltChunkStore {
	return &DoltChunkStore{dcs.org, dcs.repoName, dcs.host, dcs.csClient, noopChunkCache, dcs.metadata, dcs.nbf, dcs.httpFetcher}
}

func (dcs *DoltChunkStore) getRepoId() *remotesapi.RepoId {
	return &remotesapi.RepoId{
		Org:      dcs.org,
		RepoName: dcs.repoName,
	}
}

// Get the Chunk for the value of the hash in the store. If the hash is absent from the store EmptyChunk is returned.
func (dcs *DoltChunkStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	hashes := hash.HashSet{h: struct{}{}}
	var found *chunks.Chunk
	err := dcs.GetMany(ctx, hashes, func(c *chunks.Chunk) { found = c })
	if err != nil {
		return chunks.EmptyChunk, err
	}
	if found != nil {
		return *found, nil
	} else {
		return chunks.EmptyChunk, nil
	}
}

func (dcs *DoltChunkStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(*chunks.Chunk)) error {
	ae := atomicerr.New()
	err := dcs.GetManyCompressed(ctx, hashes, func(cc nbs.CompressedChunk) {
		if ae.IsSet() {
			return
		}
		c, err := cc.ToChunk()
		if ae.SetIfErrAndCheck(err) {
			return
		}
		found(&c)
	})
	if err != nil {
		return err
	}
	if err = ae.Get(); err != nil {
		return err
	}
	return nil
}

// GetMany gets the Chunks with |hashes| from the store. On return, |foundChunks| will have been fully sent all chunks
// which have been found. Any non-present chunks will silently be ignored.
func (dcs *DoltChunkStore) GetManyCompressed(ctx context.Context, hashes hash.HashSet, found func(nbs.CompressedChunk)) error {
	hashToChunk := dcs.cache.Get(hashes)

	notCached := make([]hash.Hash, 0, len(hashes))
	for h := range hashes {
		c := hashToChunk[h]

		if c.IsEmpty() {
			notCached = append(notCached, h)
		} else {
			found(c)
		}
	}

	if len(notCached) > 0 {
		err := dcs.readChunksAndCache(ctx, hashes, notCached, found)

		if err != nil {
			return err
		}
	}

	return nil
}

const (
	getLocsBatchSize      = 32 * 1024
	getLocsMaxConcurrency = 4
)

type GetRange remotesapi.HttpGetRange

func (gr *GetRange) ResourcePath() string {
	u, _ := url.Parse(gr.Url)
	return fmt.Sprintf("%s://%s%s", u.Scheme, u.Host, u.Path)
}

func (gr *GetRange) Append(other *GetRange) {
	gr.Url = other.Url
	gr.Ranges = append(gr.Ranges, other.Ranges...)
}

func (gr *GetRange) Sort() {
	sort.Slice(gr.Ranges, func(i, j int) bool {
		return gr.Ranges[i].Offset < gr.Ranges[j].Offset
	})
}

func (gr *GetRange) ChunkStartOffset(i int) uint64 {
	return gr.Ranges[i].Offset
}

func (gr *GetRange) ChunkEndOffset(i int) uint64 {
	return gr.Ranges[i].Offset + uint64(gr.Ranges[i].Length)
}

func (gr *GetRange) GapBetween(i, j int) uint64 {
	return gr.ChunkStartOffset(j) - gr.ChunkEndOffset(i)
}

func (gr *GetRange) SplitAtGaps(maxGapBytes uint64) []*GetRange {
	gr.Sort()
	res := make([]*GetRange, 0)
	i := 0
	for i < len(gr.Ranges) {
		j := i + 1
		for j < len(gr.Ranges) {
			if gr.GapBetween(j-1, j) > maxGapBytes {
				break
			}
			j++
		}
		res = append(res, &GetRange{Url: gr.Url, Ranges: gr.Ranges[i:j]})
		i = j
	}
	return res
}

func (gr *GetRange) NumChunks() int {
	return len(gr.Ranges)
}

func (gr *GetRange) RangeLen() uint64 {
	return gr.ChunkEndOffset(gr.NumChunks()-1) - gr.ChunkStartOffset(0)
}

func (gr *GetRange) ChunkByteRange(i int) (uint64, uint64) {
	start := gr.ChunkStartOffset(i) - gr.ChunkStartOffset(0)
	end := gr.ChunkEndOffset(i) - gr.ChunkStartOffset(0)
	return start, end
}

func (gr *GetRange) GetDownloadFunc(ctx context.Context, fetcher HTTPFetcher, chunkChan chan nbs.CompressedChunk) func() error {
	if len(gr.Ranges) == 0 {
		return func() error { return nil }
	}
	return func() error {
		comprData, err := hedgedRangeDownloadWithRetries(ctx, fetcher, gr.ChunkStartOffset(0), gr.RangeLen(), gr.Url)
		if err != nil {
			return err
		}
		// Send the chunk for each range included in GetRange.
		for i := 0; i < len(gr.Ranges); i++ {
			s, e := gr.ChunkByteRange(i)
			cmpChnk, err := nbs.NewCompressedChunk(hash.New(gr.Ranges[i].Hash), comprData[s:e])
			if err != nil {
				return err
			}
			select {
			case chunkChan <- cmpChnk:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}

func (dcs *DoltChunkStore) getDLLocs(ctx context.Context, hashes []hash.Hash) (map[string]*GetRange, error) {
	res := make(map[string]*GetRange)

	// channel for receiving results from go routines making grpc calls to get download locations for chunks
	dlLocChan := make(chan []*remotesapi.HttpGetRange)

	eg, ctx := errgroup.WithContext(ctx)

	// go routine for receiving the results of the grpc calls and aggregating the results into resourceToUrlAndRanges
	eg.Go(func() error {
		for {
			select {
			case locs, ok := <-dlLocChan:
				if !ok {
					return nil
				}
				for _, loc := range locs {
					gr := (*GetRange)(loc)
					if v, ok := res[gr.ResourcePath()]; ok {
						v.Append(gr)
					} else {
						res[gr.ResourcePath()] = gr
					}
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	hashesBytes := HashesToSlices(hashes)
	var work []func() error

	// batchItr creates work functions which request a batch of chunk download locations and write the results to the
	// dlLocChan
	batchItr(len(hashesBytes), getLocsBatchSize, func(st, end int) (stop bool) {
		batch := hashesBytes[st:end]
		f := func() error {
			req := &remotesapi.GetDownloadLocsRequest{RepoId: dcs.getRepoId(), ChunkHashes: batch}
			resp, err := dcs.csClient.GetDownloadLocations(ctx, req)
			if err != nil {
				return NewRpcError(err, "GetDownloadLocations", dcs.host, req)
			}
			tosend := make([]*remotesapi.HttpGetRange, len(resp.Locs))
			for i, l := range resp.Locs {
				tosend[i] = l.Location.(*remotesapi.DownloadLoc_HttpGetRange).HttpGetRange
			}
			select {
			case dlLocChan <- tosend:
			case <-ctx.Done():
			}
			return nil
		}
		work = append(work, f)
		return false
	})

	// execute the work and close the channel after as no more results will come in
	eg.Go(func() error {
		defer close(dlLocChan)
		return concurrentExec(work, getLocsMaxConcurrency)
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}

func (dcs *DoltChunkStore) readChunksAndCache(ctx context.Context, hashes hash.HashSet, notCached []hash.Hash, found func(nbs.CompressedChunk)) error {
	// get the locations where the chunks can be downloaded from
	resourceToUrlAndRanges, err := dcs.getDLLocs(ctx, notCached)

	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	// channel to receive chunks on
	chunkChan := make(chan nbs.CompressedChunk, 128)

	// start a go routine to receive the downloaded chunks on
	wg.Add(1)
	go func() {
		defer wg.Done()
		for chunk := range chunkChan {
			if !dcs.cache.PutChunk(chunk) {
				continue
			}

			h := chunk.Hash()
			if _, ok := hashes[h]; ok {
				found(chunk)
			}
		}
	}()

	// download the chunks and close the channel after
	func() {
		defer close(chunkChan)
		err = dcs.downloadChunks(ctx, resourceToUrlAndRanges, chunkChan)
	}()

	// wait for all the results to finish processing
	wg.Wait()

	if err != nil {
		return err
	}

	return nil
}

// Returns true iff the value at the address |h| is contained in the
// store
func (dcs *DoltChunkStore) Has(ctx context.Context, h hash.Hash) (bool, error) {
	hashes := hash.HashSet{h: struct{}{}}
	absent, err := dcs.HasMany(ctx, hashes)

	if err != nil {
		return false, err
	}

	return len(absent) == 0, nil
}

const maxHasManyBatchSize = 16 * 1024

// Returns a new HashSet containing any members of |hashes| that are
// absent from the store.
func (dcs *DoltChunkStore) HasMany(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
	// get the set of hashes that isn't already in the cache
	notCached := dcs.cache.Has(hashes)

	if len(notCached) == 0 {
		return notCached, nil
	}

	// convert the set to a slice of hashes and a corresponding slice of the byte encoding for those hashes
	hashSl, byteSl := HashSetToSlices(notCached)

	absent := make(hash.HashSet)
	var found []nbs.CompressedChunk
	var err error

	batchItr(len(hashSl), maxHasManyBatchSize, func(st, end int) (stop bool) {
		// slice the slices into a batch of hashes
		currHashSl := hashSl[st:end]
		currByteSl := byteSl[st:end]

		// send a request to the remote api to determine which chunks the remote api already has
		req := &remotesapi.HasChunksRequest{RepoId: dcs.getRepoId(), Hashes: currByteSl}
		resp, err := dcs.csClient.HasChunks(ctx, req)

		if err != nil {
			err = NewRpcError(err, "HasMany", dcs.host, req)
			return true
		}

		numAbsent := len(resp.Absent)
		sort.Slice(resp.Absent, func(i, j int) bool {
			return resp.Absent[i] < resp.Absent[j]
		})

		// loop over every hash in the current batch, and if they are absent from the remote host add them to the
		// absent set, otherwise append them to the found slice
		for i, j := 0, 0; i < len(currHashSl); i++ {
			currHash := currHashSl[i]

			nextAbsent := -1
			if j < numAbsent {
				nextAbsent = int(resp.Absent[j])
			}

			if i == nextAbsent {
				absent[currHash] = struct{}{}
				j++
			} else {
				c := nbs.ChunkToCompressedChunk(chunks.NewChunkWithHash(currHash, []byte{}))
				found = append(found, c)
			}
		}

		return false
	})

	if err != nil {
		return nil, err
	}

	if len(found)+len(absent) != len(notCached) {
		panic("not all chunks were accounted for")
	}

	if len(found) > 0 {
		dcs.cache.Put(found)
	}

	return absent, nil
}

// Put caches c. Upon return, c must be visible to
// subsequent Get and Has calls, but must not be persistent until a call
// to Flush(). Put may be called concurrently with other calls to Put(),
// Get(), GetMany(), Has() and HasMany().
func (dcs *DoltChunkStore) Put(ctx context.Context, c chunks.Chunk) error {
	cc := nbs.ChunkToCompressedChunk(c)
	dcs.cache.Put([]nbs.CompressedChunk{cc})
	return nil
}

// Returns the NomsVersion with which this ChunkSource is compatible.
func (dcs *DoltChunkStore) Version() string {
	return dcs.metadata.NbfVersion
}

// Rebase brings this ChunkStore into sync with the persistent storage's
// current root.
func (dcs *DoltChunkStore) Rebase(ctx context.Context) error {
	req := &remotesapi.RebaseRequest{RepoId: dcs.getRepoId()}
	_, err := dcs.csClient.Rebase(ctx, req)

	if err != nil {
		return NewRpcError(err, "Rebase", dcs.host, req)
	}

	return dcs.refreshRepoMetadata(ctx)
}

func (dcs *DoltChunkStore) refreshRepoMetadata(ctx context.Context) error {
	mdReq := &remotesapi.GetRepoMetadataRequest{
		RepoId: &remotesapi.RepoId{
			Org:      dcs.org,
			RepoName: dcs.repoName,
		},
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: dcs.nbf.VersionString(),
			NbsVersion: nbs.StorageVersion,
		},
	}
	metadata, err := dcs.csClient.GetRepoMetadata(ctx, mdReq)
	if err != nil {
		return NewRpcError(err, "GetRepoMetadata", dcs.host, mdReq)
	}
	dcs.metadata = metadata
	return nil
}

// Root returns the root of the database as of the time the ChunkStore
// was opened or the most recent call to Rebase.
func (dcs *DoltChunkStore) Root(ctx context.Context) (hash.Hash, error) {
	req := &remotesapi.RootRequest{RepoId: dcs.getRepoId()}
	resp, err := dcs.csClient.Root(ctx, req)

	if err != nil {
		return hash.Hash{}, NewRpcError(err, "Root", dcs.host, req)
	}

	return hash.New(resp.RootHash), nil
}

// Commit atomically attempts to persist all novel Chunks and update the
// persisted root hash from last to current (or keeps it the same).
// If last doesn't match the root in persistent storage, returns false.
func (dcs *DoltChunkStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	hashToChunkCount, err := dcs.uploadChunks(ctx)

	if err != nil {
		return false, err
	}

	chnkTblInfo := make([]*remotesapi.ChunkTableInfo, 0, len(hashToChunkCount))
	for h, cnt := range hashToChunkCount {
		chnkTblInfo = append(chnkTblInfo, &remotesapi.ChunkTableInfo{Hash: h[:], ChunkCount: uint32(cnt)})
	}

	req := &remotesapi.CommitRequest{
		RepoId:         dcs.getRepoId(),
		Current:        current[:],
		Last:           last[:],
		ChunkTableInfo: chnkTblInfo,
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: dcs.nbf.VersionString(),
			NbsVersion: nbs.StorageVersion,
		},
	}
	resp, err := dcs.csClient.Commit(ctx, req)
	if err != nil {
		return false, NewRpcError(err, "Commit", dcs.host, req)
	}

	return resp.Success, dcs.refreshRepoMetadata(ctx)
}

// Stats may return some kind of struct that reports statistics about the
// ChunkStore instance. The type is implementation-dependent, and impls
// may return nil
func (dcs *DoltChunkStore) Stats() interface{} {
	return nil
}

// StatsSummary may return a string containing summarized statistics for
// this ChunkStore. It must return "Unsupported" if this operation is not
// supported.
func (dcs *DoltChunkStore) StatsSummary() string {
	return "Unsupported"
}

// Close tears down any resources in use by the implementation. After
// Close(), the ChunkStore may not be used again. It is NOT SAFE to call
// Close() concurrently with any other ChunkStore method; behavior is
// undefined and probably crashy.
func (dcs *DoltChunkStore) Close() error {
	return nil
}

// getting this working using the simplest approach first
func (dcs *DoltChunkStore) uploadChunks(ctx context.Context) (map[hash.Hash]int, error) {
	hashToChunk := dcs.cache.GetAndClearChunksToFlush()

	if len(hashToChunk) == 0 {
		return map[hash.Hash]int{}, nil
	}

	chnks := make([]chunks.Chunk, 0, len(hashToChunk))
	for _, chable := range hashToChunk {
		ch, err := chable.ToChunk()

		if err != nil {
			return nil, err
		}

		chnks = append(chnks, ch)
	}

	hashToCount := make(map[hash.Hash]int)
	hashToData := make(map[hash.Hash][]byte)
	hashToDetails := make(map[hash.Hash]*remotesapi.TableFileDetails)

	// structuring so this can be done as multiple files in the future.
	{
		name, data, err := nbs.WriteChunks(chnks)

		if err != nil {
			return map[hash.Hash]int{}, err
		}

		h := hash.Parse(name)
		hashToData[h] = data
		hashToCount[h] = len(chnks)

		md5Bytes := md5.Sum(data)
		hashToDetails[h] = &remotesapi.TableFileDetails{
			Id:            h[:],
			ContentLength: uint64(len(data)),
			ContentHash:   md5Bytes[:],
		}
	}

	tfds := make([]*remotesapi.TableFileDetails, 0, len(hashToDetails))
	for _, v := range hashToDetails {
		tfds = append(tfds, v)
	}

	req := &remotesapi.GetUploadLocsRequest{RepoId: dcs.getRepoId(), TableFileDetails: tfds}
	resp, err := dcs.csClient.GetUploadLocations(ctx, req)

	if err != nil {
		return map[hash.Hash]int{}, err
	}

	for _, loc := range resp.Locs {
		var err error
		h := hash.New(loc.TableFileHash)
		data := hashToData[h]
		details := hashToDetails[h]
		switch typedLoc := loc.Location.(type) {
		case *remotesapi.UploadLoc_HttpPost:
			err = dcs.httpPostUpload(ctx, loc.TableFileHash, typedLoc.HttpPost, bytes.NewBuffer(data), details.ContentHash)
		default:
			break
		}

		if err != nil {
			return map[hash.Hash]int{}, err
		}
	}

	return hashToCount, nil
}

type Sizer interface {
	Size() int64
}

func (dcs *DoltChunkStore) httpPostUpload(ctx context.Context, hashBytes []byte, post *remotesapi.HttpPostTableFile, rd io.Reader, contentHash []byte) error {
	req, err := http.NewRequest(http.MethodPut, post.Url, rd)
	if err != nil {
		return err
	}

	if sizer, ok := rd.(Sizer); ok {
		req.ContentLength = sizer.Size()
	}

	if len(contentHash) > 0 {
		md5s := base64.StdEncoding.EncodeToString(contentHash)
		req.Header.Set("Content-MD5", md5s)
	}

	var resp *http.Response
	op := func() error {
		var err error
		resp, err = dcs.httpFetcher.Do(req.WithContext(ctx))

		if err == nil {
			defer func() {
				_ = resp.Body.Close()
			}()
		}

		return processHttpResp(resp, err)
	}

	err = backoff.Retry(op, backoff.WithMaxRetries(uploadRetryParams, uploadRetryCount))

	if err != nil {
		return err
	}

	return nil
}

// aggregateDownloads looks for byte ranges that need to be downloaded, and tries to aggregate them into a smaller number
// of larger downloads.  It does this by sorting the ranges of bytes that are needed, and then comparing how close together
// neighboring ranges are.  If they are within the threshold the two ranges will be aggregated into a single request for
// the entire range of data.
func AggregateDownloads(aggDistance uint64, resourceGets map[string]*GetRange) []*GetRange {
	var res []*GetRange
	for _, resourceGet := range resourceGets {
		res = append(res, resourceGet.SplitAtGaps(aggDistance)...)
	}
	return res
}

const (
	chunkAggDistance       = 8 * 1024
	maxDownloadConcurrency = 16
)

// creates work functions for each download and executes them in parallel.  The work functions write downloaded chunks
// to chunkChan
func (dcs *DoltChunkStore) downloadChunks(ctx context.Context, resourceGets map[string]*GetRange, chunkChan chan nbs.CompressedChunk) error {
	gets := AggregateDownloads(chunkAggDistance, resourceGets)

	// loop over all the gets that need to be downloaded and create a work function for each
	work := make([]func() error, len(gets))
	for i, get := range gets {
		work[i] = get.GetDownloadFunc(ctx, dcs.httpFetcher, chunkChan)
	}

	// execute the work
	err := concurrentExec(work, maxDownloadConcurrency)

	return err
}

func hedgedRangeDownloadWithRetries(ctx context.Context, fetcher HTTPFetcher, offset, length uint64, urlStr string) ([]byte, error) {
	res, err := DownloadHedger.Do(ctx, Work{
		Work: func(ctx context.Context) (interface{}, error) {
			return rangeDownloadWithRetries(ctx, fetcher, offset, length, urlStr)
		},
		Size: int(length),
	})
	if err != nil {
		return nil, err
	}
	return res.([]byte), nil
}

// rangeDownloadWithRetries executes an http get with the 'Range' header to get a range of bytes from a file.  Request
// is executed with retries and if progress was made, downloads will be resumed from where they left off on subsequent attempts.
func rangeDownloadWithRetries(ctx context.Context, fetcher HTTPFetcher, offset, length uint64, urlStr string) ([]byte, error) {
	// create the request
	req, err := http.NewRequest(http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, err
	}

	// parameters used for resuming downloads.
	var allBufs [][]byte
	currOffset := offset
	currLength := length

	//execute the request
	op := func() error {
		rangeVal := fmt.Sprintf("bytes=%d-%d", currOffset, currOffset+currLength-1)
		req.Header.Set("Range", rangeVal)

		resp, err := fetcher.Do(req.WithContext(ctx))
		if err == nil {
			defer func() {
				_ = resp.Body.Close()
			}()
		}

		respErr := processHttpResp(resp, err)
		if respErr != nil {
			return respErr
		}

		// read the results
		comprData, err := iohelp.ReadWithMinThroughput(resp.Body, int64(currLength), downThroughputCheck)

		dataRead := len(comprData)
		if dataRead > 0 {
			allBufs = append(allBufs, comprData)
			currLength -= uint64(dataRead)
			currOffset += uint64(dataRead)
		}
		return err
	}

	err = backoff.Retry(op, backoff.WithMaxRetries(downRetryParams, downRetryCount))

	if err != nil {
		return nil, err
	}

	return collapseBuffers(allBufs, length), nil
}

func collapseBuffers(bufs [][]byte, length uint64) []byte {
	if len(bufs) == 1 {
		return bufs[0]
	}
	res := make([]byte, 0, length)
	for _, buf := range bufs {
		res = append(res, buf...)
	}
	return res
}

func (dcs *DoltChunkStore) SupportedOperations() nbs.TableFileStoreOps {
	return nbs.TableFileStoreOps{
		CanRead:  true,
		CanWrite: true,
		CanPrune: false,
		CanGC:    false,
	}
}

// WriteTableFile reads a table file from the provided reader and writes it to the chunk store.
func (dcs *DoltChunkStore) WriteTableFile(ctx context.Context, fileId string, numChunks int, rd io.Reader, contentLength uint64, contentHash []byte) error {
	fileIdBytes := hash.Parse(fileId)
	tfd := &remotesapi.TableFileDetails{
		Id:            fileIdBytes[:],
		ContentLength: contentLength,
		ContentHash:   contentHash,
	}
	req := &remotesapi.GetUploadLocsRequest{
		RepoId:           dcs.getRepoId(),
		TableFileDetails: []*remotesapi.TableFileDetails{tfd},

		// redundant and deprecated.  Still setting for compatibility, but will remove promptly.
		TableFileHashes: [][]byte{fileIdBytes[:]},
	}
	resp, err := dcs.csClient.GetUploadLocations(ctx, req)

	if err != nil {
		return err
	}

	if len(resp.Locs) != 1 {
		return errors.New("unexpected upload location count")
	}

	loc := resp.Locs[0]
	switch typedLoc := loc.Location.(type) {
	case *remotesapi.UploadLoc_HttpPost:
		err = dcs.httpPostUpload(ctx, loc.TableFileHash, typedLoc.HttpPost, rd, contentHash)

		if err != nil {
			return err
		}

	default:
		return errors.New("unsupported upload location")
	}

	chnkTblInfo := []*remotesapi.ChunkTableInfo{
		{Hash: fileIdBytes[:], ChunkCount: uint32(numChunks)},
	}

	atReq := &remotesapi.AddTableFilesRequest{
		RepoId:         dcs.getRepoId(),
		ChunkTableInfo: chnkTblInfo,
		ClientRepoFormat: &remotesapi.ClientRepoFormat{
			NbfVersion: dcs.nbf.VersionString(),
			NbsVersion: nbs.StorageVersion,
		},
	}

	atResp, err := dcs.csClient.AddTableFiles(ctx, atReq)

	if err != nil {
		return NewRpcError(err, "UpdateManifest", dcs.host, atReq)
	}

	if !atResp.Success {
		return errors.New("update table files failed")
	}

	return nil
}

// PruneTableFiles deletes old table files that are no longer referenced in the manifest.
func (dcs *DoltChunkStore) PruneTableFiles(ctx context.Context) error {
	return chunks.ErrUnsupportedOperation
}

// Sources retrieves the current root hash, and a list of all the table files
func (dcs *DoltChunkStore) Sources(ctx context.Context) (hash.Hash, []nbs.TableFile, error) {
	req := &remotesapi.ListTableFilesRequest{RepoId: dcs.getRepoId()}
	resp, err := dcs.csClient.ListTableFiles(ctx, req)

	if err != nil {
		return hash.Hash{}, nil, err
	}

	var tblFiles []nbs.TableFile
	for _, nfo := range resp.TableFileInfo {
		tblFiles = append(tblFiles, DoltRemoteTableFile{dcs, nfo})
	}

	return hash.New(resp.RootHash), tblFiles, nil
}

func (dcs *DoltChunkStore) Size(ctx context.Context) (uint64, error) {
	return dcs.metadata.StorageSize, nil
}

// SetRootChunk changes the root chunk hash from the previous value to the new root.
func (dcs *DoltChunkStore) SetRootChunk(ctx context.Context, root, previous hash.Hash) error {
	panic("Not Implemented")
}

// DoltRemoteTableFile is an implementation of a TableFile that lives in a DoltChunkStore
type DoltRemoteTableFile struct {
	dcs  *DoltChunkStore
	info *remotesapi.TableFileInfo
}

// FileID gets the id of the file
func (drtf DoltRemoteTableFile) FileID() string {
	return drtf.info.FileId
}

// NumChunks returns the number of chunks in a table file
func (drtf DoltRemoteTableFile) NumChunks() int {
	return int(drtf.info.NumChunks)
}

var ErrRemoteTableFileGet = errors.New("HTTP GET for remote table file failed")

func sanitizeSignedUrl(url string) string {
	si := strings.Index(url, "Signature=")
	if si == -1 {
		return url
	}
	ei := strings.Index(url[si:], "&")
	if ei == -1 {
		return url[:si+15] + "..."
	} else {
		return url[:si+15] + "..." + url[si:][ei:]
	}
}

// Open returns an io.ReadCloser which can be used to read the bytes of a table file.
func (drtf DoltRemoteTableFile) Open(ctx context.Context) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, drtf.info.Url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := drtf.dcs.httpFetcher.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		body := make([]byte, 4096)
		n, _ := io.ReadFull(resp.Body, body)
		return nil, fmt.Errorf("%w: status code: %d;\nurl: %s\n\nbody:\n\n%s\n", ErrRemoteTableFileGet, resp.StatusCode, sanitizeSignedUrl(drtf.info.Url), string(body[0:n]))
	}

	return resp.Body, nil
}
