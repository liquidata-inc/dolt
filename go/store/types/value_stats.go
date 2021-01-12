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
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"fmt"
	"io"

	humanize "github.com/dustin/go-humanize"
	"github.com/golang/snappy"

	"github.com/dolthub/dolt/go/store/hash"
)

type ValueStats interface {
	String() string
}

func WriteValueStats(ctx context.Context, w io.Writer, v Value, vr ValueReader) error {
	switch v.Kind() {
	case BoolKind, FloatKind, StringKind, RefKind, StructKind, TypeKind, TupleKind, ChunkedStringKind:
		return writeUnchunkedValueStats(w, v, vr)
	case BlobKind, ListKind, MapKind, SetKind:
		return writePtreeStats(ctx, w, v, vr)
	}

	return nil
}

func writeUnchunkedValueStats(w io.Writer, v Value, vr ValueReader) error {
	cmpSize, err := compressedSize(vr.Format(), v)

	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, "Kind: %s\nCompressedSize: %s\n", v.Kind().String(), humanize.Bytes(cmpSize))
	return err
}

const treeRowFormat = "%5s%20s%20s%20s\n"

var treeLevelHeader = fmt.Sprintf(treeRowFormat, "Level", "Nodes", "Values/Node", "Size/Node")

func writePtreeStats(ctx context.Context, w io.Writer, v Value, vr ValueReader) error {
	totalCompressedSize := uint64(0)
	totalChunks := uint64(0)

	_, err := fmt.Fprintf(w, "Kind: %s\n", v.Kind().String())

	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, treeLevelHeader)

	if err != nil {
		return err
	}

	level := int64(v.(Collection).asSequence().treeLevel())
	nodes := ValueSlice{v}

	// TODO: For level 0, use NBS to fetch leaf sizes without actually reading leaf data.
	for level >= 0 {
		children := RefSlice{}
		visited := hash.HashSet{}
		chunkCount, valueCount, byteSize := uint64(0), uint64(0), uint64(0)

		for _, n := range nodes {
			chunkCount++
			if level > 0 {
				err := n.WalkRefs(vr.Format(), func(r Ref) error {
					children = append(children, r)
					return nil
				})

				if err != nil {
					return err
				}
			}

			s := n.(Collection).asSequence()
			valueCount += uint64(s.seqLen())

			h, err := n.Hash(vr.Format())

			if err != nil {
				return err
			}

			if !visited.Has(h) {
				// Indexed Ptrees can share nodes within the same tree level. Only count each unique value once
				cmpSize, err := compressedSize(vr.Format(), n)

				if err != nil {
					return err
				}

				byteSize += cmpSize
				visited.Insert(h)
			}
		}

		err := printTreeLevel(w, uint64(level), valueCount, chunkCount, byteSize)

		if err != nil {
			return err
		}

		nodes, err = loadNextLevel(ctx, children, vr)

		if err != nil {
			return err
		}

		level--
		totalCompressedSize += byteSize
		totalChunks += chunkCount
	}

	return nil
}

func printTreeLevel(w io.Writer, level, values, chunks, byteSize uint64) error {
	avgItems := float64(values) / float64(chunks)
	avgSize := byteSize / chunks

	_, err := fmt.Fprintf(w, treeRowFormat,
		fmt.Sprintf("%d", level),
		humanize.Comma(int64(chunks)),
		fmt.Sprintf("%.1f", avgItems),
		humanize.Bytes(avgSize))

	return err
}

func compressedSize(nbf *NomsBinFormat, v Value) (uint64, error) {
	chunk, err := EncodeValue(v, nbf)

	if err != nil {
		return 0, err
	}

	compressed := snappy.Encode(nil, chunk.Data())
	return uint64(len(compressed)), nil
}

func loadNextLevel(ctx context.Context, refs RefSlice, vr ValueReader) (ValueSlice, error) {
	hs := make(hash.HashSlice, len(refs))
	for i, r := range refs {
		hs[i] = r.TargetHash()
	}

	// Fetch committed child sequences in a single batch
	return vr.ReadManyValues(ctx, hs)
}
