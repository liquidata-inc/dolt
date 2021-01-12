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
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
)

// WalkRefs calls cb() on each Ref that can be decoded from |c|. The results
// are precisely equal to DecodeValue(c).WalkRefs(cb), but this should be much
// faster.
func WalkRefs(c chunks.Chunk, nbf *NomsBinFormat, cb RefCallback) error {
	return walkRefs(c.Data(), nbf, cb)
}

func walkRefs(data []byte, nbf *NomsBinFormat, cb RefCallback) error {
	rw := newRefWalker(data)
	return rw.walkValue(nbf, cb)
}

type refWalker struct {
	typedBinaryNomsReader
}

func newRefWalker(buff []byte) refWalker {
	nr := binaryNomsReader{buff, 0}
	return refWalker{typedBinaryNomsReader{nr, false}}
}

func (r *refWalker) walkRef(nbf *NomsBinFormat, cb RefCallback) error {
	ref, err := readRef(nbf, &(r.typedBinaryNomsReader))

	if err != nil {
		return err
	}

	return cb(ref)
}

func (r *refWalker) walkBlobLeafSequence() {
	size := r.readCount()
	r.offset += uint32(size)
}

func (r *refWalker) walkValueSequence(nbf *NomsBinFormat, cb RefCallback) error {
	count := int(r.readCount())
	for i := 0; i < count; i++ {
		err := r.walkValue(nbf, cb)

		if err != nil {
			return err
		}
	}

	return nil
}

func (r *refWalker) walkList(nbf *NomsBinFormat, cb RefCallback) error {
	return r.walkListOrSet(nbf, ListKind, cb)
}

func (r *refWalker) walkSet(nbf *NomsBinFormat, cb RefCallback) error {
	return r.walkListOrSet(nbf, SetKind, cb)
}

func (r *refWalker) walkListOrSet(nbf *NomsBinFormat, kind NomsKind, cb RefCallback) error {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		return r.walkMetaSequence(nbf, kind, level, cb)
	} else {
		return r.walkValueSequence(nbf, cb)
	}
}

func (r *refWalker) walkMap(nbf *NomsBinFormat, cb RefCallback) error {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		return r.walkMetaSequence(nbf, MapKind, level, cb)
	} else {
		return r.walkMapLeafSequence(nbf, cb)
	}
}

func (r *refWalker) walkBlob(nbf *NomsBinFormat, cb RefCallback) error {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		err := r.walkMetaSequence(nbf, BlobKind, level, cb)

		if err != nil {
			return err
		}
	} else {
		r.walkBlobLeafSequence()
	}

	return nil
}

func (r *refWalker) walkMapLeafSequence(nbf *NomsBinFormat, cb RefCallback) error {
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		err := r.walkValue(nbf, cb) // k

		if err != nil {
			return err
		}

		err = r.walkValue(nbf, cb) // v

		if err != nil {
			return err
		}
	}

	return nil
}

func (r *refWalker) walkMetaSequence(nbf *NomsBinFormat, k NomsKind, level uint64, cb RefCallback) error {
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		err := r.walkRef(nbf, cb) // ref to child sequence

		if err != nil {
			return err
		}

		err = r.skipOrderedKey(nbf)

		if err != nil {
			return err
		}

		r.skipCount() // numLeaves
	}

	return nil
}

func (r *refWalker) skipOrderedKey(nbf *NomsBinFormat) error {
	switch r.peekKind() {
	case hashKind:
		r.skipKind()
		r.skipHash()
	default:
		return r.walkValue(nbf, func(r Ref) error { return nil }) // max Value in subtree reachable from here
	}

	return nil
}

func (r *refWalker) walkValue(nbf *NomsBinFormat, cb RefCallback) error {
	k := r.peekKind()
	switch k {
	case BlobKind:
		return r.walkBlob(nbf, cb)
	case ListKind:
		return r.walkList(nbf, cb)
	case MapKind:
		return r.walkMap(nbf, cb)
	case RefKind:
		return r.walkRef(nbf, cb)
	case SetKind:
		return r.walkSet(nbf, cb)
	case StructKind:
		return r.walkStruct(nbf, cb)
	case TupleKind:
		return r.walkTuple(nbf, cb)
	case ChunkedStringKind:
		return r.walkChunkedString(nbf, cb)
	case TypeKind:
		r.skipKind()
		return r.skipType()
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	default:
		if IsPrimitiveKind(k) {
			if emptyVal := KindToType[k]; emptyVal != nil {
				r.skipKind()
				emptyVal.skip(nbf, &r.binaryNomsReader)
				return nil
			}
		}
		return ErrUnknownType
	}

	return nil
}

func (r *refWalker) walkStruct(nbf *NomsBinFormat, cb RefCallback) error {
	return walkStruct(nbf, r, cb)
}

func (r *refWalker) walkTuple(nbf *NomsBinFormat, cb RefCallback) error {
	return walkTuple(nbf, r, cb)
}

func (r *refWalker) walkChunkedString(nbf *NomsBinFormat, cb RefCallback) error {
	return walkChunkedString(nbf, r, cb)
}
