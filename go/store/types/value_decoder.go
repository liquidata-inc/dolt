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
	"errors"

	"github.com/dolthub/dolt/go/store/d"
)

var ErrUnknownType = errors.New("unknown type")

type valueDecoder struct {
	typedBinaryNomsReader
	vrw ValueReadWriter
}

// typedBinaryNomsReader provides some functionality for reading and skipping types that is shared by both valueDecoder and refWalker.
type typedBinaryNomsReader struct {
	binaryNomsReader
	validating bool
}

func newValueDecoder(buff []byte, vrw ValueReadWriter) valueDecoder {
	nr := binaryNomsReader{buff, 0}
	return valueDecoder{typedBinaryNomsReader{nr, false}, vrw}
}

func newValueDecoderWithValidation(nr binaryNomsReader, vrw ValueReadWriter) valueDecoder {
	return valueDecoder{typedBinaryNomsReader{nr, true}, vrw}
}

func (r *valueDecoder) readRef(nbf *NomsBinFormat) (Ref, error) {
	return readRef(nbf, &(r.typedBinaryNomsReader))
}

func (r *valueDecoder) skipRef() error {
	_, err := skipRef(&(r.typedBinaryNomsReader))
	return err
}

func (r *valueDecoder) skipBlobLeafSequence(nbf *NomsBinFormat) ([]uint32, uint64, error) {
	size := r.readCount()
	valuesPos := r.pos()
	r.offset += uint32(size)
	return []uint32{valuesPos, r.pos()}, size, nil
}

func (r *valueDecoder) skipValueSequence(nbf *NomsBinFormat, elementsPerIndex int) ([]uint32, uint64, error) {
	count := r.readCount()
	offsets := make([]uint32, count+1)
	offsets[0] = r.pos()
	for i := uint64(0); i < count; i++ {
		for j := 0; j < elementsPerIndex; j++ {
			err := r.skipValue(nbf)

			if err != nil {
				return nil, 0, err
			}
		}
		offsets[i+1] = r.pos()
	}
	return offsets, count, nil
}

func (r *valueDecoder) skipListLeafSequence(nbf *NomsBinFormat) ([]uint32, uint64, error) {
	return r.skipValueSequence(nbf, getValuesPerIdx(ListKind))
}

func (r *valueDecoder) skipSetLeafSequence(nbf *NomsBinFormat) ([]uint32, uint64, error) {
	return r.skipValueSequence(nbf, getValuesPerIdx(SetKind))
}

func (r *valueDecoder) skipMapLeafSequence(nbf *NomsBinFormat) ([]uint32, uint64, error) {
	return r.skipValueSequence(nbf, getValuesPerIdx(MapKind))
}

func (r *valueDecoder) readSequence(nbf *NomsBinFormat, kind NomsKind, leafSkipper func(nbf *NomsBinFormat) ([]uint32, uint64, error)) (sequence, error) {
	start := r.pos()
	offsets := []uint32{start}
	r.skipKind()
	offsets = append(offsets, r.pos())
	level := r.readCount()
	offsets = append(offsets, r.pos())
	var seqOffsets []uint32
	var length uint64
	if level > 0 {
		var err error
		seqOffsets, length, err = r.skipMetaSequence(nbf, kind, level)

		if err != nil {
			return nil, err
		}
	} else {
		var err error
		seqOffsets, length, err = leafSkipper(nbf)

		if err != nil {
			return nil, err
		}
	}
	offsets = append(offsets, seqOffsets...)
	end := r.pos()

	if level > 0 {
		return newMetaSequence(r.vrw, r.byteSlice(start, end), offsets, length), nil
	}

	return newLeafSequence(r.vrw, r.byteSlice(start, end), offsets, length), nil
}

func (r *valueDecoder) readBlobSequence(nbf *NomsBinFormat) (sequence, error) {
	seq, err := r.readSequence(nbf, BlobKind, r.skipBlobLeafSequence)

	if err != nil {
		return nil, err
	}

	if seq.isLeaf() {
		return blobLeafSequence{seq.(leafSequence)}, nil
	}

	return seq, nil
}

func (r *valueDecoder) readListSequence(nbf *NomsBinFormat) (sequence, error) {
	seq, err := r.readSequence(nbf, ListKind, r.skipListLeafSequence)

	if err != nil {
		return nil, err
	}

	if seq.isLeaf() {
		return listLeafSequence{seq.(leafSequence)}, nil
	}
	return seq, nil
}

func (r *valueDecoder) readSetSequence(nbf *NomsBinFormat) (orderedSequence, error) {
	seq, err := r.readSequence(nbf, SetKind, r.skipSetLeafSequence)

	if err != nil {
		return nil, err
	}

	if seq.isLeaf() {
		return setLeafSequence{seq.(leafSequence)}, nil
	}

	return seq.(orderedSequence), nil
}

func (r *valueDecoder) readMapSequence(nbf *NomsBinFormat) (orderedSequence, error) {
	seq, err := r.readSequence(nbf, MapKind, r.skipMapLeafSequence)

	if err != nil {
		return nil, err
	}

	if seq.isLeaf() {
		return mapLeafSequence{seq.(leafSequence)}, nil
	}

	return seq.(orderedSequence), nil
}

func (r *valueDecoder) skipList(nbf *NomsBinFormat) error {
	return r.skipSequence(nbf, ListKind, r.skipListLeafSequence)
}

func (r *valueDecoder) skipSet(nbf *NomsBinFormat) error {
	return r.skipSequence(nbf, SetKind, r.skipSetLeafSequence)
}

func (r *valueDecoder) skipMap(nbf *NomsBinFormat) error {
	return r.skipSequence(nbf, MapKind, r.skipMapLeafSequence)
}

func (r *valueDecoder) skipBlob(nbf *NomsBinFormat) error {
	return r.skipSequence(nbf, BlobKind, r.skipBlobLeafSequence)
}

func (r *valueDecoder) skipSequence(nbf *NomsBinFormat, kind NomsKind, leafSkipper func(nbf *NomsBinFormat) ([]uint32, uint64, error)) error {
	r.skipKind()
	level := r.readCount()
	if level > 0 {
		_, _, err := r.skipMetaSequence(nbf, kind, level)

		if err != nil {
			return err
		}
	} else {
		_, _, err := leafSkipper(nbf)

		if err != nil {
			return err
		}
	}

	return nil
}

func (r *valueDecoder) skipOrderedKey(nbf *NomsBinFormat) error {
	switch r.peekKind() {
	case hashKind:
		r.skipKind()
		r.skipHash()
	default:
		err := r.skipValue(nbf)

		if err != nil {
			return err
		}
	}

	return nil
}

func (r *valueDecoder) skipMetaSequence(nbf *NomsBinFormat, k NomsKind, level uint64) ([]uint32, uint64, error) {
	count := r.readCount()
	offsets := make([]uint32, count+1)
	offsets[0] = r.pos()
	length := uint64(0)
	for i := uint64(0); i < count; i++ {
		err := r.skipRef()

		if err != nil {
			return nil, 0, err
		}

		err = r.skipOrderedKey(nbf)

		if err != nil {
			return nil, 0, err
		}

		length += r.readCount()
		offsets[i+1] = r.pos()
	}
	return offsets, length, nil
}

func (r *valueDecoder) readValue(nbf *NomsBinFormat) (Value, error) {
	k := r.peekKind()
	switch k {
	case BlobKind:
		seq, err := r.readBlobSequence(nbf)
		if err != nil {
			return nil, err
		}
		return newBlob(seq), nil
	// The following primitive kinds, through StringKind, are also
	// able to be processed by the IsPrimitiveKind branch below.
	// But we include them here for efficiency, since it is about
	// 30% faster to dispatch statically to the correct reader for
	// these very common primitive types.
	case BoolKind:
		r.skipKind()
		return Bool(r.readBool()), nil
	case FloatKind:
		r.skipKind()
		return Float(r.readFloat(nbf)), nil
	case IntKind:
		r.skipKind()
		return Int(r.readInt()), nil
	case UintKind:
		r.skipKind()
		return Uint(r.readUint()), nil
	case NullKind:
		r.skipKind()
		return NullValue, nil
	case StringKind:
		r.skipKind()
		return String(r.readString()), nil
	case ListKind:
		seq, err := r.readListSequence(nbf)
		if err != nil {
			return nil, err
		}
		return newList(seq), nil
	case MapKind:
		seq, err := r.readMapSequence(nbf)
		if err != nil {
			return nil, err
		}
		return newMap(seq), nil
	case RefKind:
		return r.readRef(nbf)
	case SetKind:
		seq, err := r.readSetSequence(nbf)
		if err != nil {
			return nil, err
		}
		return newSet(seq), nil
	case StructKind:
		return r.readStruct(nbf)
	case TupleKind:
		return r.readTuple(nbf)
	case ChunkedStringKind:
		return r.readChunkedString(nbf)
	case TypeKind:
		r.skipKind()
		return r.readType()
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	}

	if IsPrimitiveKind(k) {
		if int(k) < len(KindToTypeSlice) {
			emptyVal := KindToTypeSlice[int(k)]
			if emptyVal != nil {
				r.skipKind()
				return emptyVal.readFrom(nbf, &r.binaryNomsReader)
			}
		}
	}

	return nil, ErrUnknownType
}

func (r *valueDecoder) skipValue(nbf *NomsBinFormat) error {
	k := r.peekKind()
	switch k {
	case BlobKind:
		err := r.skipBlob(nbf)
		if err != nil {
			return err
		}
	// The following primitive kinds, through StringKind, are also
	// able to be processed by the IsPrimitiveKind branch below.
	// But we include them here for efficiency, since it is about
	// 30% faster to dispatch statically to the correct reader for
	// these very common primitive types.
	case BoolKind:
		r.skipKind()
		r.skipBool()
	case FloatKind:
		r.skipKind()
		r.skipFloat(nbf)
	case NullKind:
		r.skipKind()
	case IntKind:
		r.skipKind()
		r.skipInt()
	case UintKind:
		r.skipKind()
		r.skipUint()
	case StringKind:
		r.skipKind()
		r.skipString()
	case ListKind:
		err := r.skipList(nbf)
		if err != nil {
			return err
		}
	case MapKind:
		err := r.skipMap(nbf)
		if err != nil {
			return err
		}
	case RefKind:
		err := r.skipRef()
		if err != nil {
			return err
		}
	case SetKind:
		err := r.skipSet(nbf)
		if err != nil {
			return err
		}
	case StructKind:
		err := r.skipStruct(nbf)
		if err != nil {
			return err
		}
	case TupleKind:
		err := r.skipTuple(nbf)
		if err != nil {
			return err
		}
	case ChunkedStringKind:
		err := r.skipChunkedString(nbf)
		if err != nil {
			return err
		}
	case TypeKind:
		r.skipKind()
		err := r.skipType()
		if err != nil {
			return err
		}
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	default:
		if IsPrimitiveKind(k) {
			if int(k) < len(KindToTypeSlice) {
				emptyVal := KindToTypeSlice[int(k)]
				if emptyVal != nil {
					r.skipKind()
					emptyVal.skip(nbf, &r.binaryNomsReader)
					return nil
				}
			}
		}
		return ErrUnknownType
	}

	return nil
}

// readTypeOfValue is basically readValue().typeOf() but it ensures that we do
// not allocate values where we do not need to.
func (r *valueDecoder) readTypeOfValue(nbf *NomsBinFormat) (*Type, error) {
	k := r.peekKind()
	switch k {
	case BlobKind:
		err := r.skipBlob(nbf)
		if err != nil {
			return nil, err
		}
		return PrimitiveTypeMap[BlobKind], nil
	case ListKind, MapKind, RefKind, SetKind:
		// These do not decode the actual values anyway.
		val, err := r.readValue(nbf)
		if err != nil {
			return nil, err
		}
		d.Chk.True(val != nil)
		return val.typeOf()
	case StructKind:
		return readStructTypeOfValue(nbf, r)
	case TupleKind:
		val, err := r.readValue(nbf)
		if err != nil {
			return nil, err
		}
		d.Chk.True(val != nil)
		return val.typeOf()
	case ChunkedStringKind:
		val, err := r.readValue(nbf)
		if err != nil {
			return nil, err
		}
		d.Chk.True(val != nil)
		return val.typeOf()
	case TypeKind:
		r.skipKind()
		err := r.skipType()
		if err != nil {
			return nil, err
		}
		return PrimitiveTypeMap[TypeKind], nil
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	}

	if IsPrimitiveKind(k) {
		if emptyVal := KindToType[k]; emptyVal != nil {
			r.skipKind()
			emptyVal.skip(nbf, &r.binaryNomsReader)
			return PrimitiveTypeMap[k], nil
		}
	}

	return nil, ErrUnknownType
}

// isValueSameTypeForSure may return false even though the type of the value is
// equal. We do that in cases where it would be too expensive to compute the
// type.
// If this returns false the decoder might not have visited the whole value and
// its offset is no longer valid.
func (r *valueDecoder) isValueSameTypeForSure(nbf *NomsBinFormat, t *Type) (bool, error) {
	k := r.peekKind()
	if k != t.TargetKind() {
		return false, nil
	}

	switch k {
	case ListKind, MapKind, RefKind, SetKind, TupleKind, ChunkedStringKind:
		// TODO: Maybe do some simple cases here too. Performance metrics should determine
		// what is going to be worth doing.
		// https://github.com/attic-labs/noms/issues/3776
		return false, nil
	case StructKind:
		return isStructSameTypeForSure(nbf, r, t)
	case TypeKind:
		return false, nil
	case CycleKind, UnionKind, ValueKind:
		d.Panic("A value instance can never have type %s", k)
	}

	// Captures all other types that are not the above special cases
	if IsPrimitiveKind(k) {
		err := r.skipValue(nbf)
		if err != nil {
			return false, err
		}

		return true, nil
	}

	panic("non-primitive type not special cased")
}

// isStringSame checks if the next string in the decoder matches string. It
// moves the decoder to after the string in all cases.
func (r *valueDecoder) isStringSame(s string) bool {
	count := r.readCount()
	start := uint64(r.offset)
	r.offset += uint32(count)
	if uint64(len(s)) != count {
		return false
	}

	for i := uint64(0); i < count; i++ {
		if s[i] != r.buff[start+i] {
			return false
		}
	}
	return true
}

func (r *valueDecoder) readStruct(nbf *NomsBinFormat) (Value, error) {
	return readStruct(nbf, r)
}

func (r *valueDecoder) readTuple(nbf *NomsBinFormat) (Value, error) {
	return readTuple(nbf, r)
}

func (r *valueDecoder) readChunkedString(nbf *NomsBinFormat) (Value, error) {
	return readChunkedString(nbf, r)
}

func (r *valueDecoder) skipStruct(nbf *NomsBinFormat) error {
	return skipStruct(nbf, r)
}

func (r *valueDecoder) skipTuple(nbf *NomsBinFormat) error {
	return skipTuple(nbf, r)
}

func (r *valueDecoder) skipChunkedString(nbf *NomsBinFormat) error {
	return skipChunkedString(nbf, r)
}

func (r *valueDecoder) readOrderedKey(nbf *NomsBinFormat) (orderedKey, error) {
	switch r.peekKind() {
	case hashKind:
		r.skipKind()
		h := r.readHash()
		return orderedKeyFromHash(h), nil
	default:
		v, err := r.readValue(nbf)

		if err != nil {
			return orderedKey{}, err
		}

		return newOrderedKey(v, nbf)
	}
}

func (r *typedBinaryNomsReader) readType() (*Type, error) {
	t, err := r.readTypeInner(map[string]*Type{})

	if err != nil {
		return nil, err
	}

	if r.validating {
		validateType(t)
	}
	return t, nil
}

func (r *typedBinaryNomsReader) skipType() error {
	if r.validating {
		_, err := r.readType()

		if err != nil {
			return err
		}
	}
	r.skipTypeInner()
	return nil
}

func (r *typedBinaryNomsReader) readTypeInner(seenStructs map[string]*Type) (*Type, error) {
	k := r.readKind()

	if supported := SupportedKinds[k]; !supported {
		return nil, ErrUnknownType
	}

	switch k {
	case ListKind:
		t, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		return makeCompoundType(ListKind, t)
	case MapKind:
		kt, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		vt, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		return makeCompoundType(MapKind, kt, vt)
	case RefKind:
		t, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		return makeCompoundType(RefKind, t)
	case SetKind:
		t, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		return makeCompoundType(SetKind, t)
	case StructKind:
		return r.readStructType(seenStructs)
	case TupleKind:
		t, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		return makeCompoundType(TupleKind, t)
	case ChunkedStringKind:
		t, err := r.readTypeInner(seenStructs)
		if err != nil {
			return nil, err
		}
		return makeCompoundType(ChunkedStringKind, t)
	case UnionKind:
		t, err := r.readUnionType(seenStructs)

		if err != nil {
			return nil, err
		}

		return t, nil
	case CycleKind:
		name := r.readString()
		d.PanicIfTrue(name == "") // cycles to anonymous structs are disallowed
		t, ok := seenStructs[name]
		d.PanicIfFalse(ok)
		return t, nil
	}

	d.PanicIfFalse(IsPrimitiveKind(k))
	return MakePrimitiveType(k)
}

func (r *typedBinaryNomsReader) skipTypeInner() {
	k := r.readKind()
	switch k {
	case ListKind, RefKind, SetKind, TupleKind, ChunkedStringKind:
		r.skipTypeInner()
	case MapKind:
		r.skipTypeInner()
		r.skipTypeInner()
	case StructKind:
		r.skipStructType()
	case UnionKind:
		r.skipUnionType()
	case CycleKind:
		r.skipString()
	default:
		d.PanicIfFalse(IsPrimitiveKind(k))
	}
}

func (r *typedBinaryNomsReader) readStructType(seenStructs map[string]*Type) (*Type, error) {
	name := r.readString()
	count := r.readCount()
	fields := make(structTypeFields, count)

	t := newType(StructDesc{name, fields})
	seenStructs[name] = t

	for i := uint64(0); i < count; i++ {
		t.Desc.(StructDesc).fields[i] = StructField{
			Name: r.readString(),
		}
	}
	for i := uint64(0); i < count; i++ {
		inType, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		t.Desc.(StructDesc).fields[i].Type = inType
	}
	for i := uint64(0); i < count; i++ {
		t.Desc.(StructDesc).fields[i].Optional = r.readBool()
	}

	return t, nil
}

func (r *typedBinaryNomsReader) skipStructType() {
	r.skipString() // name
	count := r.readCount()

	for i := uint64(0); i < count; i++ {
		r.skipString() // name
	}
	for i := uint64(0); i < count; i++ {
		r.skipTypeInner()
	}
	for i := uint64(0); i < count; i++ {
		r.skipBool() // optional
	}
}

func (r *typedBinaryNomsReader) readUnionType(seenStructs map[string]*Type) (*Type, error) {
	l := r.readCount()
	ts := make(typeSlice, l)
	for i := uint64(0); i < l; i++ {
		t, err := r.readTypeInner(seenStructs)

		if err != nil {
			return nil, err
		}

		ts[i] = t
	}
	return makeUnionType(ts...)
}

func (r *typedBinaryNomsReader) skipUnionType() {
	l := r.readCount()
	for i := uint64(0); i < l; i++ {
		r.skipTypeInner()
	}
}
