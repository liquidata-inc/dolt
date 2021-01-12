// Copyright 2020 Dolthub, Inc.
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

package types

import (
	"context"
	"errors"
	"math"
)

type ChunkedString struct {
	valueImpl
}

var _ Value = ChunkedString{}
var _ valueReadWriter = ChunkedString{}

//TODO: Consolidate all chunk size literals into global constants (this should be the smallest constant)
const maxChunkedStringChunk = 1<<27 - 1024

// NewChunkedString returns a new ChunkedString.
func NewChunkedString(ctx context.Context, vrw ValueReadWriter, s string) (ChunkedString, error) {
	strVal := String(s)
	var strChunks []String
	for true {
		if len(strVal) > maxChunkedStringChunk {
			strChunks = append(strChunks, strVal[:maxChunkedStringChunk])
			strVal = strVal[maxChunkedStringChunk:]
		} else {
			strChunks = append(strChunks, strVal)
			break
		}
	}

	w := newBinaryNomsWriter()
	err := ChunkedStringKind.writeTo(&w, vrw.Format())
	if err != nil {
		return ChunkedString{}, err
	}
	w.writeCount(uint64(len(strVal)))
	w.writeCount(uint64(len(strChunks)))

	for _, val := range strChunks {
		valRef, err := NewRef(val, vrw.Format())
		if err != nil {
			return ChunkedString{}, err
		}
		valRef.vrw = vrw
		targetVal, err := valRef.TargetValue(ctx, vrw)
		if err != nil {
			return ChunkedString{}, err
		}
		if targetVal == nil {
			_, err = vrw.WriteValue(ctx, val)
			if err != nil {
				return ChunkedString{}, err
			}
		}
		err = valRef.writeTo(&w, vrw.Format())
		if err != nil {
			return ChunkedString{}, err
		}
	}
	return ChunkedString{valueImpl{vrw, vrw.Format(), w.data(), nil}}, nil
}

// readChunkedString reads the data provided by a decoder and moves the decoder forward.
func readChunkedString(nbf *NomsBinFormat, dec *valueDecoder) (ChunkedString, error) {
	start := dec.pos()
	err := skipChunkedString(nbf, dec)
	if err != nil {
		return ChunkedString{}, err
	}
	end := dec.pos()
	return ChunkedString{valueImpl{dec.vrw, nbf, dec.byteSlice(start, end), nil}}, nil
}

// skipChunkedString moves the decoder past this ChunkedString.
func skipChunkedString(nbf *NomsBinFormat, dec *valueDecoder) error {
	dec.skipKind()
	dec.skipCount()
	count := dec.readCount()
	for i := uint64(0); i < count; i++ {
		err := dec.skipValue(nbf)
		if err != nil {
			return err
		}
	}
	return nil
}

// walkChunkedString runs the given callback on each contained Ref.
func walkChunkedString(nbf *NomsBinFormat, r *refWalker, cb RefCallback) error {
	r.skipKind()
	r.skipCount()
	count := r.readCount()
	for i := uint64(0); i < count; i++ {
		err := r.walkValue(nbf, cb)
		if err != nil {
			return err
		}
	}
	return nil
}

// Value implements the Value interface.
func (v ChunkedString) Value(_ context.Context) (Value, error) {
	return v, nil
}

// Less implements the Value interface.
func (v ChunkedString) Less(nbf *NomsBinFormat, other LesserValuable) (bool, error) {
	if otherCS, ok := other.(ChunkedString); ok {
		ctx := context.Background()
		itr, err := v.Iterator()
		if err != nil {
			return false, err
		}
		otherItr, err := otherCS.Iterator()
		if err != nil {
			return false, err
		}

		for itr.HasMore() {
			if !otherItr.HasMore() {
				// equal up til the end of other. other is shorter, therefore it is less
				return false, nil
			}
			_, ref, err := itr.Next()
			if err != nil {
				return false, err
			}
			_, otherRef, err := otherItr.Next()
			if err != nil {
				return false, err
			}
			val, err := ref.(Ref).TargetValue(ctx, v.vrw)
			if err != nil {
				return false, err
			}
			otherVal, err := otherRef.(Ref).TargetValue(ctx, otherCS.vrw)
			if err != nil {
				return false, err
			}
			if val == nil || otherVal == nil {
				return false, errors.New("failed to read from ChunkedString")
			}
			if !val.Equals(otherVal) {
				return val.Less(nbf, otherVal)
			}
		}
		return itr.Len() < otherItr.Len(), nil
	}
	return ChunkedStringKind < other.Kind(), nil
}

// isPrimitive implements the Value interface.
func (v ChunkedString) isPrimitive() bool {
	return false
}

// typeOf implements the Value interface.
func (v ChunkedString) typeOf() (*Type, error) {
	dec, count := v.decoderSkipToFields()
	ts := make(typeSlice, 0, count)

	var lastType *Type
	for i := uint64(0); i < count; i++ {
		if lastType != nil {
			offset := dec.offset
			is, err := dec.isValueSameTypeForSure(v.format(), lastType)
			if err != nil {
				return nil, err
			}
			if is {
				continue
			}
			dec.offset = offset
		}

		var err error
		lastType, err = dec.readTypeOfValue(v.format())
		if err != nil {
			return nil, err
		}
		if lastType.Kind() == UnknownKind {
			// if any of the elements are unknown, return unknown
			return nil, ErrUnknownType
		}
		ts = append(ts, lastType)
	}
	ut, err := makeUnionType(ts...)
	if err != nil {
		return nil, err
	}
	return makeCompoundType(ChunkedStringKind, ut)
}

// Kind implements the Value interface.
func (v ChunkedString) Kind() NomsKind {
	return ChunkedStringKind
}

// readFrom implements the Value interface.
func (v ChunkedString) readFrom(*NomsBinFormat, *binaryNomsReader) (Value, error) {
	panic("unreachable")
}

// skip implements the Value interface.
func (v ChunkedString) skip(format *NomsBinFormat, reader *binaryNomsReader) {
	panic("unreachable")
}

// WalkValues implements the Value interface.
func (v ChunkedString) WalkValues(ctx context.Context, cb ValueCallback) error {
	dec, count := v.decoderSkipToFields()
	for i := uint64(0); i < count; i++ {
		v, err := dec.readValue(v.format())
		if err != nil {
			return err
		}
		if err = cb(v); err != nil {
			return err
		}
	}
	return nil
}

// HumanReadableString implements the Value interface.
func (v ChunkedString) HumanReadableString() string {
	return "ChunkedString"
}

// RefCount returns the number of Refs in the ChunkedString.
func (v ChunkedString) RefCount() uint64 {
	if len(v.buff) == 0 {
		return 0
	}
	_, count := v.decoderSkipToFields()
	return count
}

// StringLen returns the full length of the ChunkedString.
func (v ChunkedString) StringLen() uint64 {
	dec := v.decoder()
	dec.skipKind()
	return dec.readCount()
}

// Format returns the NomsBinFormat.
func (v ChunkedString) Format() *NomsBinFormat {
	return v.format()
}

// Iterator returns an iterator that returns the Refs that comprise a ChunkedString.
func (v ChunkedString) Iterator() (*TupleIterator, error) {
	return v.IteratorAt(0)
}

// IteratorAt returns an iterator that returns the Refs that comprise a ChunkedString, starting at the given position.
func (v ChunkedString) IteratorAt(pos uint64) (*TupleIterator, error) {
	dec, count := v.decoderSkipToFields()
	for i := uint64(0); i < pos; i++ {
		err := dec.skipValue(v.format())
		if err != nil {
			return nil, err
		}
	}
	// ChunkedString is very similar to Tuple, so we can make use of the TupleIterator here
	return &TupleIterator{&dec, count, pos, v.format()}, nil
}

// decoderSkipToFields returns a valueDecoder that skips the metadata, while also returning the number of Refs present.
func (v ChunkedString) decoderSkipToFields() (valueDecoder, uint64) {
	dec := v.decoder()
	dec.skipKind()
	dec.skipCount()          // first count is string length
	count := dec.readCount() // second count is ref count
	return dec, count
}

// ReadString returns the contained String up to the given length. If the given length is greater than the contained
// String, then the full String is returned. If length is <= 0, then the full String is returned.
func (v ChunkedString) ReadString(ctx context.Context, length int64) (String, error) {
	if length <= 0 {
		length = math.MaxInt64
	}
	itr, err := v.Iterator()
	if err != nil {
		return "", err
	}

	var s String
	for itr.HasMore() {
		_, ref, err := itr.Next()
		if err != nil {
			return "", err
		}
		val, err := ref.(Ref).TargetValue(ctx, v.vrw)
		if err != nil {
			return "", err
		}
		if val == nil {
			return "", errors.New("failed to read from ChunkedString")
		}
		strVal := val.(String)

		totalLength := int64(len(s) + len(strVal))
		if totalLength <= length {
			s += strVal
			if totalLength == length {
				break
			}
		} else {
			s += strVal[:length-int64(len(s))]
			break
		}
	}
	return s, nil
}
