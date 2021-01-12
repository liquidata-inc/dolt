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

// NomsKind allows a TypeDesc to indicate what kind of type is described.
type NomsKind uint8

// All supported kinds of Noms types are enumerated here.
// The ordering of these (especially Bool, Float and String) is important for ordering of values.
const (
	BoolKind NomsKind = iota
	FloatKind
	StringKind
	BlobKind
	ValueKind
	ListKind
	MapKind
	RefKind
	SetKind

	// Keep StructKind and CycleKind together.
	StructKind
	CycleKind

	TypeKind
	UnionKind

	// Internal to decoder
	hashKind

	UUIDKind
	IntKind
	UintKind
	NullKind
	TupleKind
	InlineBlobKind
	TimestampKind
	DecimalKind
	ChunkedStringKind

	UnknownKind NomsKind = 255
)

var KindToType = make([]Value, 255)
var SupportedKinds = make([]bool, 255)

func init() {
	KindToType[BlobKind] = Blob{}
	KindToType[BoolKind] = Bool(false)
	KindToType[ListKind] = List{}
	KindToType[MapKind] = Map{}
	KindToType[FloatKind] = Float(0)
	KindToType[RefKind] = Ref{}
	KindToType[SetKind] = Set{}
	KindToType[StructKind] = Struct{}
	KindToType[StringKind] = String("")
	KindToType[TypeKind] = &Type{}
	KindToType[UUIDKind] = UUID{}
	KindToType[IntKind] = Int(0)
	KindToType[UintKind] = Uint(0)
	KindToType[NullKind] = NullValue
	KindToType[TupleKind] = EmptyTuple(Format_7_18)
	KindToType[InlineBlobKind] = InlineBlob{}
	KindToType[TimestampKind] = Timestamp{}
	KindToType[DecimalKind] = Decimal{}
	KindToType[ChunkedStringKind] = ChunkedString{}

	SupportedKinds[BlobKind] = true
	SupportedKinds[BoolKind] = true
	SupportedKinds[CycleKind] = true
	SupportedKinds[ListKind] = true
	SupportedKinds[MapKind] = true
	SupportedKinds[FloatKind] = true
	SupportedKinds[RefKind] = true
	SupportedKinds[SetKind] = true
	SupportedKinds[StructKind] = true
	SupportedKinds[StringKind] = true
	SupportedKinds[TypeKind] = true
	SupportedKinds[UnionKind] = true
	SupportedKinds[ValueKind] = true
	SupportedKinds[UUIDKind] = true
	SupportedKinds[IntKind] = true
	SupportedKinds[UintKind] = true
	SupportedKinds[NullKind] = true
	SupportedKinds[TupleKind] = true
	SupportedKinds[InlineBlobKind] = true
	SupportedKinds[TimestampKind] = true
	SupportedKinds[DecimalKind] = true
	SupportedKinds[ChunkedStringKind] = true
}

var KindToTypeSlice []Value

var KindToString = map[NomsKind]string{
	UnknownKind:       "unknown",
	BlobKind:          "Blob",
	BoolKind:          "Bool",
	CycleKind:         "Cycle",
	ListKind:          "List",
	MapKind:           "Map",
	FloatKind:         "Float",
	RefKind:           "Ref",
	SetKind:           "Set",
	StructKind:        "Struct",
	StringKind:        "String",
	TypeKind:          "Type",
	UnionKind:         "Union",
	ValueKind:         "Value",
	UUIDKind:          "UUID",
	IntKind:           "Int",
	UintKind:          "Uint",
	NullKind:          "Null",
	TupleKind:         "Tuple",
	InlineBlobKind:    "InlineBlob",
	TimestampKind:     "Timestamp",
	DecimalKind:       "Decimal",
	ChunkedStringKind: "ChunkedString",
}

// String returns the name of the kind.
func (k NomsKind) String() string {
	return KindToString[k]
}

// IsPrimitiveKind returns true if k represents a Noms primitive type, which excludes collections (List, Map, Set), Refs, Structs, Symbolic and Unresolved types.
func IsPrimitiveKind(k NomsKind) bool {
	i := int(k)
	return i < len(PrimitiveKindMask) && PrimitiveKindMask[i]
}

// isKindOrderedByValue determines if a value is ordered by its value instead of its hash.
func isKindOrderedByValue(k NomsKind) bool {
	return k <= StringKind || k >= UUIDKind
}

func (k NomsKind) writeTo(w nomsWriter, nbf *NomsBinFormat) error {
	if k == UnknownKind {
		return ErrUnknownType
	}

	w.writeUint8(uint8(k))
	return nil
}
