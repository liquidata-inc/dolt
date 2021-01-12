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

package types

import "github.com/dolthub/dolt/go/store/d"

// ContainCommonSupertype returns true if it's possible to synthesize
// a non-trivial (i.e. not empty) supertype from types |a| and |b|.
//
// It is useful for determining whether a subset of values can be extracted
// from one object to produce another object.
//
// The rules for determining whether |a| and |b| intersect are:
//    - if either type is Value, return true
//    - if either type is Union, return true iff at least one variant of |a| intersects with one variant of |b|
//    - if |a| & |b| are not the same kind, return false
//    - else
//      - if both are structs, return true iff their names are equal or one name is "", they share a field name
//        and the type of that field intersects
//      - if both are refs, sets or lists, return true iff the element type intersects
//      - if both are maps, return true iff they have a key with the same type and value types that intersect
//      - else return true
func ContainCommonSupertype(nbf *NomsBinFormat, a, b *Type) bool {
	// Avoid cycles internally.
	return containCommonSupertypeImpl(nbf, a, b, nil, nil)
}

func containCommonSupertypeImpl(nbf *NomsBinFormat, a, b *Type, aVisited, bVisited []*Type) bool {
	if a.TargetKind() == ValueKind || b.TargetKind() == ValueKind {
		return true
	}
	if a.TargetKind() == UnionKind || b.TargetKind() == UnionKind {
		return unionsIntersect(nbf, a, b, aVisited, bVisited)
	}
	if a.TargetKind() != b.TargetKind() {
		return false
	}
	switch k := a.TargetKind(); k {
	case StructKind:
		return structsIntersect(nbf, a, b, aVisited, bVisited)
	case ListKind, SetKind, RefKind, TupleKind, ChunkedStringKind:
		return containersIntersect(nbf, k, a, b, aVisited, bVisited)
	case MapKind:
		return mapsIntersect(nbf, a, b, aVisited, bVisited)
	default:
		return true
	}

}

// Checks for intersection between types that may be unions. If either or
// both is a union, union, tests all types for intersection.
func unionsIntersect(nbf *NomsBinFormat, a, b *Type, aVisited, bVisited []*Type) bool {
	aTypes, bTypes := typeList(a), typeList(b)
	for _, t := range aTypes {
		for _, u := range bTypes {
			if containCommonSupertypeImpl(nbf, t, u, aVisited, bVisited) {
				return true
			}
		}
	}
	return false
}

// if |t| is a union, returns all types represented; otherwise returns |t|
func typeList(t *Type) typeSlice {
	if t.Desc.Kind() == UnionKind {
		return t.Desc.(CompoundDesc).ElemTypes
	}
	return typeSlice{t}
}

func containersIntersect(nbf *NomsBinFormat, kind NomsKind, a, b *Type, aVisited, bVisited []*Type) bool {
	d.Chk.True(kind == a.Desc.Kind() && kind == b.Desc.Kind())
	return containCommonSupertypeImpl(nbf, a.Desc.(CompoundDesc).ElemTypes[0], b.Desc.(CompoundDesc).ElemTypes[0], aVisited, bVisited)
}

func mapsIntersect(nbf *NomsBinFormat, a, b *Type, aVisited, bVisited []*Type) bool {
	// true if a and b are the same or (if either is a union) there is
	// common type between them.
	hasCommonType := func(a, b *Type) bool {
		aTypes, bTypes := typeList(a), typeList(b)
		for _, t := range aTypes {
			for _, u := range bTypes {
				if t.Equals(u) {
					return true
				}
			}
		}
		return false
	}

	d.Chk.True(MapKind == a.Desc.Kind() && MapKind == b.Desc.Kind())

	aDesc, bDesc := a.Desc.(CompoundDesc), b.Desc.(CompoundDesc)
	if !hasCommonType(aDesc.ElemTypes[0], bDesc.ElemTypes[0]) {
		return false
	}
	return containCommonSupertypeImpl(nbf, aDesc.ElemTypes[1], bDesc.ElemTypes[1], aVisited, bVisited)
}

func structsIntersect(nbf *NomsBinFormat, a, b *Type, aVisited, bVisited []*Type) bool {
	_, aFound := indexOfType(a, aVisited)
	_, bFound := indexOfType(b, bVisited)

	if aFound && bFound {
		return true
	}

	d.Chk.True(StructKind == a.TargetKind() && StructKind == b.TargetKind())
	aDesc := a.Desc.(StructDesc)
	bDesc := b.Desc.(StructDesc)
	// must be either the same name or one has no name
	if aDesc.Name != bDesc.Name && !(aDesc.Name == "" || bDesc.Name == "") {
		return false
	}
	for i, j := 0, 0; i < len(aDesc.fields) && j < len(bDesc.fields); {
		aName, bName := aDesc.fields[i].Name, bDesc.fields[j].Name
		if aName < bName {
			i++
		} else if bName < aName {
			j++
		} else if !containCommonSupertypeImpl(nbf, aDesc.fields[i].Type, bDesc.fields[j].Type, append(aVisited, a), append(bVisited, b)) {
			i++
			j++
		} else {
			return true
		}
	}
	return false
}
