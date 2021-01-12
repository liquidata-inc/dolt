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

import (
	"sort"

	"github.com/dolthub/dolt/go/store/d"
)

// simplifyType returns a type that is a super type of the input type but is
// much smaller and less complex than a straight union of all those types would
// be.
//
// The resulting type is guaranteed to:
// a. be a super type of the input type
// b. have all unions flattened (no union inside a union)
// c. have all unions folded, which means the union
//    1. have at most one element each of kind Ref, Set, List, and Map
//    2. have at most one struct element with a given name
// e. all named unions are pointing at the same simplified struct, which means
//    that all named unions with the same name form cycles.
// f. all cycle type that can be resolved have been resolved.
// g. all types reachable from it also fulfill b-f
//
// The union folding is created roughly as follows:
//
// - The input types are deduplicated
// - Any unions in the input set are "flattened" into the input set
// - The inputs are grouped into categories:
//    - ref
//    - list
//    - set
//    - map
//    - struct, by name (each unique struct name will have its own group)
// - The ref, set, and list groups are collapsed like so:
//     {Ref<A>,Ref<B>,...} -> Ref<A|B|...>
// - The map group is collapsed like so:
//     {Map<K1,V1>|Map<K2,V2>...} -> Map<K1|K2,V1|V2>
// - Each struct group is collapsed like so:
//     {struct{foo:number,bar:string}, struct{bar:blob, baz:bool}} ->
//       struct{foo?:number,bar:string|blob,baz?:bool}
//
// All the above rules are applied recursively.
func simplifyType(t *Type, intersectStructs bool) (*Type, error) {
	if t.Desc.isSimplifiedForSure() {
		return t, nil
	}

	// 1. Clone tree because we are going to mutate it
	//    1.1 Replace all named structs and cycle types with a single `struct Name {}`
	// 2. When a union type is found change its elemTypes as needed
	//    2.1 Merge unnamed structs
	// 3. Update the fields of all named structs

	namedStructs := map[string]structInfo{}

	clone := cloneTypeTreeAndReplaceNamedStructs(t, namedStructs)
	folded, err := foldUnions(clone, typeset{}, intersectStructs)

	if err != nil {
		return nil, err
	}

	for name, info := range namedStructs {
		if len(info.sources) == 0 {
			d.PanicIfTrue(name == "")
			info.instance.Desc = CycleDesc(name)
		} else {
			fields, err := foldStructTypesFieldsOnly(name, info.sources, typeset{}, intersectStructs)

			if err != nil {
				return nil, err
			}

			info.instance.Desc = StructDesc{name, fields}
		}
	}

	return folded, nil
}

// typeset is a helper that aggregates the unique set of input types for this algorithm, flattening
// any unions recursively.
type typeset map[*Type]struct{}

func (ts typeset) add(t *Type) {
	switch t.TargetKind() {
	case UnionKind:
		for _, et := range t.Desc.(CompoundDesc).ElemTypes {
			ts.add(et)
		}
	default:
		ts[t] = struct{}{}
	}
}

func (ts typeset) has(t *Type) bool {
	_, ok := ts[t]
	return ok
}

type structInfo struct {
	instance *Type
	sources  typeset
}

func cloneTypeTreeAndReplaceNamedStructs(t *Type, namedStructs map[string]structInfo) *Type {
	getNamedStruct := func(name string, t *Type) *Type {
		record := namedStructs[name]
		if t.TargetKind() == StructKind {
			record.sources.add(t)
		}
		return record.instance
	}

	ensureInstance := func(name string) {
		if _, ok := namedStructs[name]; !ok {
			instance := newType(StructDesc{Name: name})
			namedStructs[name] = structInfo{instance, typeset{}}
		}
	}

	seenStructs := typeset{}
	var rec func(t *Type) *Type
	rec = func(t *Type) *Type {
		kind := t.TargetKind()

		if IsPrimitiveKind(kind) {
			return t
		}

		switch kind {
		case ListKind, MapKind, RefKind, SetKind, UnionKind, TupleKind, ChunkedStringKind:
			elemTypes := make(typeSlice, len(t.Desc.(CompoundDesc).ElemTypes))
			for i, et := range t.Desc.(CompoundDesc).ElemTypes {
				elemTypes[i] = rec(et)
			}
			return newType(CompoundDesc{kind, elemTypes})
		case StructKind:
			desc := t.Desc.(StructDesc)
			name := desc.Name

			if name != "" {
				ensureInstance(name)
				if seenStructs.has(t) {
					return namedStructs[name].instance
				}
			} else if seenStructs.has(t) {
				// It is OK to use the same unnamed struct type in multiple places.
				// Do not clone it again.
				return t
			}
			seenStructs.add(t)

			fields := make(structTypeFields, len(desc.fields))
			for i, f := range desc.fields {
				fields[i] = StructField{f.Name, rec(f.Type), f.Optional}
			}
			newStruct := newType(StructDesc{name, fields})
			if name == "" {
				return newStruct
			}

			return getNamedStruct(name, newStruct)

		case CycleKind:
			name := string(t.Desc.(CycleDesc))
			d.PanicIfTrue(name == "")
			ensureInstance(name)
			return getNamedStruct(name, t)

		default:
			panic("Unknown noms kind")
		}
	}

	return rec(t)
}

func foldUnions(t *Type, seenStructs typeset, intersectStructs bool) (*Type, error) {
	var err error

	kind := t.TargetKind()
	if !IsPrimitiveKind(kind) {
		switch kind {
		case CycleKind:
			break

		case ListKind, MapKind, RefKind, SetKind, TupleKind, ChunkedStringKind:
			elemTypes := t.Desc.(CompoundDesc).ElemTypes
			for i, et := range elemTypes {
				elemTypes[i], err = foldUnions(et, seenStructs, intersectStructs)

				if err != nil {
					return nil, err
				}
			}

		case StructKind:
			if seenStructs.has(t) {
				return t, nil
			}
			seenStructs.add(t)
			fields := t.Desc.(StructDesc).fields
			for i, f := range fields {
				fields[i].Type, err = foldUnions(f.Type, seenStructs, intersectStructs)

				if err != nil {
					return nil, err
				}
			}

		case UnionKind:
			elemTypes := t.Desc.(CompoundDesc).ElemTypes
			if len(elemTypes) == 0 {
				break
			}
			ts := make(typeset, len(elemTypes))
			for _, t := range elemTypes {
				ts.add(t)
			}
			if len(ts) == 0 {
				t.Desc = CompoundDesc{UnionKind, nil}
				return t, nil
			}
			return foldUnionImpl(ts, seenStructs, intersectStructs)

		default:
			panic("Unknown noms kind")
		}
	}
	return t, nil
}

func foldUnionImpl(ts typeset, seenStructs typeset, intersectStructs bool) (*Type, error) {
	type how struct {
		k NomsKind
		n string
	}
	out := make(typeSlice, 0, len(ts))
	groups := map[how]typeset{}
	for t := range ts {
		var h how
		switch t.TargetKind() {
		case RefKind, SetKind, ListKind, MapKind, TupleKind, ChunkedStringKind:
			h = how{k: t.TargetKind()}
		case StructKind:
			h = how{k: t.TargetKind(), n: t.Desc.(StructDesc).Name}
		default:
			out = append(out, t)
			continue
		}
		g := groups[h]
		if g == nil {
			g = typeset{}
			groups[h] = g
		}
		g.add(t)
	}

	for h, ts := range groups {
		if len(ts) == 1 {
			for t := range ts {
				out = append(out, t)
			}
			continue
		}

		var r *Type
		var err error
		switch h.k {
		case ListKind, RefKind, SetKind, TupleKind, ChunkedStringKind:
			r, err = foldCompoundTypesForUnion(h.k, ts, seenStructs, intersectStructs)
		case MapKind:
			r, err = foldMapTypesForUnion(ts, seenStructs, intersectStructs)
		case StructKind:
			r, err = foldStructTypes(h.n, ts, seenStructs, intersectStructs)
		}

		if err != nil {
			return nil, err
		}

		out = append(out, r)
	}

	for i, t := range out {
		var err error
		out[i], err = foldUnions(t, seenStructs, intersectStructs)

		if err != nil {
			return nil, err
		}
	}

	if len(out) == 1 {
		return out[0], nil
	}

	sort.Sort(out)

	return newType(CompoundDesc{UnionKind, out}), nil
}

func foldCompoundTypesForUnion(k NomsKind, ts, seenStructs typeset, intersectStructs bool) (*Type, error) {
	elemTypes := make(typeset, len(ts))
	for t := range ts {
		d.PanicIfFalse(t.TargetKind() == k)
		elemTypes.add(t.Desc.(CompoundDesc).ElemTypes[0])
	}

	elemType, err := foldUnionImpl(elemTypes, seenStructs, intersectStructs)

	if err != nil {
		return nil, err
	}

	return makeCompoundType(k, elemType)
}

func foldMapTypesForUnion(ts, seenStructs typeset, intersectStructs bool) (*Type, error) {
	keyTypes := make(typeset, len(ts))
	valTypes := make(typeset, len(ts))
	for t := range ts {
		d.PanicIfFalse(t.TargetKind() == MapKind)
		elemTypes := t.Desc.(CompoundDesc).ElemTypes
		keyTypes.add(elemTypes[0])
		valTypes.add(elemTypes[1])
	}

	kt, err := foldUnionImpl(keyTypes, seenStructs, intersectStructs)

	if err != nil {
		return nil, err
	}

	vt, err := foldUnionImpl(valTypes, seenStructs, intersectStructs)

	if err != nil {
		return nil, err
	}

	return makeCompoundType(MapKind, kt, vt)
}

func foldStructTypesFieldsOnly(name string, ts, seenStructs typeset, intersectStructs bool) (structTypeFields, error) {
	fieldset := make([]structTypeFields, len(ts))
	i := 0
	for t := range ts {
		desc := t.Desc.(StructDesc)
		d.PanicIfFalse(desc.Name == name)
		fieldset[i] = desc.fields
		i++
	}

	return simplifyStructFields(fieldset, seenStructs, intersectStructs)
}

func foldStructTypes(name string, ts, seenStructs typeset, intersectStructs bool) (*Type, error) {
	fields, err := foldStructTypesFieldsOnly(name, ts, seenStructs, intersectStructs)

	if err != nil {
		return nil, err
	}

	return newType(StructDesc{name, fields}), nil
}

func simplifyStructFields(in []structTypeFields, seenStructs typeset, intersectStructs bool) (structTypeFields, error) {
	// We gather all the fields/types into allFields. If the number of
	// times a field name is present is less that then number of types we
	// are simplifying then the field must be optional.
	// If we see an optional field we do not increment the count for it and
	// it will be treated as optional in the end.

	// If intersectStructs is true we need to pick the more restrictive version (n: T over n?: T).
	type fieldTypeInfo struct {
		anyNonOptional bool
		count          int
		ts             typeSlice
	}
	allFields := map[string]fieldTypeInfo{}

	for _, ff := range in {
		for _, f := range ff {
			fti, ok := allFields[f.Name]
			if !ok {
				fti = fieldTypeInfo{
					ts: make(typeSlice, 0, len(in)),
				}
			}
			fti.ts = append(fti.ts, f.Type)
			if !f.Optional {
				fti.count++
				fti.anyNonOptional = true
			}
			allFields[f.Name] = fti
		}
	}

	count := len(in)
	fields := make(structTypeFields, len(allFields))
	i := 0
	for name, fti := range allFields {
		nt, err := makeUnionType(fti.ts...)

		if err != nil {
			return nil, err
		}

		t, err := foldUnions(nt, seenStructs, intersectStructs)

		if err != nil {
			return nil, err
		}

		fields[i] = StructField{
			Name:     name,
			Type:     t,
			Optional: !(intersectStructs && fti.anyNonOptional) && fti.count < count,
		}
		i++
	}

	sort.Sort(fields)

	return fields, nil
}
