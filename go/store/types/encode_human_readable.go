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
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/dustin/go-humanize"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/util/writers"
)

// Clients can register a 'commenter' to return a comment that will get appended
// to the first line of encoded values. For example, the noms DateTime struct
// normally gets encoded as follows:
//    lastRefresh: DateTime {
//      secSinceEpoch: 1.501801626877e+09,
//    }
//
// By registering a commenter that returns a nicely formatted date,
// the struct will be coded with a comment:
//    lastRefresh: DateTime { // 2017-08-03T16:07:06-07:00
//      secSinceEpoch: 1.501801626877e+09,
//    }

// Function type for commenter functions
type HRSCommenter interface {
	Comment(context.Context, Value) string
}

var (
	commenterRegistry = map[string]map[string]HRSCommenter{}
	registryLock      sync.RWMutex
)

// RegisterHRSCommenter is called to with three arguments:
//  typename: the name of the struct this function will be applied to
//  unique: an arbitrary string to differentiate functions that should be applied
//    to different structs that have the same name (e.g. two implementations of
//    the "Employee" type.
//  commenter: an interface with a 'Comment()' function that gets called for all
//    Values with this name. The function should verify the type of the Value
//    and, if appropriate, return a non-empty string to be appended as the comment
func RegisterHRSCommenter(typename, unique string, commenter HRSCommenter) {
	registryLock.Lock()
	defer registryLock.Unlock()
	commenters := commenterRegistry[typename]
	if commenters == nil {
		commenters = map[string]HRSCommenter{}
		commenterRegistry[typename] = commenters
	}
	commenters[unique] = commenter
}

// UnregisterHRSCommenter will remove a commenter function for a specified
// typename/unique string combination.
func UnregisterHRSCommenter(typename, unique string) {
	registryLock.Lock()
	defer registryLock.Unlock()
	r := commenterRegistry[typename]
	if r == nil {
		return
	}
	delete(r, unique)
}

// GetHRSCommenters the map of 'unique' strings to HRSCommentFunc for
// a specified typename.
func GetHRSCommenters(typename string) []HRSCommenter {
	registryLock.RLock()
	defer registryLock.RUnlock()
	// need to copy this value so we can release the lock
	commenters := []HRSCommenter{}
	for _, f := range commenterRegistry[typename] {
		commenters = append(commenters, f)
	}
	return commenters
}

// Human Readable Serialization
type hrsWriter struct {
	ind         int
	w           io.Writer
	lineLength  int
	floatFormat byte
	err         error
}

func (w *hrsWriter) maybeWriteIndentation() {
	if w.lineLength == 0 {
		for i := 0; i < w.ind && w.err == nil; i++ {
			_, w.err = io.WriteString(w.w, "  ")
		}
		w.lineLength = 2 * w.ind
	}
}

func (w *hrsWriter) write(s string) {
	if w.err != nil {
		return
	}
	w.maybeWriteIndentation()
	var n int
	n, w.err = io.WriteString(w.w, s)
	w.lineLength += n
}

func (w *hrsWriter) indent() {
	w.ind++
}

func (w *hrsWriter) outdent() {
	w.ind--
}

func (w *hrsWriter) newLine() {
	w.write("\n")
	w.lineLength = 0
}

// hexWriter is used to write blob byte data as "00 01 ... 0f\n10 11 .."
// hexWriter is an io.Writer that writes to an underlying hrsWriter.
type hexWriter struct {
	hrs         *hrsWriter
	count       uint
	sizeWritten bool
	size        uint64
}

func (w *hexWriter) Write(p []byte) (n int, err error) {
	for _, v := range p {
		if !w.sizeWritten && len(p) > 16 {
			w.hrs.write("  // ")
			w.hrs.write(humanize.Bytes(w.size))
			w.sizeWritten = true
			w.hrs.indent()
			w.hrs.newLine()
		}

		if w.count == 16 {
			w.hrs.newLine()
			w.count = 0
		} else if w.count != 0 {
			w.hrs.write(" ")
		}
		if v < 0x10 {
			w.hrs.write("0")
		}
		w.hrs.write(strconv.FormatUint(uint64(v), 16))
		if w.hrs.err != nil {
			err = w.hrs.err
			return
		}
		n++
		w.count++
	}

	if w.sizeWritten {
		w.hrs.outdent()
		w.hrs.newLine()
	}

	return
}

func (w *hrsWriter) Write(ctx context.Context, v Value) error {
	if v == nil {
		w.write("nil")
		return nil
	}

	switch v.Kind() {
	case FloatKind: // We're special-casing floats since it requires floatFormat & most other kinds don't need anything special
		w.write(strconv.FormatFloat(float64(v.(Float)), w.floatFormat, -1, 64))

	case BlobKind:
		w.write("blob {")
		blob := v.(Blob)
		encoder := &hexWriter{hrs: w, size: blob.Len()}
		_, w.err = io.Copy(encoder, blob.Reader(ctx))
		w.write("}")

	case ListKind:
		w.write("[")
		w.writeSize(v)
		w.indent()

		var err error
		iterErr := v.(List).Iter(ctx, func(v Value, i uint64) bool {
			if i == 0 {
				w.newLine()
			}

			err = w.Write(ctx, v)

			if err != nil {
				return true
			}

			w.write(",")
			w.newLine()
			err = w.err

			return err != nil
		})

		if err != nil {
			return err
		}

		if iterErr != nil {
			return iterErr
		}

		w.outdent()
		w.write("]")

	case ChunkedStringKind:
		s, err := v.(ChunkedString).ReadString(ctx, 64)
		if err != nil {
			return err
		}
		w.write(strconv.Quote(string(s)))

	case TupleKind:
		w.write("(")
		err := v.(Tuple).IterFields(func(i uint64, v Value) (bool, error) {
			if i != 0 {
				w.write(",")
			}

			err := w.Write(ctx, v)

			if err != nil {
				return false, err
			}

			if w.err != nil {
				return false, w.err
			}

			return false, nil
		})

		if err != nil {
			return err
		}

		w.outdent()
		w.write(")")

	case MapKind:
		w.write("map {")
		w.writeSize(v)
		w.indent()
		if !v.(Map).Empty() {
			w.newLine()
		}

		err := v.(Map).Iter(ctx, func(key, val Value) (bool, error) {
			err := w.Write(ctx, key)

			if err != nil {
				return false, err
			}

			w.write(": ")
			err = w.Write(ctx, val)

			if err != nil {
				return false, err
			}

			w.write(",")
			w.newLine()

			if w.err != nil {
				return false, w.err
			}

			return false, nil
		})

		if err != nil {
			return err
		}

		w.outdent()
		w.write("}")

	case RefKind:
		w.write("#")
		w.write(v.(Ref).TargetHash().String())

	case SetKind:
		w.write("set {")
		w.writeSize(v)
		w.indent()
		if !v.(Set).Empty() {
			w.newLine()
		}

		err := v.(Set).Iter(ctx, func(v Value) (bool, error) {
			err := w.Write(ctx, v)

			if err != nil {
				return false, err
			}

			w.write(",")
			w.newLine()

			if w.err != nil {
				return false, err
			}

			return false, nil
		})

		if err != nil {
			return err
		}

		w.outdent()
		w.write("}")

	case TypeKind:
		w.writeType(v.(*Type), map[*Type]struct{}{})

	case StructKind:
		err := w.writeStruct(ctx, v.(Struct))

		if err != nil {
			return err
		}

	default:
		if IsPrimitiveKind(v.Kind()) {
			w.write(v.HumanReadableString())
			return nil
		}
		return ErrUnknownType
	}

	return nil
}

type hrsStructWriter struct {
	*hrsWriter
	v Struct
}

func (w hrsStructWriter) name(ctx context.Context, n string) {
	w.write("struct ")
	if n != "" {
		w.write(n)
		w.write(" ")
	}
	w.write("{")
	commenters := GetHRSCommenters(n)
	for _, commenter := range commenters {
		if comment := commenter.Comment(ctx, w.v); comment != "" {
			w.write(" // " + comment)
			break
		}

	}
	w.indent()
}

func (w hrsStructWriter) count(c uint64) {
	if c > 0 {
		w.newLine()
	}
}

func (w hrsStructWriter) fieldName(n string) {
	w.write(n)
	w.write(": ")
}

func (w hrsStructWriter) fieldValue(ctx context.Context, v Value) error {
	err := w.Write(ctx, v)

	if err != nil {
		return err
	}

	w.write(",")
	w.newLine()

	return nil
}

func (w hrsStructWriter) end() {
	w.outdent()
	w.write("}")
}

func (w *hrsWriter) writeStruct(ctx context.Context, v Struct) error {
	return v.iterParts(ctx, hrsStructWriter{w, v})
}

func (w *hrsWriter) writeSize(v Value) {
	switch v.Kind() {
	case ListKind, MapKind, SetKind:
		l := v.(Collection).Len()
		if l < 4 {
			return
		}
		w.write(fmt.Sprintf("  // %s items", humanize.Comma(int64(l))))
	default:
		panic("unreachable")
	}
}

func (w *hrsWriter) writeType(t *Type, seenStructs map[*Type]struct{}) {
	switch t.TargetKind() {
	case ListKind, RefKind, SetKind, MapKind, TupleKind:
		w.write(t.TargetKind().String())
		w.write("<")
		for i, et := range t.Desc.(CompoundDesc).ElemTypes {
			if et.TargetKind() == UnionKind && len(et.Desc.(CompoundDesc).ElemTypes) == 0 {
				// If one of the element types is an empty union all the other element types must
				// also be empty union types.
				break
			}
			if i != 0 {
				w.write(", ")
			}
			w.writeType(et, seenStructs)
			if w.err != nil {
				break
			}
		}
		w.write(">")
	case UnionKind:
		for i, et := range t.Desc.(CompoundDesc).ElemTypes {
			if i != 0 {
				w.write(" | ")
			}
			w.writeType(et, seenStructs)
			if w.err != nil {
				break
			}
		}
	case StructKind:
		w.writeStructType(t, seenStructs)
	case CycleKind:
		name := string(t.Desc.(CycleDesc))
		d.PanicIfTrue(name == "")

		// This can happen for types that have unresolved cyclic refs
		w.write(fmt.Sprintf("UnresolvedCycle<%s>", name))
		if w.err != nil {
			return
		}
	default:
		if IsPrimitiveKind(t.TargetKind()) {
			w.write(t.TargetKind().String())
		} else {
			panic("unreachable")
		}
	}
}

func (w *hrsWriter) writeStructType(t *Type, seenStructs map[*Type]struct{}) {
	name := t.Desc.(StructDesc).Name
	if _, ok := seenStructs[t]; ok {
		w.write(fmt.Sprintf("Cycle<%s>", name))
		return
	}
	seenStructs[t] = struct{}{}

	desc := t.Desc.(StructDesc)
	w.write("Struct ")
	if desc.Name != "" {
		w.write(desc.Name + " ")
	}
	w.write("{")
	w.indent()
	if desc.Len() > 0 {
		w.newLine()
	}
	desc.IterFields(func(name string, t *Type, optional bool) {
		w.write(name)
		if optional {
			w.write("?")
		}
		w.write(": ")
		w.writeType(t, seenStructs)
		w.write(",")
		w.newLine()
	})
	w.outdent()
	w.write("}")
}

func encodedValueFormatMaxLines(ctx context.Context, v Value, floatFormat byte, maxLines uint32) (string, error) {
	var buf bytes.Buffer
	mlw := &writers.MaxLineWriter{Dest: &buf, MaxLines: maxLines}
	w := &hrsWriter{w: mlw, floatFormat: floatFormat}
	err := w.Write(ctx, v)

	if err == writers.MaxLinesErr {
		return buf.String(), writers.MaxLinesErr
	}

	if err != nil && err != writers.MaxLinesErr {
		return "", err
	}

	if w.err != nil {
		return "", w.err
	}

	return buf.String(), nil
}

func encodedValueFormat(ctx context.Context, v Value, floatFormat byte) (string, error) {
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf, floatFormat: floatFormat}
	err := w.Write(ctx, v)

	if err != nil {
		return "", err
	}

	if w.err != nil {
		return "", w.err
	}

	return buf.String(), nil
}

func EncodedIndexValue(ctx context.Context, v Value) (string, error) {
	return encodedValueFormat(ctx, v, 'f')
}

// EncodedValue returns a string containing the serialization of a value.
func EncodedValue(ctx context.Context, v Value) (string, error) {
	return encodedValueFormat(ctx, v, 'g')
}

// EncodedValueMaxLines returns a string containing the serialization of a value.
// The string is truncated at |maxLines|.
func EncodedValueMaxLines(ctx context.Context, v Value, maxLines uint32) (string, error) {
	return encodedValueFormatMaxLines(ctx, v, 'g', maxLines)
}

// WriteEncodedValue writes the serialization of a value
func WriteEncodedValue(ctx context.Context, w io.Writer, v Value) error {
	hrs := &hrsWriter{w: w, floatFormat: 'g'}
	err := hrs.Write(ctx, v)

	if err != nil {
		return err
	}

	return hrs.err
}

// WriteEncodedValueMaxLines writes the serialization of a value. Writing will be
// stopped and an error returned after |maxLines|.
func WriteEncodedValueMaxLines(ctx context.Context, w io.Writer, v Value, maxLines uint32) error {
	mlw := &writers.MaxLineWriter{Dest: w, MaxLines: maxLines}
	hrs := &hrsWriter{w: mlw, floatFormat: 'g'}
	err := hrs.Write(ctx, v)

	if err != nil {
		return err
	}

	return hrs.err
}
