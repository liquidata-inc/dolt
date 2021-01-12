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

package diff

import (
	"context"
	"errors"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/types"
)

type (
	diffFunc     func(ctx context.Context, changeChan chan<- types.ValueChanged) error
	pathPartFunc func(v types.Value) (types.PathPart, error)
	valueFunc    func(k types.Value) (types.Value, error)
)

// Difference represents a "diff" between two Noms graphs.
type Difference struct {
	// Path to the Value that has changed
	Path types.Path
	// ChangeType indicates the type of diff: modified, added, deleted
	ChangeType types.DiffChangeType
	// OldValue is Value before the change, can be nil if Value was added
	OldValue types.Value
	// NewValue is Value after the change, can be nil if Value was removed
	NewValue types.Value
	// NewKeyValue is used for when elements are added to diffs with a
	// non-primitive key. The new key must available when the map gets updated.
	NewKeyValue types.Value
	// KeyValue holds the key associated with a changed map value
	KeyValue types.Value
}

func (dif Difference) IsEmpty() bool {
	return dif.Path == nil && dif.OldValue == nil && dif.NewValue == nil
}

type ShouldDescFunc func(v1, v2 types.Value) bool

// differ is used internally to hold information necessary for diffing two graphs.
type differ struct {
	// Channel used to send Difference objects back to caller
	diffChan chan<- Difference
	// Use LeftRight diff as opposed to TopDown
	leftRight bool

	shouldDescend ShouldDescFunc

	eg         *errgroup.Group
	asyncPanic *atomic.Value
}

// Diff traverses two graphs simultaneously looking for differences. It returns
// two channels: a DiffReceiveChan that the caller can use to iterate over the
// diffs in the graph and a StopSendChanel that a caller can use to signal the
// Diff function to stop processing.
// Diff returns the Differences in depth-first first order. A 'diff' is defined
// as one of the following conditions:
//  * a Value is Added or Removed from a node in the graph
//  * the type of a Value has changed in the graph
//  * a primitive (i.e. Bool, Float, String, Ref or Blob) Value has changed.
//
// A Difference is not returned when a non-primitive value has been modified. For
// example, a struct field has been changed from one Value of type Employee to
// another. Those modifications are accounted for by the Differences described
// above at a lower point in the graph.
//
// If leftRight is true then the left-right diff is used for ordered sequences
// - see Diff vs DiffLeftRight in Set and Map.
//
// Note: the function sends messages on diffChan and checks whether stopChan has
// been closed to know if it needs to terminate diffing early. To function
// properly it needs to be executed concurrently with code that reads values from
// diffChan. The following is a typical invocation of Diff():
//    dChan := make(chan Difference)
//    sChan := make(chan struct{})
//    go func() {
//        d.Diff(s3, s4, dChan, sChan, leftRight)
//        close(dChan)
//    }()
//    for dif := range dChan {
//        <some code>
//    }
func Diff(ctx context.Context, v1, v2 types.Value, dChan chan<- Difference, leftRight bool, descFunc ShouldDescFunc) error {
	if descFunc == nil {
		descFunc = ShouldDescend
	}

	eg, ctx := errgroup.WithContext(ctx)
	d := differ{
		diffChan:      dChan,
		leftRight:     leftRight,
		shouldDescend: descFunc,

		eg:         eg,
		asyncPanic: new(atomic.Value),
	}
	if !v1.Equals(v2) {
		if !d.shouldDescend(v1, v2) {
			return d.sendDiff(ctx, Difference{Path: nil, ChangeType: types.DiffChangeModified, OldValue: v1, NewValue: v2})
		} else {
			d.GoCatchPanic(func() error {
				return d.diff(ctx, nil, v1, v2)
			})
			return d.Wait()
		}
	}
	return nil
}

func (d differ) diff(ctx context.Context, p types.Path, v1, v2 types.Value) error {
	switch v1.Kind() {
	case types.ListKind:
		return d.diffLists(ctx, p, v1.(types.List), v2.(types.List))
	case types.MapKind:
		return d.diffMaps(ctx, p, v1.(types.Map), v2.(types.Map))
	case types.SetKind:
		return d.diffSets(ctx, p, v1.(types.Set), v2.(types.Set))
	case types.StructKind:
		return d.diffStructs(ctx, p, v1.(types.Struct), v2.(types.Struct))
	default:
		panic("Unrecognized type in diff function")
	}
}

var AsyncPanicErr = errors.New("async panic")

func (d differ) GoCatchPanic(f func() error) {
	d.eg.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				d.asyncPanic.Store(r)
				err = AsyncPanicErr
			}
		}()
		return f()
	})
}

func (d differ) Wait() error {
	err := d.eg.Wait()
	if p := d.asyncPanic.Load(); p != nil {
		panic(p)
	}
	return err
}

func (d differ) diffLists(ctx context.Context, p types.Path, v1, v2 types.List) error {
	spliceChan := make(chan types.Splice)

	d.GoCatchPanic(func() error {
		defer close(spliceChan)
		return v2.Diff(ctx, v1, spliceChan)
	})

	for splice := range spliceChan {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if splice.SpRemoved == splice.SpAdded {
			// Heuristic: list only has modifications.
			for i := uint64(0); i < splice.SpRemoved; i++ {
				lastEl, err := v1.Get(ctx, splice.SpAt+i)
				if err != nil {
					return err
				}

				newEl, err := v2.Get(ctx, splice.SpFrom+i)
				if err != nil {
					return err
				}

				if d.shouldDescend(lastEl, newEl) {
					idx := types.Float(splice.SpAt + i)
					err := d.diff(ctx, append(p, types.NewIndexPath(idx)), lastEl, newEl)
					if err != nil {
						return err
					}
				} else {
					p1 := p.Append(types.NewIndexPath(types.Float(splice.SpAt + i)))
					oldVal, err := v1.Get(ctx, splice.SpAt+i)
					if err != nil {
						return err
					}

					newVal, err := v2.Get(ctx, splice.SpFrom+i)
					if err != nil {
						return err
					}

					dif := Difference{Path: p1, ChangeType: types.DiffChangeModified, OldValue: oldVal, NewValue: newVal}
					err = d.sendDiff(ctx, dif)
					if err != nil {
						return err
					}
				}
			}
			continue
		}

		// Heuristic: list only has additions/removals.
		for i := uint64(0); i < splice.SpRemoved; i++ {
			p1 := p.Append(types.NewIndexPath(types.Float(splice.SpAt + i)))
			oldVal, err := v1.Get(ctx, splice.SpAt+i)
			if err != nil {
				return err
			}

			dif := Difference{Path: p1, ChangeType: types.DiffChangeRemoved, OldValue: oldVal, NewValue: nil}
			err = d.sendDiff(ctx, dif)
			if err != nil {
				return err
			}
		}
		for i := uint64(0); i < splice.SpAdded; i++ {
			p1 := p.Append(types.NewIndexPath(types.Float(splice.SpFrom + i)))
			newVal, err := v2.Get(ctx, splice.SpFrom+i)
			if err != nil {
				return err
			}

			dif := Difference{Path: p1, ChangeType: types.DiffChangeAdded, OldValue: nil, NewValue: newVal}
			err = d.sendDiff(ctx, dif)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (d differ) diffMaps(ctx context.Context, p types.Path, v1, v2 types.Map) error {
	return d.diffOrdered(ctx, p,
		func(v types.Value) (types.PathPart, error) {
			if types.ValueCanBePathIndex(v) {
				return types.NewIndexPath(v), nil
			} else {
				h, err := v.Hash(v1.Format())

				if err != nil {
					return nil, err
				}

				return types.NewHashIndexPath(h), nil
			}
		},
		func(ctx context.Context, cc chan<- types.ValueChanged) error {
			if d.leftRight {
				return v2.DiffLeftRight(ctx, v1, cc)
			} else {
				return v2.DiffHybrid(ctx, v1, cc)
			}
		},
		func(k types.Value) (types.Value, error) {
			return k, nil
		},
		func(k types.Value) (types.Value, error) {
			v, _, err := v1.MaybeGet(ctx, k)
			return v, err
		},
		func(k types.Value) (types.Value, error) {
			v, _, err := v2.MaybeGet(ctx, k)
			return v, err
		},
	)
}

func (d differ) diffStructs(ctx context.Context, p types.Path, v1, v2 types.Struct) error {
	str := func(v types.Value) string {
		return string(v.(types.String))
	}
	return d.diffOrdered(ctx, p,
		func(v types.Value) (types.PathPart, error) {
			return types.NewFieldPath(str(v)), nil
		},
		func(ctx context.Context, cc chan<- types.ValueChanged) error {
			return v2.Diff(ctx, v1, cc)
		},
		func(k types.Value) (types.Value, error) { return k, nil },
		func(k types.Value) (types.Value, error) {
			val, _, err := v1.MaybeGet(str(k))
			return val, err
		},
		func(k types.Value) (types.Value, error) {
			val, _, err := v2.MaybeGet(str(k))
			return val, err
		},
	)
}

func (d differ) diffSets(ctx context.Context, p types.Path, v1, v2 types.Set) error {
	return d.diffOrdered(ctx, p,
		func(v types.Value) (types.PathPart, error) {
			if types.ValueCanBePathIndex(v) {
				return types.NewIndexPath(v), nil
			}

			h, err := v.Hash(v1.Format())

			if err != nil {
				return nil, err
			}

			return types.NewHashIndexPath(h), nil
		},
		func(ctx context.Context, cc chan<- types.ValueChanged) error {
			if d.leftRight {
				return v2.DiffLeftRight(ctx, v1, cc)
			} else {
				return v2.DiffHybrid(ctx, v1, cc)
			}
		},
		func(k types.Value) (types.Value, error) { return k, nil },
		func(k types.Value) (types.Value, error) { return k, nil },
		func(k types.Value) (types.Value, error) { return k, nil },
	)
}

func (d differ) diffOrdered(ctx context.Context, p types.Path, ppf pathPartFunc, df diffFunc, kf, v1, v2 valueFunc) error {
	changeChan := make(chan types.ValueChanged)

	d.GoCatchPanic(func() error {
		defer close(changeChan)
		return df(ctx, changeChan)
	})

	for change := range changeChan {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		k, err := kf(change.Key)
		if err != nil {
			return err
		}

		ppfRes, err := ppf(k)
		if err != nil {
			return err
		}

		p1 := p.Append(ppfRes)

		switch change.ChangeType {
		case types.DiffChangeAdded:
			newVal, err := v2(change.Key)
			if err != nil {
				return err
			}

			dif := Difference{Path: p1, ChangeType: types.DiffChangeAdded, OldValue: nil, NewValue: newVal, NewKeyValue: k, KeyValue: change.Key}
			err = d.sendDiff(ctx, dif)
			if err != nil {
				return err
			}
		case types.DiffChangeRemoved:
			oldVal, err := v1(change.Key)
			if err != nil {
				return err
			}

			dif := Difference{Path: p1, ChangeType: types.DiffChangeRemoved, OldValue: oldVal, KeyValue: change.Key}
			err = d.sendDiff(ctx, dif)
			if err != nil {
				return err
			}
		case types.DiffChangeModified:
			c1, err := v1(change.Key)
			if err != nil {
				return err
			}

			c2, err := v2(change.Key)
			if err != nil {
				return err
			}

			if d.shouldDescend(c1, c2) {
				err = d.diff(ctx, p1, c1, c2)
				if err != nil {
					return err
				}
			} else {
				dif := Difference{Path: p1, ChangeType: types.DiffChangeModified, OldValue: c1, NewValue: c2, KeyValue: change.Key}
				err = d.sendDiff(ctx, dif)
				if err != nil {
					return err
				}
			}
		default:
			panic("unknown change type")
		}
	}

	return nil
}

// shouldDescend returns true, if Value is not primitive or is a Ref.
func ShouldDescend(v1, v2 types.Value) bool {
	kind := v1.Kind()
	return !types.IsPrimitiveKind(kind) && kind == v2.Kind() && kind != types.RefKind && kind != types.TupleKind && kind != types.ChunkedStringKind
}

func (d differ) sendDiff(ctx context.Context, dif Difference) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case d.diffChan <- dif:
		return nil
	}
}
