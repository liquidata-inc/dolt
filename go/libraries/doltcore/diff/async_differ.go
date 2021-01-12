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

package diff

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/store/diff"
	"github.com/dolthub/dolt/go/store/types"
)

type RowDiffer interface {
	// Start starts the RowDiffer.
	Start(ctx context.Context, from, to types.Map)

	// GetDiffs returns the requested number of diff.Differences, or times out.
	GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error)

	// Close closes the RowDiffer.
	Close() error
}

func NewRowDiffer(ctx context.Context, fromSch, toSch schema.Schema, buf int) RowDiffer {
	ad := NewAsyncDiffer(buf)

	// assumes no PK changes
	// mixed diffing of keyless and pk tables no supported
	if schema.IsKeyless(fromSch) || schema.IsKeyless(toSch) {
		return &keylessDiffer{AsyncDiffer: ad}
	}

	return ad
}

// todo: make package private
type AsyncDiffer struct {
	diffChan   chan diff.Difference
	bufferSize int

	eg       *errgroup.Group
	egCtx    context.Context
	egCancel func()
}

var _ RowDiffer = &AsyncDiffer{}

// todo: make package private once dolthub is migrated
func NewAsyncDiffer(bufferedDiffs int) *AsyncDiffer {
	return &AsyncDiffer{
		make(chan diff.Difference, bufferedDiffs),
		bufferedDiffs,
		nil,
		context.Background(),
		func() {},
	}
}

func tableDontDescendLists(v1, v2 types.Value) bool {
	kind := v1.Kind()
	return !types.IsPrimitiveKind(kind) && kind != types.TupleKind && kind != types.ChunkedStringKind && kind == v2.Kind() && kind != types.RefKind
}

func (ad *AsyncDiffer) Start(ctx context.Context, from, to types.Map) {
	ad.eg, ad.egCtx = errgroup.WithContext(ctx)
	ad.egCancel = async.GoWithCancel(ad.egCtx, ad.eg, func(ctx context.Context) (err error) {
		defer close(ad.diffChan)
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in diff.Diff: %v", r)
			}
		}()
		return diff.Diff(ctx, from, to, ad.diffChan, true, tableDontDescendLists)
	})
}

func (ad *AsyncDiffer) Close() error {
	ad.egCancel()
	return ad.eg.Wait()
}

func (ad *AsyncDiffer) GetDiffs(numDiffs int, timeout time.Duration) ([]*diff.Difference, bool, error) {
	diffs := make([]*diff.Difference, 0, ad.bufferSize)
	timeoutChan := time.After(timeout)
	for {
		select {
		case d, more := <-ad.diffChan:
			if more {
				diffs = append(diffs, &d)
				if numDiffs != 0 && numDiffs == len(diffs) {
					return diffs, true, nil
				}
			} else {
				return diffs, false, ad.eg.Wait()
			}
		case <-timeoutChan:
			return diffs, true, nil
		case <-ad.egCtx.Done():
			return nil, false, ad.eg.Wait()
		}
	}
}

type keylessDiffer struct {
	*AsyncDiffer

	df         diff.Difference
	copiesLeft uint64
}

var _ RowDiffer = &keylessDiffer{}

func (kd *keylessDiffer) GetDiffs(numDiffs int, timeout time.Duration) (diffs []*diff.Difference, more bool, err error) {
	timeoutChan := time.After(timeout)
	diffs = make([]*diff.Difference, numDiffs)
	idx := 0

	for {
		// first populate |diffs| with copies of |kd.df|
		for (idx < numDiffs) && (kd.copiesLeft > 0) {
			diffs[idx] = &kd.df

			idx++
			kd.copiesLeft--
		}
		if idx == numDiffs {
			return diffs, true, nil
		}

		// then get another Difference
		var d diff.Difference
		select {
		case <-timeoutChan:
			return diffs, true, nil

		case <-kd.egCtx.Done():
			return nil, false, kd.eg.Wait()

		case d, more = <-kd.diffChan:
			if !more {
				return diffs[:idx], more, nil
			}

			kd.df, kd.copiesLeft, err = convertDiff(d)
			if err != nil {
				return nil, false, err
			}
		}
	}

}

// convertDiff reports the cardinality of a change,
// and converts updates to adds or deletes
func convertDiff(df diff.Difference) (diff.Difference, uint64, error) {
	var oldCard uint64
	if df.OldValue != nil {
		v, err := df.OldValue.(types.Tuple).Get(row.KeylessCardinalityValIdx)
		if err != nil {
			return df, 0, err
		}
		oldCard = uint64(v.(types.Uint))
	}

	var newCard uint64
	if df.NewValue != nil {
		v, err := df.NewValue.(types.Tuple).Get(row.KeylessCardinalityValIdx)
		if err != nil {
			return df, 0, err
		}
		newCard = uint64(v.(types.Uint))
	}

	switch df.ChangeType {
	case types.DiffChangeRemoved:
		return df, oldCard, nil

	case types.DiffChangeAdded:
		return df, newCard, nil

	case types.DiffChangeModified:
		delta := int64(newCard) - int64(oldCard)
		if delta > 0 {
			df.ChangeType = types.DiffChangeAdded
			df.OldValue = nil
			return df, uint64(delta), nil
		} else if delta < 0 {
			df.ChangeType = types.DiffChangeRemoved
			df.NewValue = nil
			return df, uint64(-delta), nil
		} else {
			panic(fmt.Sprintf("diff with delta = 0 for key: %s", df.KeyValue.HumanReadableString()))
		}
	default:
		return df, 0, fmt.Errorf("unexpected DiffChange type %d", df.ChangeType)
	}
}
