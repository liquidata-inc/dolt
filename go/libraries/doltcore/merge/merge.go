package merge

import (
	"errors"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/valutil"
)

var ErrFastForward = errors.New("fast forward")
var ErrSameTblAddedTwice = errors.New("table with same name added in 2 commits can't be merged")
var ErrSchemaNotIdentical = errors.New("schemas not identical and can't be merged")

type Merger struct {
	commit      *doltdb.Commit
	mergeCommit *doltdb.Commit
	ancestor    *doltdb.Commit
	vrw         types.ValueReadWriter
}

func NewMerger(commit, mergeCommit *doltdb.Commit, vrw types.ValueReadWriter) (*Merger, error) {
	ancestor, err := doltdb.GetCommitAnscestor(commit, mergeCommit)

	if err != nil {
		return nil, err
	}

	ff, err := commit.CanFastForwardTo(mergeCommit)
	if err != nil {
		return nil, err
	} else if ff {
		return nil, ErrFastForward
	}
	return &Merger{commit, mergeCommit, ancestor, vrw}, nil
}

func (merger *Merger) MergeTable(tblName string) (*doltdb.Table, *MergeStats, error) {
	root := merger.commit.GetRootValue()
	mergeRoot := merger.mergeCommit.GetRootValue()
	ancRoot := merger.ancestor.GetRootValue()

	tbl, ok := root.GetTable(tblName)
	mergeTbl, mergeOk := mergeRoot.GetTable(tblName)
	ancTbl, ancOk := ancRoot.GetTable(tblName)

	if ok && mergeOk && tbl.HashOf() == mergeTbl.HashOf() {
		return tbl, &MergeStats{Operation: TableUnmodified}, nil
	}

	if !ancOk {
		if mergeOk && ok {
			return nil, nil, ErrSameTblAddedTwice
		} else if ok {
			return tbl, &MergeStats{Operation: TableUnmodified}, nil
		} else {
			return mergeTbl, &MergeStats{Operation: TableAdded}, nil
		}
	}

	tblSchema := tbl.GetSchema()
	mergeTblSchema := mergeTbl.GetSchema()
	schemaUnion, err := typed.TypedSchemaUnion(tblSchema, mergeTblSchema)

	if err != nil {
		return nil, nil, err
	}

	rows := tbl.GetRowData()
	mergeRows := mergeTbl.GetRowData()
	ancRows := ancTbl.GetRowData()

	mergedRowData, conflicts, stats, err := mergeTableData(schemaUnion, rows, mergeRows, ancRows, merger.vrw)

	if err != nil {
		return nil, nil, err
	}

	mergedTable := tbl.UpdateRows(mergedRowData)

	if conflicts.Len() > 0 {
		schemas := doltdb.NewConflict(ancTbl.GetSchemaRef(), tbl.GetSchemaRef(), mergeTbl.GetSchemaRef())
		mergedTable = mergedTable.SetConflicts(schemas, conflicts)
	}

	return mergedTable, stats, nil
}

func stopAndDrain(stop chan<- struct{}, drain <-chan types.ValueChanged) {
	close(stop)
	for range drain {
	}
}

func mergeTableData(sch schema.Schema, rows, mergeRows, ancRows types.Map, vrw types.ValueReadWriter) (types.Map, types.Map, *MergeStats, error) {
	//changeChan1, changeChan2 := make(chan diff.Difference, 32), make(chan diff.Difference, 32)
	changeChan, mergeChangeChan := make(chan types.ValueChanged, 32), make(chan types.ValueChanged, 32)
	stopChan, mergeStopChan := make(chan struct{}, 1), make(chan struct{}, 1)

	go func() {
		//diff.Diff(rows1, ancRows, changeChan1, stopChan1, true, dontDescend)
		rows.Diff(ancRows, changeChan, stopChan)
		close(changeChan)
	}()

	go func() {
		//diff.Diff(rows2, ancRows, changeChan2, stopChan2, true, dontDescend)
		mergeRows.Diff(ancRows, mergeChangeChan, mergeStopChan)
		close(mergeChangeChan)
	}()

	defer stopAndDrain(stopChan, changeChan)
	defer stopAndDrain(mergeStopChan, mergeChangeChan)

	conflictValChan := make(chan types.Value)
	conflictMapChan := types.NewStreamingMap(vrw, conflictValChan)
	mapEditor := rows.Edit()

	stats := &MergeStats{Operation: TableModified}
	var change, mergeChange types.ValueChanged
	for {
		// Get the next change from both a and b. If either diff(a, parent) or diff(b, parent) is complete, aChange or bChange will get an empty types.ValueChanged containing a nil Value. Generally, though, this allows us to proceed through both diffs in (key) order, considering the "current" change from both diffs at the same time.
		if change.Key == nil {
			change = <-changeChan
		}
		if mergeChange.Key == nil {
			mergeChange = <-mergeChangeChan
		}

		key, mergeKey := change.Key, mergeChange.Key

		// Both channels are producing zero values, so we're done.
		if key == nil && mergeKey == nil {
			break
		}

		if key != nil && (mergeKey == nil || key.Less(mergeKey)) {
			// change will already be in the map
			change = types.ValueChanged{}
		} else if mergeKey != nil && (key == nil || mergeKey.Less(key)) {
			applyChange(mapEditor, stats, mergeChange)
			mergeChange = types.ValueChanged{}
		} else {
			r, mergeRow, ancRow := change.NewValue, mergeChange.NewValue, change.OldValue
			mergedRow, isConflict := rowMerge(sch, r, mergeRow, ancRow)

			if isConflict {
				stats.Conflicts++
				conflictTuple := doltdb.NewConflict(ancRow, r, mergeRow).ToNomsList(vrw)
				addConflict(conflictValChan, key, conflictTuple)
			} else {
				applyChange(mapEditor, stats, types.ValueChanged{change.ChangeType, key, r, mergedRow})
			}

			change = types.ValueChanged{}
			mergeChange = types.ValueChanged{}
		}
	}

	close(conflictValChan)
	conflicts := <-conflictMapChan
	mergedData := mapEditor.Map()

	return mergedData, conflicts, stats, nil
}

func addConflict(conflictChan chan types.Value, key types.Value, value types.Tuple) {
	conflictChan <- key
	conflictChan <- value
}

func applyChange(me *types.MapEditor, stats *MergeStats, change types.ValueChanged) {
	switch change.ChangeType {
	case types.DiffChangeAdded:
		stats.Adds++
		me.Set(change.Key, change.NewValue)
	case types.DiffChangeModified:
		stats.Modifications++
		me.Set(change.Key, change.NewValue)
	case types.DiffChangeRemoved:
		stats.Deletes++
		me.Remove(change.Key)
	}
}

func rowMerge(sch schema.Schema, r, mergeRow, baseRow types.Value) (resultRow types.Value, isConflict bool) {
	if baseRow == nil {
		if r.Equals(mergeRow) {
			// same row added to both
			return r, false
		} else {
			// different rows added for the same key
			return nil, true
		}
	} else if r == nil && mergeRow == nil {
		// same row removed from both
		return nil, false
	} else if r == nil || mergeRow == nil {
		// removed from one and modified in another
		return nil, true
	} else {
		rowVals := row.ParseTaggedValues(r.(types.Tuple))
		mergeVals := row.ParseTaggedValues(mergeRow.(types.Tuple))
		baseVals := row.ParseTaggedValues(baseRow.(types.Tuple))

		processTagFunc := func(tag uint64) (resultVal types.Value, isConflict bool) {
			baseVal, _ := baseVals.Get(tag)
			val, _ := rowVals.Get(tag)
			mergeVal, _ := mergeVals.Get(tag)

			if valutil.NilSafeEqCheck(val, mergeVal) {
				return val, false
			} else {
				modified := !valutil.NilSafeEqCheck(val, baseVal)
				mergeModified := !valutil.NilSafeEqCheck(mergeVal, baseVal)
				switch {
				case modified && mergeModified:
					return nil, true
				case modified:
					return val, false
				default:
					return mergeVal, false
				}
			}

		}

		resultVals := make(row.TaggedValues)

		var isConflict bool
		sch.GetNonPKCols().Iter(func(tag uint64, _ schema.Column) (stop bool) {
			var val types.Value
			val, isConflict = processTagFunc(tag)
			resultVals[tag] = val

			return isConflict
		})

		if isConflict {
			return nil, true
		}

		tpl := resultVals.NomsTupleForTags(sch.GetNonPKCols().SortedTags, false)

		return tpl, false
	}
}