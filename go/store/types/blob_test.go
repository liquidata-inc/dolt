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
	"io"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"

	"github.com/dolthub/dolt/go/store/d"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func randomBuff(length uint) []byte {
	rr := rand.New(rand.NewSource(int64(length)))
	buff := make([]byte, length)
	rr.Read(buff)
	return buff
}

type blobTestSuite struct {
	collectionTestSuite
	buff []byte
}

func newBlobTestSuite(size uint, expectChunkCount int, expectPrependChunkDiff int, expectAppendChunkDiff int) (*blobTestSuite, error) {
	vrw := newTestValueStore()

	length := 1 << size
	buff := randomBuff(size)
	blob, err := NewBlob(context.Background(), vrw, bytes.NewReader(buff))

	if err != nil {
		return nil, err
	}

	return &blobTestSuite{
		collectionTestSuite: collectionTestSuite{
			col:                    blob,
			expectType:             PrimitiveTypeMap[BlobKind],
			expectLen:              uint64(length),
			expectChunkCount:       expectChunkCount,
			expectPrependChunkDiff: expectPrependChunkDiff,
			expectAppendChunkDiff:  expectAppendChunkDiff,
			validate: func(v2 Collection) bool {
				b2 := v2.(Blob)
				outBuff := &bytes.Buffer{}
				n, err := b2.Copy(context.Background(), outBuff)
				if int(n) != outBuff.Len() {
					d.PanicIfError(err)
				}

				return bytes.Equal(outBuff.Bytes(), buff)
			},
			prependOne: func() (Collection, error) {
				dup := make([]byte, length+1)
				dup[0] = 0
				copy(dup[1:], buff)
				return NewBlob(context.Background(), vrw, bytes.NewReader(dup))
			},
			appendOne: func() (Collection, error) {
				dup := make([]byte, length+1)
				copy(dup, buff)
				dup[len(dup)-1] = 0
				return NewBlob(context.Background(), vrw, bytes.NewReader(dup))
			},
		},
		buff: buff,
	}, nil
}

func TestBlobSuite4K(t *testing.T) {
	s, err := newBlobTestSuite(12, 2, 2, 2)
	assert.NoError(t, err)
	suite.Run(t, s)
}

func TestBlobSuite64K(t *testing.T) {
	s, err := newBlobTestSuite(16, 15, 2, 2)
	assert.NoError(t, err)
	suite.Run(t, s)
}

func TestBlobSuite256K(t *testing.T) {
	s, err := newBlobTestSuite(18, 64, 2, 2)
	assert.NoError(t, err)
	suite.Run(t, s)
}

func TestBlobSuite1M(t *testing.T) {
	s, err := newBlobTestSuite(20, 245, 2, 2)
	assert.NoError(t, err)
	suite.Run(t, s)
}

// Checks the first 1/2 of the bytes, then 1/2 of the remainder, then 1/2 of the remainder, etc...
func (suite *blobTestSuite) TestRandomRead() {
	buffReader := bytes.NewReader(suite.buff)
	blobReader := suite.col.(Blob).Reader(context.Background())

	readByteRange := func(r io.ReadSeeker, start, rel, count int64) []byte {
		bytes := make([]byte, count)
		n, err := r.Seek(start, 0)
		suite.NoError(err)
		suite.Equal(start, n)
		n2, err := r.Seek(rel, 1)
		suite.NoError(err)
		suite.Equal(start+rel, n2)
		n3, err := io.ReadFull(r, bytes)
		suite.NoError(err)
		suite.Equal(int(count), n3)
		return bytes
	}

	readByteRangeFromEnd := func(r io.ReadSeeker, length, offset, count int64) []byte {
		bytes := make([]byte, count)
		n, err := r.Seek(offset, 2)
		suite.NoError(err)
		suite.Equal(length+offset, n)
		n2, err := io.ReadFull(r, bytes)
		suite.NoError(err)
		suite.Equal(int(count), n2)
		return bytes
	}

	checkByteRange := func(start, rel, count int64) {
		expect := readByteRange(buffReader, start, rel, count)
		actual := readByteRange(blobReader, start, rel, count)
		suite.Equal(expect, actual)
	}

	checkByteRangeFromEnd := func(length, offset, count int64) {
		expect := readByteRangeFromEnd(buffReader, length, offset, count)
		actual := readByteRangeFromEnd(blobReader, length, offset, count)
		suite.Equal(expect, actual)
	}

	length := int64(len(suite.buff))
	start := int64(0)
	count := int64(length / 2)
	for count > 2 {
		checkByteRange(start, 0, count)
		checkByteRange(0, start, count)
		checkByteRange(start/2, start-(start/2), count)
		checkByteRangeFromEnd(length, start-length, count)
		start = start + count
		count = (length - start) / 2
	}
}

type testReader struct {
	readCount int
	buf       *bytes.Buffer
}

func (r *testReader) Read(p []byte) (n int, err error) {
	r.readCount++

	switch r.readCount {
	case 1:
		for i := 0; i < len(p); i++ {
			p[i] = 0x01
		}

		_, err = io.Copy(r.buf, bytes.NewReader(p))

		if err != nil {
			return 0, err
		}

		return len(p), nil
	case 2:
		p[0] = 0x02
		r.buf.WriteByte(p[0])
		return 1, io.EOF
	default:
		return 0, io.EOF
	}
}

func TestBlobFromReaderThatReturnsDataAndError(t *testing.T) {
	// See issue #264.
	// This tests the case of building a Blob from a reader who returns both data and an error for the final Read() call.
	assert := assert.New(t)
	vrw := newTestValueStore()
	tr := &testReader{buf: &bytes.Buffer{}}

	b, err := NewBlob(context.Background(), vrw, tr)
	assert.NoError(err)

	actual := &bytes.Buffer{}
	_, err = io.Copy(actual, b.Reader(context.Background()))
	assert.NoError(err)

	assert.True(bytes.Equal(actual.Bytes(), tr.buf.Bytes()))
	assert.Equal(byte(2), actual.Bytes()[len(actual.Bytes())-1])
}

func TestBlobConcat(t *testing.T) {
	assert := assert.New(t)

	smallTestChunks()
	defer normalProductionChunks()

	vs := newTestValueStore()
	reload := func(b Blob) Blob {
		return mustValue(vs.ReadValue(context.Background(), mustRef(vs.WriteValue(context.Background(), b)).TargetHash())).(Blob)
	}

	split := func(b Blob, at int64) (Blob, Blob) {
		read1, read2 := b.Reader(context.Background()), b.Reader(context.Background())
		b1, err := NewBlob(context.Background(), vs, &io.LimitedReader{R: read1, N: at})
		assert.NoError(err)
		_, err = read2.Seek(at, 0)
		assert.NoError(err)
		b2, err := NewBlob(context.Background(), vs, read2)
		assert.NoError(err)
		return reload(b1), reload(b2)
	}

	// Random 1MB Blob.
	// Note that List.Concat is exhaustively tested, don't worry here.
	r := rand.New(rand.NewSource(0))
	b, err := NewBlob(context.Background(), vs, &io.LimitedReader{R: r, N: 1e6})
	assert.NoError(err)
	b = reload(b)

	b1, err := NewEmptyBlob(vs)
	assert.NoError(err)
	b1, err = b1.Concat(context.Background(), b)
	assert.NoError(err)
	assert.True(b.Equals(b1))

	b2, err := b.Concat(context.Background(), mustBlob(NewEmptyBlob(vs)))
	assert.NoError(err)
	assert.True(b.Equals(b2))

	b3, b4 := split(b, 10)
	b34, err := b3.Concat(context.Background(), b4)
	assert.NoError(err)
	assert.True(b.Equals(b34))

	b5, b6 := split(b, 1e6-10)
	b56, err := b5.Concat(context.Background(), b6)
	assert.True(b.Equals(b56))
	assert.NoError(err)

	b7, b8 := split(b, 1e6/2)
	b78, err := b7.Concat(context.Background(), b8)
	assert.True(b.Equals(b78))
}

func TestBlobNewParallel(t *testing.T) {
	assert := assert.New(t)
	vrw := newTestValueStore()

	readAll := func(b Blob) []byte {
		data, err := ioutil.ReadAll(b.Reader(context.Background()))
		assert.NoError(err)
		return data
	}

	b, err := NewBlob(context.Background(), vrw)
	assert.NoError(err)
	assert.True(b.Len() == 0)

	b, err = NewBlob(context.Background(), vrw, strings.NewReader("abc"))
	assert.NoError(err)
	assert.Equal("abc", string(readAll(b)))

	b, err = NewBlob(context.Background(), vrw, strings.NewReader("abc"), strings.NewReader("def"))
	assert.NoError(err)
	assert.Equal("abcdef", string(readAll(b)))

	p, size := 100, 1024
	r := rand.New(rand.NewSource(0))
	data := make([]byte, p*size)
	_, err = r.Read(data)
	assert.NoError(err)

	readers := make([]io.Reader, p)
	for i := range readers {
		readers[i] = bytes.NewBuffer(data[i*size : (i+1)*size])
	}

	b, err = NewBlob(context.Background(), vrw, readers...)
	assert.NoError(err)
	assert.Equal(data, readAll(b))
}

func TestStreamingParallelBlob(t *testing.T) {
	assert := assert.New(t)

	buff := randomBuff(1 << 26 /* 64MB */)
	chunks := 4
	readers := make([]io.Reader, chunks)
	chunkSize := len(buff) / chunks

	for i := 0; i < len(readers); i++ {
		readers[i] = bytes.NewReader(buff[i*chunkSize : (i+1)*chunkSize])
	}

	vs := newTestValueStore()
	blob, err := NewBlob(context.Background(), vs, readers...)
	assert.NoError(err)
	outBuff := &bytes.Buffer{}
	_, err = blob.Copy(context.Background(), outBuff)
	assert.NoError(err)
	assert.True(bytes.Equal(buff, outBuff.Bytes()))
}
