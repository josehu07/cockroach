// CW: added RS coding utils using 'reedsolomon' library

package raft

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testEntries [][]byte

func (ents testEntries) rsSize() (uint64, []uint32) {
	var dataSize uint64

	inputSizes := make([]uint32, len(ents))
	for i, e := range ents {
		inputSize := len(e)
		inputSizes[i] = uint32(inputSize)
		dataSize += uint64(4 + inputSize)
	}
	dataSize += 4

	return dataSize, inputSizes
}

func (ents testEntries) rsMarshalTo(buf []byte, inputSizes []uint32) error {
	binary.BigEndian.PutUint32(buf[:4], uint32(len(ents)))
	curr := 4

	for i, e := range ents {
		binary.BigEndian.PutUint32(buf[curr:curr+4], inputSizes[i])
		curr += 4

		inputSize := int(inputSizes[i])
		if len(e) != inputSize {
			return fmt.Errorf("entry size %d != expected %d", len(e), inputSize)
		}
		copy(buf[curr:curr+inputSize], e)
		curr += inputSize
	}

	return nil
}

func (ents testEntries) rsUnmarshal(buf []byte) error {
	if len(buf) < 4 {
		return fmt.Errorf("actual size %d too small", len(buf))
	}

	numElems := binary.BigEndian.Uint32(buf[:4])
	curr := 4
	ents = make(testEntries, numElems)

	for i := 0; i < int(numElems); i++ {
		elemSize := binary.BigEndian.Uint32(buf[curr : curr+4])
		curr += 4

		outputSize := int(elemSize)
		copy(ents[i], buf[curr:curr+outputSize])
		curr += outputSize
	}

	return nil
}

func TestRSCodingFromIntoData(t *testing.T) {
	input := testEntries([][]byte{
		[]byte("hello"),
		[]byte("world"),
	})

	coder, err := newCoder(3, 2)
	assert.Nil(t, err)
	cw := newCodeword[testEntries](coder)
	require.Equal(t, 3, cw.numDataShards)
	require.Equal(t, 2, cw.numParityShards)
	require.Equal(t, [][]byte{nil, nil, nil, nil, nil}, cw.shards)
	require.Equal(t, uint64(0), cw.shardSize)
	require.Equal(t, uint64(0), cw.dataSize)
	require.Equal(t, (*testEntries)(nil), cw.dataRef)

	err = cw.fromData(&input, coder)
	assert.Nil(t, err)
	require.Equal(t, []byte("\x00\x00\x00\x02\x00\x00\x00\x05"), cw.shards[0])
	require.Equal(t, []byte("hello\x00\x00\x00"), cw.shards[1])
	require.Equal(t, []byte("\x05world\x00\x00"), cw.shards[2])
	require.Equal(t, []byte("\x00\x00\x00\x00\x00\x00\x00\x00"), cw.shards[3])
	require.Equal(t, []byte("\x00\x00\x00\x00\x00\x00\x00\x00"), cw.shards[4])
	require.Equal(t, uint64(8), cw.shardSize)
	require.Equal(t, uint64(22), cw.dataSize)
	require.Equal(t, input, *cw.dataRef)

	outputPtr, err := cw.intoData(coder)
	assert.Nil(t, err)
	require.Equal(t, input, *outputPtr)

	cw.shards[1] = nil

	_, err = cw.intoData(coder)
	assert.NotNil(t, err)
}

func TestRSCodingSubsetAbsorb(t *testing.T) {
	input := testEntries([][]byte{
		[]byte("hello"),
		[]byte("world"),
	})

	coder, err := newCoder(3, 2)
	assert.Nil(t, err)
	cwa := newCodeword[testEntries](coder)
	err = cwa.fromData(&input, coder)
	assert.Nil(t, err)

	_, err = cwa.subsetCopy([]bool{true, false, true, false, false, false}, false)
	assert.NotNil(t, err)

	cw01, err := cwa.subsetCopy([]bool{true, false, true, false, false}, false)
	assert.Nil(t, err)
	require.Equal(t, 3, cw01.numDataShards)
	require.Equal(t, 2, cw01.numParityShards)
	require.Equal(t, 2, cw01.availDataShards())
	require.Equal(t, 0, cw01.availParityShards())
	require.Equal(t, (*testEntries)(nil), cw01.dataRef)

	cw02, err := cwa.subsetCopy([]bool{true, false, false, true, false}, true)
	assert.Nil(t, err)
	require.Equal(t, 3, cw02.numDataShards)
	require.Equal(t, 2, cw02.numParityShards)
	require.Equal(t, 1, cw02.availDataShards())
	require.Equal(t, 1, cw02.availParityShards())
	require.Equal(t, input, *cw02.dataRef)

	cwb := newCodeword[testEntries](coder)
	err = cwb.absorbOther(cw01)
	assert.Nil(t, err)
	require.Equal(t, 2, cwb.availDataShards())
	require.Equal(t, 0, cwb.availParityShards())

	err = cwb.absorbOther(cw02)
	assert.Nil(t, err)
	require.Equal(t, 2, cwb.availDataShards())
	require.Equal(t, 1, cwb.availParityShards())
	require.Equal(t, 3, cwb.availShards())
}

func TestRSCodingComputeVerify(t *testing.T) {
	input := testEntries([][]byte{
		[]byte("hello"),
		[]byte("world"),
	})

	coder, err := newCoder(3, 2)
	assert.Nil(t, err)
	cw := newCodeword[testEntries](coder)
	err = cw.computeParity(coder)
	assert.NotNil(t, err)
	err = cw.verifyParity(coder)
	assert.NotNil(t, err)

	err = cw.fromData(&input, coder)
	assert.Nil(t, err)
	err = cw.computeParity(coder)
	assert.Nil(t, err)
	require.Equal(t, 3, cw.availDataShards())
	require.Equal(t, 2, cw.availParityShards())
	require.Equal(t, 5, cw.availShards())
	err = cw.verifyParity(coder)
	assert.Nil(t, err)

	cw.shards[3] = nil
	err = cw.verifyParity(coder)
	assert.NotNil(t, err)
	err = cw.computeParity(coder)
	assert.Nil(t, err)
	require.Equal(t, 2, cw.availParityShards())

	cw.shards[1] = nil
	cw.shards[3] = nil
	err = cw.verifyParity(coder)
	assert.NotNil(t, err)
	err = cw.computeParity(coder)
	assert.NotNil(t, err)
}

func TestRSCodingReconstruction(t *testing.T) {
	input := testEntries([][]byte{
		[]byte("hello"),
		[]byte("world"),
	})

	coder, err := newCoder(3, 2)
	assert.Nil(t, err)
	cw := newCodeword[testEntries](coder)
	err = cw.reconstructAll(coder)
	assert.NotNil(t, err)
	err = cw.reconstructData(coder)
	assert.NotNil(t, err)

	err = cw.fromData(&input, coder)
	assert.Nil(t, err)
	err = cw.reconstructAll(coder)
	assert.Nil(t, err)
	require.Equal(t, 3, cw.availDataShards())
	require.Equal(t, 2, cw.availParityShards())

	cw.shards[1] = nil
	cw.shards[3] = nil
	err = cw.reconstructAll(coder)
	assert.Nil(t, err)
	require.Equal(t, 3, cw.availDataShards())
	require.Equal(t, 2, cw.availParityShards())

	cw.shards[0] = nil
	cw.shards[4] = nil
	err = cw.reconstructData(coder)
	assert.Nil(t, err)
	require.Equal(t, 3, cw.availDataShards())
	require.Equal(t, 1, cw.availParityShards())

	cw.shards[0] = nil
	cw.shards[2] = nil
	err = cw.reconstructAll(coder)
	assert.NotNil(t, err)
	err = cw.reconstructData(coder)
	assert.NotNil(t, err)
}
