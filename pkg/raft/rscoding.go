// CW: added RS coding utils using 'reedsolomon' library

package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	rs "github.com/klauspost/reedsolomon"
)

// Minimal requirements on marshalling/unmarshalling for RS-codable pointed data.
type marshallablePtr[T any] interface {
	*T

	// First output is marshalled data size. Second output is an optional slice of entry sizes and
	// should be calculated if type is slice.
	rsSize() (uint64, []uint32)

	// Second input is an optional slice of entry sizes for slice type. NOT yet supporting nested
	// slices as input type.
	rsMarshalTo([]byte, []uint32) error

	// This requires the implementing type being a pointer as this method modifies self.
	rsUnmarshal([]byte) error
}

// Alias for pb.Entry, whose pointers implement marshallablePtr.
type raftEntry pb.Entry

func (ent *raftEntry) rsSize() (uint64, []uint32) {
	return uint64((*pb.Entry)(ent).Size()), nil
}

func (ent *raftEntry) rsMarshalTo(buf []byte, _ []uint32) error {
	_, err := (*pb.Entry)(ent).MarshalTo(buf)
	return err
}

func (ent *raftEntry) rsUnmarshal(buf []byte) error {
	return (*pb.Entry)(ent).Unmarshal(buf)
}

// Alias for []pb.Entry, whose pointers implement marshallablePtr.
type raftEntries []raftEntry

func (ents *raftEntries) rsSize() (uint64, []uint32) {
	var dataSize uint64

	inputSizes := make([]uint32, len(*ents))
	for i, e := range *ents {
		inputSize, _ := e.rsSize()
		inputSizes[i] = uint32(inputSize)
		dataSize += 4 + inputSize
	}
	dataSize += 4

	return dataSize, inputSizes
}

func (ents *raftEntries) rsMarshalTo(buf []byte, inputSizes []uint32) error {
	binary.BigEndian.PutUint32(buf[:4], uint32(len(*ents)))
	curr := 4

	for i, e := range *ents {
		binary.BigEndian.PutUint32(buf[curr:curr+4], inputSizes[i])
		curr += 4

		inputSize := int(inputSizes[i])
		if err := e.rsMarshalTo(buf[curr:curr+inputSize], nil); err != nil {
			return err
		}
		curr += inputSize
	}

	return nil
}

func (ents *raftEntries) rsUnmarshal(buf []byte) error {
	if len(buf) < 4 {
		return fmt.Errorf("actual size %d too small", len(buf))
	}

	numElems := binary.BigEndian.Uint32(buf[:4])
	curr := 4
	*ents = make(raftEntries, numElems)

	for i := 0; i < int(numElems); i++ {
		elemSize := binary.BigEndian.Uint32(buf[curr : curr+4])
		curr += 4

		outputSize := int(elemSize)
		if err := (&(*ents)[i]).rsUnmarshal(buf[curr : curr+outputSize]); err != nil {
			return err
		}
		curr += outputSize
	}

	return nil
}

// RS coder with Encoder and Extensions interfaces.
type rsCoder interface {
	rs.Encoder
	rs.Extensions
}

// Create an extended RS coder with given number of data shards and parity shards.
func newCoder(numDataShards, numParityShards int) (rsCoder, error) {
	if numDataShards < 3 || numParityShards < 0 || numDataShards != (numDataShards+numParityShards)/2+1 {
		return nil, fmt.Errorf("invalid (data, parity) pair (%d, %d)", numDataShards, numParityShards)
	}
	coder, err := rs.New(numDataShards, numParityShards)
	if err != nil {
		return nil, err
	}
	return coder.(rsCoder), nil
}

// RS codeword with given number of data shards and parity shards.
type rsCodeword[T any, P marshallablePtr[T]] struct {
	numDataShards   int
	numParityShards int
	shards          [][]byte
	shardSize       uint64
	dataSize        uint64
	dataRef         P // optional, helps avoid copied unmarshalling in some cases
}

// Create a new empty RS codeword.
func newCodeword[T any, P marshallablePtr[T]](coder rsCoder) *rsCodeword[T, P] {
	return &rsCodeword[T, P]{
		numDataShards:   coder.DataShards(),
		numParityShards: coder.ParityShards(),
		shards:          make([][]byte, coder.DataShards()+coder.ParityShards()),
		shardSize:       0,
		dataSize:        0,
		dataRef:         nil,
	}
}

func (cw *rsCodeword[T, P]) String() string {
	return fmt.Sprintf("RS(%d,%d)/%d/", cw.numDataShards, cw.numParityShards, cw.dataSize)
}

func (cw *rsCodeword[T, P]) dimsMatch(coder rsCoder) error {
	if cw.numDataShards != coder.DataShards() || cw.numParityShards != coder.ParityShards() {
		return fmt.Errorf("(data, parity) pair mismatch: (%d, %d) != (%d, %d)",
			cw.numDataShards, cw.numParityShards, coder.DataShards(), coder.ParityShards())
	} else {
		return nil
	}
}

// Calculate (and set) size per shard, size of padded data, and size of parity data using actual size.
func (cw *rsCodeword[T, P]) usefulSizes() (paddedSize, paritySize uint64) {
	paddedSize = cw.dataSize
	residue := cw.dataSize % uint64(cw.numDataShards)
	if residue != 0 {
		paddedSize += uint64(cw.numDataShards) - residue
	}
	cw.shardSize = paddedSize / uint64(cw.numDataShards)
	paritySize = uint64(cw.numParityShards) * cw.shardSize
	return
}

// Encode input marshallablePtr data into shards. The second output is the size of actual bytes of
// marshalled input, without zero padding.
func (cw *rsCodeword[T, P]) fromData(input P, coder rsCoder) error {
	if err := cw.dimsMatch(coder); err != nil {
		return err
	}

	dataSize, inputSizes := input.rsSize()
	cw.dataSize = dataSize
	paddedSize, paritySize := cw.usefulSizes()

	buf := make([]byte, paddedSize, paddedSize+paritySize)
	if err := input.rsMarshalTo(buf, inputSizes); err != nil {
		return err
	}

	if shards, err := coder.Split(buf); err != nil {
		return err
	} else {
		cw.shards = shards
		cw.dataRef = input
		return nil
	}
}

// Decode from shards and actual bytes size into output object, requiring that all data shards
// must already be available.
func (cw *rsCodeword[T, P]) intoData(coder rsCoder) (P, error) {
	if err := cw.dimsMatch(coder); err != nil {
		return nil, err
	}
	if cw.dataSize == 0 {
		return nil, fmt.Errorf("codeword is null")
	}
	if cw.availDataShards() < cw.numDataShards {
		return nil, fmt.Errorf("not all data shards present: %d / %d",
			cw.availDataShards(), cw.numDataShards)
	}

	if cw.dataRef == nil {
		var buf bytes.Buffer
		if err := coder.Join(&buf, cw.shards, int(cw.dataSize)); err != nil {
			return nil, err
		}

		var outputData T
		output := P(&outputData)
		if err := output.rsUnmarshal(buf.Bytes()); err != nil {
			return nil, err
		}
		return output, nil
	} else {
		return cw.dataRef, nil
	}
}

// Total number of shards.
func (cw *rsCodeword[T, P]) numShards() int {
	return cw.numDataShards + cw.numParityShards
}

// Number of available data shards.
func (cw *rsCodeword[T, P]) availDataShards() int {
	avail := 0
	for i := 0; i < cw.numDataShards; i++ {
		if cw.shards[i] != nil {
			avail++
		}
	}
	return avail
}

// Number of available parity shards.
func (cw *rsCodeword[T, P]) availParityShards() int {
	avail := 0
	for i := cw.numDataShards; i < cw.numShards(); i++ {
		if cw.shards[i] != nil {
			avail++
		}
	}
	return avail
}

// Total number of available shards.
func (cw *rsCodeword[T, P]) availShards() int {
	avail := 0
	for i := 0; i < cw.numShards(); i++ {
		if cw.shards[i] != nil {
			avail++
		}
	}
	return avail
}

// Get a "bitmap" (as a slice of bools) of available shards' indices.
func (cw *rsCodeword[T, P]) availShardsMap() []bool {
	avail := make([]bool, cw.numShards())
	for i := 0; i < cw.numShards(); i++ {
		if cw.shards[i] != nil {
			avail[i] = true
		}
	}
	return avail
}

// Compute the parity shards.
func (cw *rsCodeword[T, P]) computeParity(coder rsCoder) error {
	if err := cw.dimsMatch(coder); err != nil {
		return err
	}
	if cw.dataSize == 0 {
		return fmt.Errorf("codeword is null")
	}
	if cw.numParityShards == 0 {
		if cw.availDataShards() == cw.numDataShards {
			return nil
		} else {
			return fmt.Errorf("insufficient data shards: %d / %d", cw.availDataShards(), cw.numDataShards)
		}
	}

	for i, e := range cw.shards {
		if e == nil {
			if i < cw.numDataShards {
				return fmt.Errorf("data shard %d is missing", i)
			} else {
				cw.shards[i] = make([]byte, cw.shardSize)
			}
		}
	}
	if err := coder.Encode(cw.shards); err != nil {
		return err
	}
	return nil
}

// Reconstruct all shards from available shards.
func (cw *rsCodeword[T, P]) reconstructAll(coder rsCoder) error {
	if err := cw.dimsMatch(coder); err != nil {
		return err
	}
	if cw.dataSize == 0 {
		return fmt.Errorf("codeword is null")
	}
	if cw.numParityShards == 0 {
		if cw.availDataShards() == cw.numDataShards {
			return nil
		} else {
			return fmt.Errorf("insufficient data shards: %d / %d", cw.availDataShards(), cw.numDataShards)
		}
	}

	if err := coder.Reconstruct(cw.shards); err != nil {
		return err
	}
	return nil
}

// Reconstruct data shards from available shards.
func (cw *rsCodeword[T, P]) reconstructData(coder rsCoder) error {
	if err := cw.dimsMatch(coder); err != nil {
		return err
	}
	if cw.dataSize == 0 {
		return fmt.Errorf("codeword is null")
	}
	if cw.numParityShards == 0 {
		if cw.availDataShards() == cw.numDataShards {
			return nil
		} else {
			return fmt.Errorf("insufficient data shards: %d / %d", cw.availDataShards(), cw.numDataShards)
		}
	}

	if err := coder.ReconstructData(cw.shards); err != nil {
		return err
	}
	return nil
}

// Verify if the current parity shards are correct.
func (cw *rsCodeword[T, P]) verifyParity(coder rsCoder) error {
	if err := cw.dimsMatch(coder); err != nil {
		return err
	}
	if cw.dataSize == 0 {
		return fmt.Errorf("codeword is null")
	}
	if cw.numParityShards == 0 {
		if cw.availDataShards() == cw.numDataShards {
			return nil
		} else {
			return fmt.Errorf("not all shards present: %d / %d", cw.availDataShards(), cw.numDataShards)
		}
	}

	if cw.availShards() < cw.numShards() {
		return fmt.Errorf("not all shards preseent: %d / %d", cw.availShards(), cw.numShards())
	}

	pass, err := coder.Verify(cw.shards)
	if err != nil {
		return err
	} else if !pass {
		return fmt.Errorf("parity check failed")
	} else {
		return nil
	}
}

// Creates a new rsCodeword that owns a copy of a subset of the shards, and optionally a
// pointer to the original data.
func (cw *rsCodeword[T, P]) subsetCopy(subset []bool, keepDataRef bool) (*rsCodeword[T, P], error) {
	if len(subset) != cw.numShards() {
		return nil, fmt.Errorf("len(subset) %d != numShards %d", len(subset), cw.numShards())
	}

	shards := make([][]byte, cw.numShards())
	for i, need := range subset {
		if need {
			shards[i] = make([]byte, cw.shardSize)
			copy(shards[i], cw.shards[i])
		}
	}

	var dataRef P
	if keepDataRef {
		dataRef = cw.dataRef
	}

	return &rsCodeword[T, P]{
		numDataShards:   cw.numDataShards,
		numParityShards: cw.numParityShards,
		shards:          shards,
		shardSize:       cw.shardSize,
		dataSize:        cw.dataSize,
		dataRef:         dataRef,
	}, nil
}

// Absorbs another rsCodeword, taking its available shards (without copying).
func (cw *rsCodeword[T, P]) absorbOther(other *rsCodeword[T, P]) error {
	if cw.numDataShards != other.numDataShards || cw.numParityShards != other.numParityShards {
		return fmt.Errorf("(data, parity) pair mismatch: (%d, %d) != (%d, %d)",
			cw.numDataShards, cw.numParityShards, other.numDataShards, other.numParityShards)
	}
	if cw.dataSize != 0 && cw.dataSize != other.dataSize {
		return fmt.Errorf("actual size mismatch: %d != %d", cw.dataSize, other.dataSize)
	}

	if cw.dataSize == 0 {
		// cw was null
		cw.dataSize = other.dataSize
		cw.shardSize = other.shardSize
	}

	for i := 0; i < cw.numShards(); i++ {
		if cw.shards[i] == nil && other.shards[i] != nil {
			cw.shards[i] = other.shards[i]
		}
	}
	return nil
}
