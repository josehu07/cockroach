// CW: added RS coding utils using 'reedsolomon' library

package raft

import (
	"fmt"

	rs "github.com/klauspost/reedsolomon"
)

// RS codward TODO: doc
type RSCodeword struct {
	numDataShards   uint
	numParityShards uint
	coder           *rs.Encoder // points to the coder stored in raft struct
}

func (r *raft) newRSCodeword(numDataShards, numParityShards uint) *RSCodeword {
	if numDataShards < 3 || numDataShards != (numDataShards+numParityShards)/2+1 {
		r.logger.Errorf(
			"invalid (numDataShards, numParityShards) pair (%d, %d)",
			numDataShards, numParityShards,
		)
		return nil
	}

	coder, ok := r.rscoders[numDataShards]
	if !ok {
		coder, err := rs.New(int(numDataShards), int(numParityShards))
		if err != nil {
			r.logger.Errorf("failed to create RS coder: %v", err)
			return nil
		}
		r.rscoders[numDataShards] = &coder
	}

	return &RSCodeword{
		numDataShards:   numDataShards,
		numParityShards: numParityShards,
		coder:           coder,
	}
}

func (cw *RSCodeword) String() string {
	return fmt.Sprintf("(%d,%d)", cw.numDataShards, cw.numParityShards)
}
