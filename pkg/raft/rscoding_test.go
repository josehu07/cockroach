// CW: added RS coding utils using 'reedsolomon' library

package raft

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRSCodingDummy(t *testing.T) {
	fmt.Println("rscoding dummy test")
	require.Equal(t, 7, 7)
}
