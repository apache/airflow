// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build !race

// The oversized-payload test uses unsafe.Slice to fabricate a slice whose
// length exceeds MaxFrameSize without actually allocating 4 GiB of memory.
// Under -race, Go's checkptr instrumentation treats the resulting slice as
// straddling allocations and fatals the entire test binary, which would
// prevent `go test -race ./pkg/execution/` from running on the package. The
// guard under test is a single len() comparison, so excluding it from race
// builds keeps the rest of the package's race coverage runnable at the cost
// of losing race-mode coverage on this one path.

package execution

import (
	"bytes"
	"strconv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWriteFrameRejectsOversizedPayload pins the guard at the top of
// writeFrame against the rename/refactor that previously dropped its
// coverage. The guard only inspects len(payload) before doing any allocation
// or read of payload bytes, so we hand it a fake-length slice built with
// unsafe.Slice (one real byte of backing storage, length > MaxFrameSize)
// rather than allocating 4 GiB of real memory.
//
// The matching read-side guard at the top of readFrame is dead code with
// MaxFrameSize pinned at the uint32 maximum (payloadLen is uint32, so
// payloadLen > MaxFrameSize is never true) and cannot be exercised without
// modifying production code; it remains as defense-in-depth in case
// MaxFrameSize is ever lowered.
func TestWriteFrameRejectsOversizedPayload(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("requires 64-bit int to construct a slice longer than MaxFrameSize")
	}
	var backing byte
	payload := unsafe.Slice(&backing, uint64(MaxFrameSize)+1)

	err := writeFrame(&bytes.Buffer{}, payload)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds max")
}
