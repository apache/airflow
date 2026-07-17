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

package bundlefooter

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTempBinary(t *testing.T, contents []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "fake-binary")
	require.NoError(t, os.WriteFile(path, contents, 0o755))
	return path
}

func TestAppendAndRead_RoundTrip(t *testing.T) {
	binary := []byte("\x7FELFnot-really-an-elf-but-good-enough")
	source := []byte("package main\n\nfunc main() {}\n")
	metadata := []byte("airflow_bundle_metadata_version: \"1.0\"\nsdk:\n  language: go\n")

	path := writeTempBinary(t, binary)
	require.NoError(t, Append(path, source, metadata))

	got := mustRead(t, path)
	assert.Equal(t, len(binary)+len(source)+len(metadata)+TrailerSize, got.size)

	gotSource, gotMetadata, err := Read(path)
	require.NoError(t, err)
	assert.Equal(t, source, gotSource)
	assert.Equal(t, metadata, gotMetadata)

	ok, err := IsBundle(path)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestAppend_ZeroLengthSource(t *testing.T) {
	binary := []byte("\x7FELFstub")
	metadata := []byte("manifest")

	path := writeTempBinary(t, binary)
	require.NoError(t, Append(path, nil, metadata))

	source, gotMetadata, err := Read(path)
	require.NoError(t, err)
	assert.Empty(t, source)
	assert.Equal(t, metadata, gotMetadata)
}

func TestAppend_DeterministicOutput(t *testing.T) {
	binary := []byte("\x7FELFstub-binary-bytes")
	source := []byte("source")
	metadata := []byte("manifest")

	pathA := writeTempBinary(t, binary)
	pathB := writeTempBinary(t, binary)
	require.NoError(t, Append(pathA, source, metadata))
	require.NoError(t, Append(pathB, source, metadata))

	a, err := os.ReadFile(pathA)
	require.NoError(t, err)
	b, err := os.ReadFile(pathB)
	require.NoError(t, err)
	assert.Equal(t, a, b)
}

func TestRead_NotBundle(t *testing.T) {
	path := writeTempBinary(t, []byte("just a regular file with no footer"))

	_, _, err := Read(path)
	require.ErrorIs(t, err, ErrNotBundle)

	ok, err := IsBundle(path)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestRead_TooShort(t *testing.T) {
	path := writeTempBinary(t, []byte("hi"))

	_, _, err := Read(path)
	require.ErrorIs(t, err, ErrNotBundle)
}

func TestRead_UnknownVersion(t *testing.T) {
	binary := []byte("\x7FELFstub")
	source := []byte("src")
	metadata := []byte("md")
	path := writeTempBinary(t, binary)
	require.NoError(t, Append(path, source, metadata))

	// Mutate the version byte in the trailer.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	stat, err := f.Stat()
	require.NoError(t, err)
	// footer_ver lives at bytes 8..11 of the trailer.
	versionOffset := stat.Size() - TrailerSize + 8
	_, err = f.WriteAt([]byte{99, 0, 0, 0}, versionOffset)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, _, err = Read(path)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnknownVersion))
}

func TestRead_HashMismatch(t *testing.T) {
	binary := []byte("\x7FELFstub-binary-bytes")
	source := []byte("src")
	metadata := []byte("md")
	path := writeTempBinary(t, binary)
	require.NoError(t, Append(path, source, metadata))

	// Corrupt a byte inside the binary region; the trailer's binary_sha256
	// no longer matches what Read recomputes over [0, source_start).
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{'X'}, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, _, err = Read(path)
	require.ErrorIs(t, err, ErrHashMismatch)
}

type bundleStat struct {
	size int
}

func mustRead(t *testing.T, path string) bundleStat {
	t.Helper()
	stat, err := os.Stat(path)
	require.NoError(t, err)
	return bundleStat{size: int(stat.Size())}
}
