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

// Package bundlefooter implements the AFBNDL01 trailer described in
// ADR 0004 (and providers/sdk/executable/docs/bundle-spec.rst). A bundle
// file is the compiled executable with three appended regions: the source
// bytes, the manifest bytes, and a fixed 32-byte trailer that locates them.
//
// The trailer layout (all little-endian) is:
//
//	bytes  0..3   source_len    uint32
//	bytes  4..7   metadata_len  uint32
//	bytes  8..11  footer_ver    uint32  (= 1)
//	bytes 12..23  reserved      12 bytes, zero
//	bytes 24..31  magic         8 bytes ASCII "AFBNDL01"
package bundlefooter

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
)

const (
	// TrailerSize is the fixed length of the trailer, in bytes.
	TrailerSize = 32

	// FooterVersion is the currently defined trailer-format version.
	FooterVersion = 1

	// MaxRegionSize is the largest source or metadata region this footer
	// format can address (uint32 length field).
	MaxRegionSize = math.MaxUint32
)

// Magic is the 8-byte ASCII tag that identifies a file as a bundle.
var Magic = [8]byte{'A', 'F', 'B', 'N', 'D', 'L', '0', '1'}

// ErrNotBundle is returned by Read when the file does not end with the
// AFBNDL01 magic.
var ErrNotBundle = errors.New("bundlefooter: not a bundle (magic mismatch)")

// ErrUnknownVersion is returned by Read when the trailer's footer_ver field
// is something other than FooterVersion.
var ErrUnknownVersion = errors.New("bundlefooter: unknown footer version")

// Trailer carries the parsed contents of a bundle's 32-byte trailer.
type Trailer struct {
	SourceLen     uint32
	MetadataLen   uint32
	FooterVersion uint32
}

// Append writes the source bytes, metadata bytes, and trailer to the end of
// the file at execPath. The file's existing contents (the executable) are
// left intact and its mode bits are preserved. source MAY be nil/empty.
func Append(execPath string, source, metadata []byte) error {
	if int64(len(source)) > MaxRegionSize {
		return fmt.Errorf(
			"bundlefooter: source region too large (%d bytes, max %d)",
			len(source),
			MaxRegionSize,
		)
	}
	if int64(len(metadata)) > MaxRegionSize {
		return fmt.Errorf(
			"bundlefooter: metadata region too large (%d bytes, max %d)",
			len(metadata),
			MaxRegionSize,
		)
	}

	f, err := os.OpenFile(execPath, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return fmt.Errorf("bundlefooter: opening %s: %w", execPath, err)
	}
	defer f.Close()

	if len(source) > 0 {
		if _, err := f.Write(source); err != nil {
			return fmt.Errorf("bundlefooter: writing source region: %w", err)
		}
	}
	if len(metadata) > 0 {
		if _, err := f.Write(metadata); err != nil {
			return fmt.Errorf("bundlefooter: writing metadata region: %w", err)
		}
	}

	trailer := encodeTrailer(uint32(len(source)), uint32(len(metadata)))
	if _, err := f.Write(trailer[:]); err != nil {
		return fmt.Errorf("bundlefooter: writing trailer: %w", err)
	}
	return nil
}

// Read parses the trailer of the file at path and returns the embedded
// source and metadata regions. Returns ErrNotBundle if the magic does not
// match (so callers may silently ignore non-bundle files).
func Read(path string) (source, metadata []byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("bundlefooter: opening %s: %w", path, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("bundlefooter: stat %s: %w", path, err)
	}
	size := stat.Size()
	if size < TrailerSize {
		return nil, nil, ErrNotBundle
	}

	var trailer [TrailerSize]byte
	if _, err := f.ReadAt(trailer[:], size-TrailerSize); err != nil {
		return nil, nil, fmt.Errorf("bundlefooter: reading trailer: %w", err)
	}

	t, err := decodeTrailer(trailer)
	if err != nil {
		return nil, nil, err
	}

	metadataStart := size - TrailerSize - int64(t.MetadataLen)
	sourceStart := metadataStart - int64(t.SourceLen)
	if sourceStart < 0 {
		return nil, nil, fmt.Errorf(
			"bundlefooter: trailer reports regions larger than file (source_len=%d metadata_len=%d size=%d)",
			t.SourceLen,
			t.MetadataLen,
			size,
		)
	}
	if sourceStart == 0 {
		return nil, nil, fmt.Errorf("bundlefooter: empty binary region")
	}

	if t.SourceLen > 0 {
		source = make([]byte, t.SourceLen)
		if _, err := f.ReadAt(source, sourceStart); err != nil && !errors.Is(err, io.EOF) {
			return nil, nil, fmt.Errorf("bundlefooter: reading source region: %w", err)
		}
	}
	if t.MetadataLen > 0 {
		metadata = make([]byte, t.MetadataLen)
		if _, err := f.ReadAt(metadata, metadataStart); err != nil && !errors.Is(err, io.EOF) {
			return nil, nil, fmt.Errorf("bundlefooter: reading metadata region: %w", err)
		}
	}
	return source, metadata, nil
}

// IsBundle reports whether the file at path ends with the AFBNDL01 magic.
// It does not validate the trailer beyond the magic check, so a file with a
// matching magic but a corrupt trailer body still returns true.
func IsBundle(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return false, err
	}
	if stat.Size() < TrailerSize {
		return false, nil
	}

	var tail [8]byte
	if _, err := f.ReadAt(tail[:], stat.Size()-int64(len(tail))); err != nil {
		return false, err
	}
	return tail == Magic, nil
}

func encodeTrailer(sourceLen, metadataLen uint32) [TrailerSize]byte {
	var t [TrailerSize]byte
	binary.LittleEndian.PutUint32(t[0:4], sourceLen)
	binary.LittleEndian.PutUint32(t[4:8], metadataLen)
	binary.LittleEndian.PutUint32(t[8:12], FooterVersion)
	// bytes 12..23 are reserved, zero
	copy(t[24:32], Magic[:])
	return t
}

func decodeTrailer(b [TrailerSize]byte) (Trailer, error) {
	var magic [8]byte
	copy(magic[:], b[24:32])
	if magic != Magic {
		return Trailer{}, ErrNotBundle
	}
	t := Trailer{
		SourceLen:     binary.LittleEndian.Uint32(b[0:4]),
		MetadataLen:   binary.LittleEndian.Uint32(b[4:8]),
		FooterVersion: binary.LittleEndian.Uint32(b[8:12]),
	}
	if t.FooterVersion != FooterVersion {
		return Trailer{}, fmt.Errorf("%w: %d", ErrUnknownVersion, t.FooterVersion)
	}
	return t, nil
}
