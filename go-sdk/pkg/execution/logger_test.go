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

package execution

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSocketLogHandlerBasicOutput(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler)

	logger.Info("test message", "key1", "val1")

	// Parse the output.
	output := buf.String()
	assert.True(t, strings.HasSuffix(output, "\n"), "output should end with newline")

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(output)), &entry))

	assert.Equal(t, "test message", entry["event"])
	assert.Equal(t, "info", entry["level"])
	assert.Equal(t, "val1", entry["key1"])
	assert.Contains(t, entry, "timestamp")
}

func TestSocketLogHandlerLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelWarn)
	logger := slog.New(handler)

	logger.Debug("should be filtered")
	logger.Info("also filtered")
	logger.Warn("should appear")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, 1)

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &entry))
	assert.Equal(t, "should appear", entry["event"])
	assert.Equal(t, "warn", entry["level"])
}

func TestSocketLogHandlerBuffering(t *testing.T) {
	// Create handler without a writer — messages should be buffered.
	handler := NewSocketLogHandler(nil, slog.LevelDebug)
	logger := slog.New(handler)

	logger.Info("buffered message 1")
	logger.Info("buffered message 2")

	// Connect the writer — buffered messages should flush.
	var buf bytes.Buffer
	handler.Connect(&buf)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	assert.Len(t, lines, 2)

	// New messages should write directly.
	logger.Info("direct message")
	lines = strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, 3)
}

func TestSocketLogHandlerWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler).With("component", "test")

	logger.Info("with attrs")

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	assert.Equal(t, "test", entry["component"])
}

func TestSocketLogHandlerWithGroup(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler).WithGroup("grp")

	logger.Info("grouped", "key", "val")

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	assert.Equal(t, "val", entry["grp.key"])
}

func TestSocketLogHandlerKeyMapping(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler)

	logger.Error("an error occurred")

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))

	// Check key mapping: "event" not "msg", "level" lowercase, "timestamp" not "time"
	assert.Equal(t, "an error occurred", entry["event"])
	assert.Equal(t, "error", entry["level"])
	_, hasTimestamp := entry["timestamp"]
	assert.True(t, hasTimestamp)
	_, hasMsg := entry["msg"]
	assert.False(t, hasMsg)
	_, hasTime := entry["time"]
	assert.False(t, hasTime)
}
