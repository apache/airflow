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
	"errors"
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

// TestSocketLogHandlerInlineGroup verifies that an inline slog.Group passed as
// a record-level attr is expanded into dotted keys (req.method) rather than
// being marshaled as "{}". A KindGroup value resolves to []slog.Attr whose
// unexported slog.Value would otherwise drop every field, so task code logging
// with slog.Group would silently lose data.
func TestSocketLogHandlerInlineGroup(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler)

	logger.Info(
		"inline group",
		slog.Group("req", slog.String("method", "GET"), slog.Int("code", 200)),
	)

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	assert.Equal(t, "GET", entry["req.method"])
	assert.EqualValues(t, 200, entry["req.code"])
	assert.NotContains(t, entry, "req")
}

// TestSocketLogHandlerInlineGroupUnderActiveGroup verifies that an inline group
// is further qualified by any group active at Handle time, so the dotted prefix
// stacks (outer.req.method).
func TestSocketLogHandlerInlineGroupUnderActiveGroup(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler).WithGroup("outer")

	logger.Info("nested inline group", slog.Group("req", slog.String("method", "GET")))

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	assert.Equal(t, "GET", entry["outer.req.method"])
}

// TestSocketLogHandlerGroupViaWithAttrs verifies that a group added through
// With() (a pre-configured attr) is expanded the same way as an inline group.
func TestSocketLogHandlerGroupViaWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler).With(slog.Group("req", slog.String("method", "GET")))

	logger.Info("group via with")

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	assert.Equal(t, "GET", entry["req.method"])
	assert.NotContains(t, entry, "req")
}

// TestSocketLogHandlerEmptyGroupOmitted verifies that a group with no attrs is
// dropped entirely, matching the slog.Handler contract.
func TestSocketLogHandlerEmptyGroupOmitted(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler)

	logger.Info("empty group", slog.Group("empty"), "kept", "yes")

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	assert.NotContains(t, entry, "empty")
	assert.Equal(t, "yes", entry["kept"])
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

// TestSocketLogHandlerStringifiesErrorAttr verifies that an attribute whose
// value is a stock `error` is rendered as its .Error() string. Without value
// resolution the underlying struct's unexported fields would be marshaled as
// "{}", silently dropping the message.
func TestSocketLogHandlerStringifiesErrorAttr(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler)

	logger.Error("task failed", "error", errors.New("boom"))

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	assert.Equal(t, "boom", entry["error"])
}

// logValuerAttr is an slog.LogValuer that resolves to a string. Used to
// confirm Resolve() is called before JSON marshaling.
type logValuerAttr string

func (l logValuerAttr) LogValue() slog.Value {
	return slog.StringValue(string(l) + ":resolved")
}

// TestSocketLogHandlerResolvesLogValuer verifies that an attribute whose
// value implements slog.LogValuer has Resolve() called before marshaling,
// matching the behaviour of slog.NewJSONHandler.
func TestSocketLogHandlerResolvesLogValuer(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler)

	logger.Info("with valuer", "field", logValuerAttr("hello"))

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))
	assert.Equal(t, "hello:resolved", entry["field"])
}

// TestSocketLogHandlerWithAttrsBeforeWithGroup verifies that attributes added
// via With() before a WithGroup() call are NOT retroactively qualified by
// that group. Per the slog.Handler contract: "All attributes added
// subsequently to that handler will be qualified by the group" — emphasis on
// subsequently. A naive implementation that stores attrs raw and applies the
// current group prefix at Handle time would break this and silently re-key
// pre-group attrs.
func TestSocketLogHandlerWithAttrsBeforeWithGroup(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler).With("a", 1).WithGroup("g").With("b", 2)

	logger.Info("interleaved", "c", 3)

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))

	// a was added before WithGroup("g") — must stay at top level.
	assert.EqualValues(
		t,
		1,
		entry["a"],
		"pre-group attr must not be re-qualified by a later WithGroup",
	)
	assert.NotContains(
		t,
		entry,
		"g.a",
		"pre-group attr must not appear under the later group prefix",
	)

	// b was added after WithGroup("g") — must be under g.
	assert.EqualValues(t, 2, entry["g.b"], "post-group attr must be qualified by the active group")
	assert.NotContains(t, entry, "b", "post-group attr must not appear at top level")

	// c is a record-level attr handled while "g" is active — also under g.
	assert.EqualValues(
		t,
		3,
		entry["g.c"],
		"record-level attr must be qualified by the active group",
	)
}

// TestSocketLogHandlerNestedGroups verifies that nested WithGroup calls
// produce dotted prefixes in declaration order, and that an attr added
// between two WithGroup calls only sees the groups that were active at its
// WithAttrs call site (not the later one).
func TestSocketLogHandlerNestedGroups(t *testing.T) {
	var buf bytes.Buffer
	handler := NewSocketLogHandler(&buf, slog.LevelDebug)
	logger := slog.New(handler).WithGroup("outer").With("a", 1).WithGroup("inner").With("b", 2)

	logger.Info("nested", "c", 3)

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry))

	assert.EqualValues(t, 1, entry["outer.a"], "a was added under outer only")
	assert.EqualValues(t, 2, entry["outer.inner.b"], "b was added under outer.inner")
	assert.EqualValues(t, 3, entry["outer.inner.c"], "record attr inherits outer.inner")
}
