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
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// SocketLogHandler is an slog.Handler that streams structured JSON log lines
// to the logs TCP socket. Each log entry is a single JSON object followed by
// a newline, matching the Airflow log streaming format.
//
// Key mapping:
//   - "event" for the log message (not "msg")
//   - "level" in lowercase (not "INFO"/"ERROR")
//   - "timestamp" in RFC3339Nano format (not "time")
//   - Additional attributes are included as top-level fields
//
// Groups are encoded as dotted key prefixes on a flat JSON object
// (`{"grp.key": "val"}`), not as nested objects. The Airflow supervisor's
// log-streaming format consumes flat top-level fields, so emitting nested
// objects here would not be parsed correctly. Do not change this without
// updating the supervisor side in lockstep.
type SocketLogHandler struct {
	shared *socketLogHandlerShared
	level  slog.Level
	// attrs is the list of attributes accumulated via WithAttrs. Each entry's
	// key has already been qualified with whatever groups were active at the
	// WithAttrs call site, so a later WithGroup does NOT retroactively prefix
	// them — matching the slog.Handler contract that groups apply only to
	// subsequently-added attributes.
	attrs  []prefixedAttr
	groups []string
}

// prefixedAttr is an attribute whose key has been pre-qualified with the
// dotted prefix of the groups that were active when it was added via
// WithAttrs. The slog.Value is kept unresolved so LogValuer attributes are
// still evaluated lazily at Handle time.
type prefixedAttr struct {
	key   string
	value slog.Value
}

// socketLogHandlerShared holds the writer and buffer that must remain shared
// across WithAttrs / WithGroup clones; otherwise the sync.Mutex would be
// copied (which the runtime detector flags as a bug).
type socketLogHandlerShared struct {
	mu        sync.Mutex
	writer    io.Writer
	buf       [][]byte
	connected bool
}

var _ slog.Handler = (*SocketLogHandler)(nil)

// NewSocketLogHandler creates a new handler. If writer is nil, messages are
// buffered until Connect() is called.
func NewSocketLogHandler(writer io.Writer, level slog.Level) *SocketLogHandler {
	shared := &socketLogHandlerShared{}
	if writer != nil {
		shared.writer = writer
		shared.connected = true
	}
	return &SocketLogHandler{
		shared: shared,
		level:  level,
	}
}

// Connect sets the writer and flushes any buffered log messages.
func (h *SocketLogHandler) Connect(w io.Writer) {
	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()

	h.shared.writer = w
	h.shared.connected = true

	for _, line := range h.shared.buf {
		_, _ = w.Write(line)
	}
	h.shared.buf = nil
}

func (h *SocketLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *SocketLogHandler) Handle(_ context.Context, r slog.Record) error {
	entry := make(map[string]any)

	// Set standard fields.
	entry["event"] = r.Message
	entry["level"] = strings.ToLower(r.Level.String())
	if !r.Time.IsZero() {
		entry["timestamp"] = r.Time.Format(time.RFC3339Nano)
	}

	// Apply pre-configured attrs. Keys are already qualified with the groups
	// active at the WithAttrs call site, so the current h.groups is NOT
	// applied here — only to record-level attrs below.
	for _, a := range h.attrs {
		entry[a.key] = resolveAttrValue(a.value)
	}

	// Apply record attrs. These were added at Handle time, so they pick up
	// the currently-active group prefix.
	r.Attrs(func(a slog.Attr) bool {
		entry[h.prefixedKey(a.Key)] = resolveAttrValue(a.Value)
		return true
	})

	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	h.shared.mu.Lock()
	defer h.shared.mu.Unlock()

	if !h.shared.connected {
		h.shared.buf = append(h.shared.buf, line)
		return nil
	}

	_, err = h.shared.writer.Write(line)
	return err
}

func (h *SocketLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	// Qualify each new attr's key with the groups active right now, then
	// freeze the result. A later WithGroup must not retroactively re-prefix
	// these.
	prefix := h.groupPrefix()
	newAttrs := make([]prefixedAttr, len(h.attrs), len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	for _, a := range attrs {
		newAttrs = append(newAttrs, prefixedAttr{
			key:   prefix + a.Key,
			value: a.Value,
		})
	}
	return &SocketLogHandler{
		shared: h.shared,
		level:  h.level,
		attrs:  newAttrs,
		groups: h.groups,
	}
}

func (h *SocketLogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &SocketLogHandler{
		shared: h.shared,
		level:  h.level,
		attrs:  h.attrs,
		groups: append(append([]string{}, h.groups...), name),
	}
}

// groupPrefix returns the dotted prefix (including trailing ".") to apply to
// attribute keys for the currently-active groups, or "" if no group is
// active.
func (h *SocketLogHandler) groupPrefix() string {
	if len(h.groups) == 0 {
		return ""
	}
	return strings.Join(h.groups, ".") + "."
}

// prefixedKey prepends any active group names to the attribute key. Only
// used for record-level attrs, since attrs added via WithAttrs are stored
// with their key already qualified.
func (h *SocketLogHandler) prefixedKey(key string) string {
	return h.groupPrefix() + key
}

// resolveAttrValue returns the JSON-friendly representation of a slog.Value.
// It dereferences slog.LogValuer via Resolve() and then stringifies an error
// (whose unexported fields would otherwise marshal as "{}"), matching the
// behaviour of slog.NewJSONHandler. Non-error values pass through unchanged.
func resolveAttrValue(v slog.Value) any {
	resolved := v.Resolve().Any()
	if err, ok := resolved.(error); ok {
		return err.Error()
	}
	return resolved
}
