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
type SocketLogHandler struct {
	shared *socketLogHandlerShared
	level  slog.Level
	attrs  []slog.Attr
	groups []string
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

	// Apply pre-configured attrs.
	for _, a := range h.attrs {
		key := h.prefixedKey(a.Key)
		entry[key] = a.Value.Any()
	}

	// Apply record attrs.
	r.Attrs(func(a slog.Attr) bool {
		key := h.prefixedKey(a.Key)
		entry[key] = a.Value.Any()
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
	return &SocketLogHandler{
		shared: h.shared,
		level:  h.level,
		attrs:  append(append([]slog.Attr{}, h.attrs...), attrs...),
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

// prefixedKey prepends any active group names to the attribute key.
func (h *SocketLogHandler) prefixedKey(key string) string {
	if len(h.groups) == 0 {
		return key
	}
	return strings.Join(h.groups, ".") + "." + key
}
