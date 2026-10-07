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
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	loggingLevelEnv    = "AIRFLOW__LOGGING__LOGGING_LEVEL"
	namespaceLevelsEnv = "AIRFLOW__LOGGING__NAMESPACE_LEVELS"

	// NOTSET sits below every supported record level. CRITICAL sits above ERROR
	// because slog has no built-in equivalents for either Airflow level.
	notsetLogLevel   = slog.Level(-100)
	criticalLogLevel = slog.LevelError + 4
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
	filter *logLevelFilter
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

type logLevelFilter struct {
	defaultLevel    slog.Level
	namespaceLevels map[string]slog.Level
}

// NewSocketLogHandler creates a new handler. If writer is nil, messages are
// buffered until Connect() is called.
func NewSocketLogHandler(writer io.Writer, level slog.Level) *SocketLogHandler {
	return newSocketLogHandler(writer, newLogLevelFilter(level, nil))
}

func newSocketLogHandlerFromEnv(writer io.Writer) *SocketLogHandler {
	defaultLevel, ok := parseLogLevel(os.Getenv(loggingLevelEnv))
	if !ok {
		defaultLevel = slog.LevelInfo
	}
	namespaceLevels, invalidEntries := parseNamespaceLogLevels(os.Getenv(namespaceLevelsEnv))
	handler := newSocketLogHandler(
		writer,
		newLogLevelFilter(defaultLevel, namespaceLevels),
	)
	logger := slog.New(handler)
	for _, invalidEntry := range invalidEntries {
		logger.Error("Ignoring invalid namespace_levels entry", "entry", invalidEntry)
	}
	return handler
}

func newSocketLogHandler(writer io.Writer, filter *logLevelFilter) *SocketLogHandler {
	shared := &socketLogHandlerShared{}
	if writer != nil {
		shared.writer = writer
		shared.connected = true
	}
	return &SocketLogHandler{
		shared: shared,
		filter: filter,
	}
}

func newLogLevelFilter(
	defaultLevel slog.Level,
	namespaceLevels map[string]slog.Level,
) *logLevelFilter {
	return &logLevelFilter{
		defaultLevel:    defaultLevel,
		namespaceLevels: namespaceLevels,
	}
}

func parseLogLevel(value string) (slog.Level, bool) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "NOTSET":
		return notsetLogLevel, true
	case "DEBUG":
		return slog.LevelDebug, true
	case "INFO":
		return slog.LevelInfo, true
	case "WARN", "WARNING":
		return slog.LevelWarn, true
	case "ERROR", "EXCEPTION":
		return slog.LevelError, true
	case "CRITICAL", "FATAL":
		return criticalLogLevel, true
	default:
		return 0, false
	}
}

func getAirflowLogLevelName(level slog.Level) string {
	switch {
	case level < slog.LevelDebug:
		return "notset"
	case level < slog.LevelInfo:
		return "debug"
	case level < slog.LevelWarn:
		return "info"
	case level < slog.LevelError:
		return "warning"
	case level < criticalLogLevel:
		return "error"
	default:
		return "critical"
	}
}

func parseNamespaceLogLevels(value string) (map[string]slog.Level, []string) {
	levels := make(map[string]slog.Level)
	var invalidEntries []string
	entries := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
	for _, entry := range entries {
		loggerName, levelName, ok := strings.Cut(entry, "=")
		loggerName = strings.TrimSpace(loggerName)
		level, validLevel := parseLogLevel(levelName)
		if !ok || loggerName == "" || !validLevel {
			invalidEntries = append(invalidEntries, entry)
			continue
		}
		levels[loggerName] = level
	}
	return levels, invalidEntries
}

func (f *logLevelFilter) getLevel(loggerName string) slog.Level {
	for namespace := loggerName; namespace != ""; {
		if level, ok := f.namespaceLevels[namespace]; ok {
			return level
		}
		separator := strings.LastIndexByte(namespace, '.')
		if separator == -1 {
			break
		}
		namespace = namespace[:separator]
	}
	return f.defaultLevel
}

func (f *logLevelFilter) isEnabled(loggerName string, level slog.Level) bool {
	return level >= f.getLevel(loggerName)
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
	return h.filter.isEnabled(h.getLoggerName(), level)
}

func (h *SocketLogHandler) Handle(_ context.Context, r slog.Record) error {
	loggerName := h.getLoggerName()
	if !h.filter.isEnabled(loggerName, r.Level) {
		return nil
	}

	entry := make(map[string]any)

	// Set standard fields.
	entry["event"] = r.Message
	if !r.Time.IsZero() {
		entry["timestamp"] = r.Time.Format(time.RFC3339Nano)
	}

	// Apply pre-configured attrs. Keys are already qualified with the groups
	// active at the WithAttrs call site, so the current h.groups is NOT
	// applied here — only to record-level attrs below. The stored key already
	// carries its prefix, so append with an empty prefix; any group value is
	// still expanded under that key.
	for _, a := range h.attrs {
		appendAttr(entry, "", slog.Attr{Key: a.key, Value: a.value})
	}

	// Apply record attrs. These were added at Handle time, so they pick up
	// the currently-active group prefix.
	prefix := h.groupPrefix()
	r.Attrs(func(a slog.Attr) bool {
		appendAttr(entry, prefix, a)
		return true
	})

	if loggerName != "" {
		entry["logger"] = loggerName
	}
	entry["level"] = getAirflowLogLevelName(r.Level)

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
		filter: h.filter,
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
		filter: h.filter,
		attrs:  h.attrs,
		groups: append(append([]string{}, h.groups...), name),
	}
}

func (h *SocketLogHandler) getLoggerName() string {
	return strings.Join(h.groups, ".")
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

// appendAttr writes a single attribute into entry under prefix+key. A group
// value is expanded recursively into dotted keys (matching WithGroup), so an
// inline slog.Group("req", slog.String("method", "GET")) becomes
// {"req.method": "GET"} rather than being dropped as "{}". Following the
// slog.Handler contract, an empty attr is skipped, a group with no attrs is
// ignored, and a group with an empty key is inlined at the current prefix.
func appendAttr(entry map[string]any, prefix string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		groupAttrs := a.Value.Group()
		if len(groupAttrs) == 0 {
			return
		}
		groupPrefix := prefix
		if a.Key != "" {
			groupPrefix = prefix + a.Key + "."
		}
		for _, ga := range groupAttrs {
			appendAttr(entry, groupPrefix, ga)
		}
		return
	}
	entry[prefix+a.Key] = resolveAttrValue(a.Value)
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
