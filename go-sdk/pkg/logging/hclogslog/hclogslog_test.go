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

package hclogslog_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/pkg/logging"
	"github.com/apache/airflow/go-sdk/pkg/logging/hclogslog"
)

// newLogger returns an slog.Logger writing hclog JSON into buf, plus a decoder
// for the single record produced by one log call.
func newLogger(buf *bytes.Buffer, level hclog.Level) *slog.Logger {
	hc := hclog.New(&hclog.LoggerOptions{Level: level, Output: buf, JSONFormat: true})
	return slog.New(hclogslog.Adapt(hc))
}

func decode(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	return rec
}

func TestForwardsMessageAndAttrs(t *testing.T) {
	var buf bytes.Buffer
	newLogger(&buf, hclog.Trace).Info("hello", "user", "alice", "count", 3)

	rec := decode(t, &buf)
	assert.Equal(t, "hello", rec["@message"])
	assert.Equal(t, "info", rec["@level"])
	assert.Equal(t, "alice", rec["user"])
	assert.EqualValues(t, 3, rec["count"])
}

func TestTranslatesLevels(t *testing.T) {
	cases := map[string]struct {
		log      func(l *slog.Logger)
		expected string
	}{
		"trace": {func(l *slog.Logger) { l.Log(t.Context(), logging.LevelTrace, "m") }, "trace"},
		"debug": {func(l *slog.Logger) { l.Debug("m") }, "debug"},
		"info":  {func(l *slog.Logger) { l.Info("m") }, "info"},
		"warn":  {func(l *slog.Logger) { l.Warn("m") }, "warn"},
		"error": {func(l *slog.Logger) { l.Error("m") }, "error"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			tc.log(newLogger(&buf, hclog.Trace))
			assert.Equal(t, tc.expected, decode(t, &buf)["@level"])
		})
	}
}

func TestEnabledRespectsUnderlyingLevel(t *testing.T) {
	var buf bytes.Buffer
	log := newLogger(&buf, hclog.Warn)

	log.Info("dropped")
	assert.Empty(t, buf.String(), "records below the hclog level must be discarded")

	log.Error("kept")
	assert.Contains(t, buf.String(), "kept")
}

func TestFlattensGroupsWithDottedKeys(t *testing.T) {
	var buf bytes.Buffer
	log := newLogger(&buf, hclog.Trace).WithGroup("req").With("id", "abc")
	log.Info("done", slog.Group("meta", "n", 1))

	rec := decode(t, &buf)
	assert.Equal(t, "abc", rec["req.id"])
	assert.EqualValues(t, 1, rec["req.meta.n"])
}

func TestEmptyKeyAttrsUsePosition(t *testing.T) {
	var buf bytes.Buffer
	log := newLogger(&buf, hclog.Trace).
		With(slog.String("named", "bound"), slog.String("", "bound-empty")).
		WithGroup("req")
	log.LogAttrs(t.Context(), slog.LevelInfo, "done",
		slog.String("named", "record"),
		slog.String("", "record-empty"),
		slog.Group("meta",
			slog.String("", "group-first"),
			slog.String("named", "group-named"),
			slog.String("", "group-last"),
		),
	)

	rec := decode(t, &buf)
	assert.Equal(t, "bound-empty", rec["1"])
	assert.Equal(t, "record-empty", rec["req.1"])
	assert.Equal(t, "group-first", rec["req.meta.0"])
	assert.Equal(t, "group-last", rec["req.meta.2"])
}

func TestEmptyKeyBoundAndRecordDoNotCollide(t *testing.T) {
	var buf bytes.Buffer
	// No WithGroup between the bound and record attrs, so both share the empty
	// prefix; record positions must continue past the bound ones rather than
	// restart at 0, otherwise both empty-keyed attrs synthesize key "0" and hclog
	// emits a duplicate JSON key that drops the bound value.
	log := newLogger(&buf, hclog.Trace).With(slog.String("", "bound"))
	log.Info("m", slog.String("", "record"))

	rec := decode(t, &buf)
	assert.Equal(t, "bound", rec["0"])
	assert.Equal(t, "record", rec["1"])
}

func TestUnnamedGroupIsInlinedAndEmptyDropped(t *testing.T) {
	var buf bytes.Buffer
	log := newLogger(&buf, hclog.Trace)
	log.Info("m",
		slog.Group("", "inlined", "yes"),
		slog.Group("empty"),
		slog.Attr{},
	)

	rec := decode(t, &buf)
	assert.Equal(t, "yes", rec["inlined"])
	_, hasEmpty := rec["empty"]
	assert.False(t, hasEmpty, "empty group must not be emitted")
}

func TestWithGroupEmptyNameIsNoop(t *testing.T) {
	var buf bytes.Buffer
	log := newLogger(&buf, hclog.Trace).WithGroup("")
	log.Info("m", "k", "v")

	assert.Equal(t, "v", decode(t, &buf)["k"])
}

// slog.Logger short-circuits WithGroup("") and prunes childless named groups
// before they reach the handler, so drive the slog.Handler directly to cover
// those branches.
func TestHandlerContractEdgeCases(t *testing.T) {
	var buf bytes.Buffer
	hc := hclog.New(&hclog.LoggerOptions{Level: hclog.Trace, Output: &buf, JSONFormat: true})
	h := hclogslog.Adapt(hc)

	assert.Same(t, h, h.WithGroup(""), "empty group name must return the same handler")

	// WithAttrs receives []slog.Attr verbatim (no slog.Logger pruning), so a
	// childless named group here exercises flatten's empty-group branch.
	h = h.WithAttrs([]slog.Attr{slog.Group("empty"), slog.String("bound", "b")})
	rec := slog.NewRecord(time.Time{}, slog.LevelInfo, "m", 0)
	rec.AddAttrs(slog.String("kept", "v"))
	require.NoError(t, h.Handle(t.Context(), rec))

	decoded := decode(t, &buf)
	assert.Equal(t, "v", decoded["kept"])
	assert.Equal(t, "b", decoded["bound"])
	_, hasEmpty := decoded["empty"]
	assert.False(t, hasEmpty, "childless named group must not be emitted")
}
