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

// Package hclogslog adapts a log/slog Logger onto an hclog.Logger: it provides
// an slog.Handler that forwards records to an underlying hclog.Logger. It is the
// inverse of pkg/logging/shclog (which presents an slog Logger as an hclog).
//
// This lets code that logs through slog emit records in hclog's format, which is
// what the go-plugin host expects to parse from a plugin's stderr.
package hclogslog

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/hashicorp/go-hclog"
)

// Handler is an slog.Handler that forwards records to an hclog.Logger.
type Handler struct {
	l      hclog.Logger
	prefix string
	// basePos is how many positional slots the bound (WithAttrs) attributes at
	// the current group scope already consumed. Record attributes continue from
	// it so an empty-keyed bound attr and an empty-keyed record attr never
	// synthesize the same numeric key (which hclog would emit as a duplicate JSON
	// key, silently dropping one value). Reset to zero by WithGroup, since a new
	// group prefix already namespaces its keys.
	basePos int
}

// Adapt returns an slog.Handler that emits records through the given hclog.Logger.
func Adapt(l hclog.Logger) slog.Handler {
	return &Handler{l: l}
}

var _ slog.Handler = (*Handler)(nil)

// Enabled reports whether a record at the given level would be logged by the
// underlying hclog.Logger.
func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	switch {
	case level < slog.LevelDebug:
		return h.l.IsTrace()
	case level < slog.LevelInfo:
		return h.l.IsDebug()
	case level < slog.LevelWarn:
		return h.l.IsInfo()
	case level < slog.LevelError:
		return h.l.IsWarn()
	default:
		return h.l.IsError()
	}
}

// Handle forwards the record's message, level and attributes to the hclog.Logger.
func (h *Handler) Handle(_ context.Context, rec slog.Record) error {
	args := make([]any, 0, rec.NumAttrs()*2)
	position := h.basePos
	rec.Attrs(func(a slog.Attr) bool {
		args = append(args, h.flatten(h.prefix, position, a)...)
		position++
		return true
	})
	h.l.Log(translateLevel(rec.Level), rec.Message, args...)
	return nil
}

// WithAttrs returns a new handler whose records also carry the given attributes.
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	args := make([]any, 0, len(attrs)*2)
	for i, a := range attrs {
		args = append(args, h.flatten(h.prefix, h.basePos+i, a)...)
	}
	return &Handler{l: h.l.With(args...), prefix: h.prefix, basePos: h.basePos + len(attrs)}
}

// WithGroup returns a new handler that qualifies subsequent attribute keys with
// the group name.
func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &Handler{l: h.l, prefix: h.prefix + name + "."}
}

// flatten converts a single slog.Attr into a flat list of hclog key/value pairs.
// Groups are expanded recursively, their keys joined with a dot; an unnamed group
// is inlined; an empty attribute is dropped.
func (h *Handler) flatten(prefix string, position int, a slog.Attr) []any {
	value := a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return nil
	}
	if value.Kind() != slog.KindGroup {
		key := a.Key
		if key == "" {
			key = strconv.Itoa(position)
		}
		return []any{prefix + key, value.Any()}
	}

	group := value.Group()
	if len(group) == 0 {
		return nil
	}
	childPrefix := prefix
	if a.Key != "" {
		childPrefix = prefix + a.Key + "."
	}
	args := make([]any, 0, len(group)*2)
	for position, sub := range group {
		args = append(args, h.flatten(childPrefix, position, sub)...)
	}
	return args
}

func translateLevel(level slog.Level) hclog.Level {
	switch {
	case level < slog.LevelDebug:
		return hclog.Trace
	case level < slog.LevelInfo:
		return hclog.Debug
	case level < slog.LevelWarn:
		return hclog.Info
	case level < slog.LevelError:
		return hclog.Warn
	default:
		return hclog.Error
	}
}
