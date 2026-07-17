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

package logging

import (
	"context"
	"log/slog"
)

var _ slog.Handler = (*TeeLogger)(nil)

type TeeLogger struct {
	handlers []slog.Handler
}

func NewTeeLogger(handlers ...slog.Handler) *TeeLogger {
	return &TeeLogger{
		handlers: handlers,
	}
}

func (t *TeeLogger) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range t.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}

	return false
}

func (t *TeeLogger) Handle(ctx context.Context, record slog.Record) error {
	for _, handler := range t.handlers {
		if err := handler.Handle(ctx, record); err != nil {
			return err
		}
	}

	return nil
}

func (t *TeeLogger) WithAttrs(attrs []slog.Attr) slog.Handler {
	ret := &TeeLogger{
		handlers: make([]slog.Handler, len(t.handlers)),
	}
	for k, handler := range t.handlers {
		ret.handlers[k] = handler.WithAttrs(attrs)
	}

	return ret
}

func (t *TeeLogger) WithGroup(name string) slog.Handler {
	ret := &TeeLogger{
		handlers: make([]slog.Handler, len(t.handlers)),
	}
	for k, handler := range t.handlers {
		ret.handlers[k] = handler.WithGroup(name)
	}

	return ret
}
