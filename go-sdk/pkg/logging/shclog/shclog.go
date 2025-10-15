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

// Package shclog provides an type that acts like an hclog, but that outputs to a log/slog Logger
package shclog

// Based off https://github.com/ValerySidorin/shclog/blob/89f4174bdefffff1f3cf6410e7e9fe8d0e8538d0/shclog.go
// but adapted to keep caller location right, and use timestamp from the remote side when it's sent.

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"runtime"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/apache/airflow/go-sdk/pkg/logging"
)

const (
	TimeFormatJSON = "2006-01-02T15:04:05.000000Z0700"
	TimeFormat     = "2006-01-02T15:04:05.000Z0700"
)

type Shclog struct {
	l *slog.Logger

	namePrefix  string
	level       hclog.Level
	impliedArgs []any
}

func New(l *slog.Logger) hclog.Logger {
	return &Shclog{l: l, level: getSlogLevel(l)}
}

func (l *Shclog) Log(level hclog.Level, msg string, args ...any) {
	switch level {
	case hclog.Trace:
		l.Trace(msg, args...)
	case hclog.Debug:
		l.Debug(msg, args...)
	case hclog.Info:
		l.Info(msg, args...)
	case hclog.Warn:
		l.Warn(msg, args...)
	case hclog.Error:
		l.Error(msg, args...)
	case hclog.Off:
	default:
		l.Info(msg, args...)
	}
}

func processAndAddAttrs(r *slog.Record, args []any) {
	for len(args) > 0 {
		switch key := args[0].(type) {
		case string:
			if len(args) == 1 {
				// Bad key, oh well.
				r.Add(args...)
				return
			}
			if key == "timestamp" {
				val := fmt.Sprintf("%s", args[1])
				t, err := time.Parse(TimeFormatJSON, val)
				if err != nil {
					// Try parsing with the other format
					t, err = time.Parse(TimeFormat, val)
				}
				if err == nil {
					r.Time = t
					args = args[2:]
					continue
				}
			}
			r.AddAttrs(slog.Any(key, args[1]))
			args = args[2:]
		default:
			r.Add(args[0])
			args = args[1:]
		}
	}
}

func (l *Shclog) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	if !l.l.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip [Callers, this fn, the Trace/Log/Info etc wrapper]
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	processAndAddAttrs(&r, args)
	_ = l.l.Handler().Handle(context.Background(), r)
}

func (l *Shclog) Trace(msg string, args ...any) {
	l.log(context.Background(), logging.LevelTrace, msg, args...)
}

func (l *Shclog) Debug(msg string, args ...any) {
	l.log(context.Background(), slog.LevelDebug, msg, args...)
}

func (l *Shclog) Info(msg string, args ...any) {
	l.log(context.Background(), slog.LevelInfo, msg, args...)
}

func (l *Shclog) Warn(msg string, args ...any) {
	l.log(context.Background(), slog.LevelWarn, msg, args...)
}

func (l *Shclog) Error(msg string, args ...any) {
	l.log(context.Background(), slog.LevelError, msg, args...)
}

func (l *Shclog) IsTrace() bool {
	return l.level == hclog.Trace
}

func (l *Shclog) IsDebug() bool {
	return l.level == hclog.Debug
}

func (l *Shclog) IsInfo() bool {
	return l.level == hclog.Info
}

func (l *Shclog) IsWarn() bool {
	return l.level == hclog.Warn
}

func (l *Shclog) IsError() bool {
	return l.level == hclog.Error
}

func (l *Shclog) ImpliedArgs() []any {
	return l.impliedArgs
}

func (l *Shclog) With(args ...any) hclog.Logger {
	sl := cloneSlog(l.l)
	impliedArgs := append(l.impliedArgs, args...)

	return &Shclog{
		l:           sl.With(args...),
		namePrefix:  l.namePrefix,
		level:       l.level,
		impliedArgs: impliedArgs,
	}
}

func (l *Shclog) Name() string {
	return l.namePrefix
}

func (l *Shclog) Named(name string) hclog.Logger {
	if l.namePrefix != "" {
		name = l.namePrefix + "." + name
	}
	return &Shclog{
		l:           l.l.With("name", name),
		namePrefix:  name,
		level:       l.level,
		impliedArgs: l.impliedArgs,
	}
}

func (l *Shclog) ResetNamed(name string) hclog.Logger {
	sl := cloneSlog(l.l)

	return &Shclog{
		l:           sl,
		namePrefix:  name,
		level:       l.level,
		impliedArgs: l.impliedArgs,
	}
}

func (l *Shclog) SetLevel(level hclog.Level) {
	// noop: Can not set level from here. Please set it through slog.Handler options
}

func (l *Shclog) GetLevel() hclog.Level {
	return l.level
}

func (l *Shclog) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	if opts == nil {
		opts = &hclog.StandardLoggerOptions{}
	}
	return log.New(l.StandardWriter(opts), "", 0)
}

func (l *Shclog) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return io.Discard
}

func getSlogLevel(l *slog.Logger) hclog.Level {
	h := l.Handler()
	ctx := context.Background()

	if h.Enabled(ctx, logging.LevelTrace) {
		return hclog.Trace
	}
	if h.Enabled(ctx, slog.LevelDebug) {
		return hclog.Debug
	}
	if h.Enabled(ctx, slog.LevelInfo) {
		return hclog.Info
	}
	if h.Enabled(ctx, slog.LevelError) {
		return hclog.Error
	}

	return hclog.Info
}

func cloneSlog(l *slog.Logger) *slog.Logger {
	c := *l
	return &c
}
