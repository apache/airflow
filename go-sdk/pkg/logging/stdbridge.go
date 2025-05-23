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
	"fmt"
	"log/slog"
	"runtime"
	"time"
)

type RestyLoggerBridge struct {
	Handler slog.Handler
	Context context.Context
}

func (b *RestyLoggerBridge) log(level slog.Level, format string, args ...any) {
	if !b.Handler.Enabled(b.Context, level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip [Callers, log, Infof etc]
	r := slog.NewRecord(time.Now(), level, fmt.Sprintf(format, args...), pcs[0])
	_ = b.Handler.Handle(b.Context, r)
}

func (b *RestyLoggerBridge) Debugf(format string, v ...any) {
	b.log(slog.LevelDebug, format, v...)
}

func (b *RestyLoggerBridge) Infof(format string, v ...any) {
	b.log(slog.LevelInfo, format, v...)
}

func (b *RestyLoggerBridge) Warnf(format string, v ...any) {
	b.log(slog.LevelWarn, format, v...)
}

func (b *RestyLoggerBridge) Errorf(format string, v ...any) {
	b.log(slog.LevelError, format, v...)
}
