/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Log channel — newline-delimited JSON log records over TCP.
//
// The Airflow coordinator's `_bridge` reads lines from this socket,
// parses each as JSON, and re-emits through structlog using the same
// handler used for ordinary Python task logs
// (`process_log_messages_from_subprocess`). Required fields are
// `event`, `level`, `logger`, `timestamp`. Extra fields pass through
// as structured log keys.
//
// The `logger` field becomes the bracketed name column in structlog's
// ConsoleRenderer output (e.g. `[ts-sdk.runtime] Coordinator runtime
// started`). Use hierarchical names — `ts-sdk.runtime`, `ts-sdk.comm`,
// `ts-sdk.client` — so SDK-emitted lines are visibly distinct from
// user task logs (which typically use the task's module name).

import type { Socket } from "node:net";
import { connectTcp } from "./tcp-connect.js";

export type LogLevel = "debug" | "info" | "warning" | "error";

export interface LogRecord {
  event: string;
  level: LogLevel;
  logger: string;
  timestamp: string;
  [key: string]: unknown;
}

const DEFAULT_LOGGER_NAME = "ts-sdk";

const CLOSE_FLUSH_TIMEOUT_MS = 3_000;

interface LogChannelState {
  sock: Socket;
  connected: boolean;
  closed: boolean;
}

export class LogChannel {
  private readonly shared: LogChannelState;
  private readonly name: string;
  private readonly isRoot: boolean;

  private constructor(shared: LogChannelState, name: string, isRoot: boolean) {
    this.shared = shared;
    this.name = name;
    this.isRoot = isRoot;
  }

  static async connect(addr: string, name: string = DEFAULT_LOGGER_NAME): Promise<LogChannel> {
    const shared: LogChannelState = {
      sock: await connectTcp(addr),
      connected: true,
      closed: false,
    };
    shared.sock.on("error", (err) => {
      shared.connected = false;
      process.stderr.write(`[${name}] log socket error: ${err.message}\n`);
    });
    shared.sock.on("close", () => {
      if (shared.closed || !shared.connected) return;
      shared.connected = false;
      process.stderr.write(`[${name}] log socket closed unexpectedly; further logs go to stderr\n`);
    });
    return new LogChannel(shared, name, true);
  }

  /** Create a sibling logger that shares the underlying socket but
   *  carries a hierarchical name (`parent.suffix`). Only the root
   *  owns the socket — children's `close()` is a no-op. */
  child(suffix: string): LogChannel {
    return new LogChannel(this.shared, `${this.name}.${suffix}`, false);
  }

  /** Name reported in the `logger` field of every record this
   *  instance emits. Useful for tests. */
  get loggerName(): string {
    return this.name;
  }

  send(
    record: Omit<LogRecord, "timestamp" | "logger"> & {
      timestamp?: string;
      logger?: string;
    },
  ): void {
    // Drop late records after the log socket has closed.
    if (this.shared.closed) return;
    // Prepend the logger name to the event message so it surfaces in
    // the Airflow UI task log view, which renders the message text but
    // hides the `logger` JSON field. The field is still emitted for
    // JSON consumers (grep/jq). Remove the prefix here if the
    // supervisor-side renderer ever starts showing the logger column.
    const line = JSON.stringify({
      logger: this.name,
      ...record,
      event: `[${this.name}] ${record.event}`,
      timestamp: record.timestamp ?? new Date().toISOString(),
    });
    const payload = line + "\n";
    if (!this.shared.connected || !this.shared.sock.writable) {
      process.stderr.write(payload);
      return;
    }
    this.shared.sock.write(Buffer.from(payload, "utf8"));
  }

  debug(event: string, args: Record<string, unknown> = {}): void {
    this.send({ event, level: "debug", ...args });
  }

  info(event: string, args: Record<string, unknown> = {}): void {
    this.send({ event, level: "info", ...args });
  }

  warning(event: string, args: Record<string, unknown> = {}): void {
    this.send({ event, level: "warning", ...args });
  }

  error(event: string, args: Record<string, unknown> = {}): void {
    this.send({ event, level: "error", ...args });
  }

  async close(): Promise<void> {
    if (!this.isRoot) return;
    this.shared.closed = true;
    if (!this.shared.connected) {
      this.shared.sock.destroy();
      return;
    }
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.shared.sock.destroy();
        resolve();
      }, CLOSE_FLUSH_TIMEOUT_MS);
      timer.unref();
      this.shared.sock.end(() => {
        clearTimeout(timer);
        resolve();
      });
    });
  }
}
