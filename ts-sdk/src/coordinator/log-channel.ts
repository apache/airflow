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

// Level names match Airflow's Python parser: structlog's NAME_TO_LEVEL plus the `fatal` alias
// that configure_logging adds.
const LOG_LEVELS = new Map([
  ["NOTSET", 0],
  ["DEBUG", 10],
  ["INFO", 20],
  ["WARN", 30],
  ["WARNING", 30],
  ["ERROR", 40],
  ["EXCEPTION", 40],
  ["CRITICAL", 50],
  ["FATAL", 50],
]);

const DEFAULT_LOG_LEVEL = LOG_LEVELS.get("INFO")!;

function parseLogLevel(value: string | undefined): number | undefined {
  return LOG_LEVELS.get(value?.trim().toUpperCase() ?? "");
}

/** Parse `namespace_levels`: `<logger>=<level>` pairs separated by whitespace and/or commas.
 *  Later duplicates win; malformed entries are returned so the caller can report them. */
function parseNamespaceLevels(value: string | undefined): {
  levels: Map<string, number>;
  invalid: string[];
} {
  const levels = new Map<string, number>();
  const invalid: string[] = [];
  for (const entry of (value ?? "").split(/[\s,]+/).filter(Boolean)) {
    const separator = entry.indexOf("=");
    const level = parseLogLevel(entry.slice(separator + 1));
    if (separator <= 0 || level === undefined) {
      invalid.push(entry);
    } else {
      levels.set(entry.slice(0, separator), level);
    }
  }
  return { levels, invalid };
}

/** Threshold for `name`: the most specific configured dotted ancestor, else the global level. */
function resolveLogLevel(shared: LogChannelState, name: string): number {
  let namespace = name;
  while (namespace) {
    const level = shared.namespaceLevels.get(namespace);
    if (level !== undefined) return level;
    const separator = namespace.lastIndexOf(".");
    if (separator < 0) break;
    namespace = namespace.slice(0, separator);
  }
  return shared.level;
}

interface LogChannelState {
  sock: Socket | null;
  connected: boolean;
  closed: boolean;
  buffer: string[];
  level: number;
  namespaceLevels: ReadonlyMap<string, number>;
}

export class LogChannel {
  private readonly shared: LogChannelState;
  private readonly name: string;
  private readonly isRoot: boolean;
  private readonly logLevel: number;

  private constructor(shared: LogChannelState, name: string, isRoot: boolean) {
    this.shared = shared;
    this.name = name;
    this.isRoot = isRoot;
    this.logLevel = resolveLogLevel(shared, name);
  }

  /** Create a root channel with no socket yet; records are buffered
   *  until {@link LogChannel#connect} flushes them. */
  static createBuffered(name: string = DEFAULT_LOGGER_NAME): LogChannel {
    const rawLevel = process.env["AIRFLOW__LOGGING__LOGGING_LEVEL"];
    const level = parseLogLevel(rawLevel);
    const namespaceLevels = parseNamespaceLevels(process.env["AIRFLOW__LOGGING__NAMESPACE_LEVELS"]);
    const root = new LogChannel(
      {
        sock: null,
        connected: false,
        closed: false,
        buffer: [],
        level: level ?? DEFAULT_LOG_LEVEL,
        namespaceLevels: namespaceLevels.levels,
      },
      name,
      true,
    );
    if (level === undefined && rawLevel?.trim()) {
      root.error("Ignoring invalid logging_level; using INFO", { logging_level: rawLevel });
    }
    for (const entry of namespaceLevels.invalid) {
      root.error("Ignoring invalid namespace_levels entry", { entry });
    }
    return root;
  }

  /** Connect the socket and flush buffered records, in order. Root only,
   *  and callable once: a second call would orphan the first socket. */
  async connect(addr: string): Promise<void> {
    if (!this.isRoot) {
      throw new Error(`[${this.name}] connect() is root-only`);
    }
    if (this.shared.sock) {
      throw new Error(`[${this.name}] connect() called more than once`);
    }
    const shared = this.shared;
    const name = this.name;
    const sock = await connectTcp(addr);
    shared.sock = sock;
    shared.connected = true;
    sock.on("error", (err) => {
      shared.connected = false;
      process.stderr.write(`[${name}] log socket error: ${err.message}\n`);
    });
    sock.on("close", () => {
      if (shared.closed || !shared.connected) return;
      shared.connected = false;
      process.stderr.write(`[${name}] log socket closed unexpectedly; further logs go to stderr\n`);
    });
    for (const line of shared.buffer) {
      sock.write(Buffer.from(line, "utf8"));
    }
    shared.buffer = [];
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
    record: Pick<LogRecord, "event" | "level"> & {
      timestamp?: string;
      logger?: string;
      [key: string]: unknown;
    },
  ): void {
    // Drop late records after the log socket has closed.
    if (this.shared.closed) return;
    const level =
      typeof record.level === "string" ? LOG_LEVELS.get(record.level.toUpperCase()) : undefined;
    if ((level ?? DEFAULT_LOG_LEVEL) < this.logLevel) return;
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
    const sock = this.shared.sock;
    if (!sock) {
      this.shared.buffer.push(payload);
      return;
    }
    if (!this.shared.connected || !sock.writable) {
      process.stderr.write(payload);
      return;
    }
    sock.write(Buffer.from(payload, "utf8"));
  }

  debug(event: string, args: Record<string, unknown> = {}): void {
    this.send({ event, ...args, level: "debug" });
  }

  info(event: string, args: Record<string, unknown> = {}): void {
    this.send({ event, ...args, level: "info" });
  }

  warning(event: string, args: Record<string, unknown> = {}): void {
    this.send({ event, ...args, level: "warning" });
  }

  error(event: string, args: Record<string, unknown> = {}): void {
    this.send({ event, ...args, level: "error" });
  }

  async close(): Promise<void> {
    if (!this.isRoot) return;
    this.shared.closed = true;
    const sock = this.shared.sock;
    if (!sock) {
      // never connected: surface buffered records instead of dropping them
      for (const line of this.shared.buffer) {
        process.stderr.write(line);
      }
      this.shared.buffer = [];
      return;
    }
    if (!this.shared.connected) {
      sock.destroy();
      return;
    }
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        sock.destroy();
        resolve();
      }, CLOSE_FLUSH_TIMEOUT_MS);
      timer.unref();
      sock.end(() => {
        clearTimeout(timer);
        resolve();
      });
    });
  }
}
