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

// Comm socket client — length-prefixed msgpack frames over TCP.
// Mirrors the Airflow supervisor's comm socket protocol.
//
// The channel is the sole reader on the socket. The task sends
// requests and awaits id-correlated replies. The supervisor's only
// unprompted frame is the greeting (StartupDetails /
// DagFileParseRequest), pre-caught into the `greeting` promise that
// `connect()` awaits — the protocol sends nothing else
// supervisor-initiated (comms.py: "No messages are sent to task
// process except in response to a request").

import type { Socket } from "node:net";
import { encodeRequest, encodeResponse, FrameReader, type Frame } from "./frames.js";
import { connectTcp } from "./tcp-connect.js";
import { Deferred } from "./deferred.js";
import type { LogChannel } from "./log-channel.js";

/** What `CommChannel.connect` resolves to: the live channel plus the
 *  supervisor's first frame (StartupDetails / DagFileParseRequest),
 *  already in hand so the caller never has to manage a "frame arrived
 *  with no consumer" window. */
export interface CommConnection {
  channel: CommChannel;
  firstFrame: Frame;
}

export interface SendResponseOptions {
  timeoutMs?: number;
}

export interface RequestOptions {
  timeoutMs?: number;
}

export const COORDINATOR_REQUEST_TIMEOUT_MS = 30_000;

export class CommChannel {
  private readonly sock: Socket;
  private readonly reader = new FrameReader();
  private readonly logs: LogChannel | null;
  private nextId = 0;
  private pendingReplies = new Map<number, (frame: Frame) => void>();
  private closed = false;
  private closeError: Error | null = null;

  // The greeting (first supervisor-initiated frame). The promise is
  // its own buffer: arriving before `connect()` awaits is fine — it
  // stays settled with the value, so there is no race to handle.
  private readonly greeting = new Deferred<Frame>();

  private constructor(sock: Socket, logs: LogChannel | null) {
    this.sock = sock;
    this.logs = logs;
    // A `new Socket()` (from `connectTcp`) starts paused: it buffers
    // inbound bytes and emits no `data` until a listener attaches
    // and flips it to flowing. Attaching synchronously here — same
    // tick as construction, before the event loop can deliver a
    // read, and as the only reader — loses nothing, double-reads
    // nothing.
    sock.on("data", (chunk) => this.handleData(chunk));
    sock.on("close", () => this.handleClose(null));
    sock.on("error", (err) => this.handleClose(err));
  }

  /** Connect and wait for the supervisor's greeting; rejects if the
   *  socket dies before it arrives. */
  static async connect(addr: string, logs: LogChannel | null = null): Promise<CommConnection> {
    const sock = await connectTcp(addr);
    const channel = new CommChannel(sock, logs);
    const firstFrame = await channel.greeting.promise;
    return { channel, firstFrame };
  }

  /** Send a request to the supervisor and await its matching response. */
  async request(body: unknown, opts: RequestOptions = {}): Promise<Frame> {
    if (this.closed) {
      throw this.closeError ?? new Error("Comm channel closed");
    }

    const id = this.nextId++;
    const type = describeFrameType(body);
    const timeoutMs = opts.timeoutMs ?? COORDINATOR_REQUEST_TIMEOUT_MS;

    // A reply that times out on its own and removes its pending entry once done.
    const reply = new Deferred<Frame>()
      .rejectAfter(
        timeoutMs,
        () => new Error(`Timed out waiting for ${type} response after ${timeoutMs} ms`),
      )
      .onSettle(() => this.pendingReplies.delete(id));

    // How the reply gets fulfilled: the reader matches this id and calls us.
    this.pendingReplies.set(id, (frame) => {
      this.logs?.debug("Response received", {
        id,
        request_type: type,
        response_type: describeFrameType(frame.body),
        error: frame.error ?? null,
      });
      reply.resolve(frame);
    });

    this.logs?.debug("Sending request", { id, type });
    try {
      this.sock.write(encodeRequest(id, body), (err) => {
        if (err) reply.reject(err);
      });
    } catch (err) {
      reply.reject(err as Error);
    }

    return reply.promise;
  }

  /** Send a response for an incoming supervisor request. */
  async sendResponse(
    id: number,
    body: unknown,
    error?: unknown,
    opts: SendResponseOptions = {},
  ): Promise<void> {
    this.logs?.debug("Sending response", {
      id,
      type: describeFrameType(body),
      error: error ?? null,
    });
    if (this.closed) {
      throw this.closeError ?? new Error("Comm channel closed");
    }

    const done = new Deferred<void>();
    if (opts.timeoutMs !== undefined) {
      // A wedged terminal write must not hang the process: destroy the socket
      // and fail so the runtime exits non-zero instead of waiting forever.
      done.rejectAfter(opts.timeoutMs, () => {
        const err = new Error(`Timed out sending response after ${opts.timeoutMs} ms`);
        this.sock.destroy(err);
        return err;
      });
    }

    try {
      this.sock.write(encodeResponse(id, body, error), (err) =>
        err ? done.reject(err) : done.resolve(),
      );
    } catch (err) {
      done.reject(err as Error);
    }

    return done.promise;
  }

  async close(): Promise<void> {
    return new Promise((resolve) => {
      if (this.closed) {
        resolve();
        return;
      }
      this.sock.end(() => resolve());
    });
  }

  // -- internals --

  private handleData(chunk: Buffer): void {
    let frames: Frame[];
    try {
      frames = this.reader.push(chunk);
    } catch (err) {
      // Frame decode failure — protocol violation or socket
      // corruption. Surface it so it's not a silent dropped chunk.
      this.logs?.error("Frame decode failed", {
        error: (err as Error).message ?? String(err),
        pending_bytes: this.reader.pending,
      });
      this.handleClose(err as Error);
      return;
    }
    for (const frame of frames) {
      this.logs?.debug("Handling frame", { id: frame.id });
      this.route(frame);
    }
  }

  private route(frame: Frame): void {
    // Route by pending-request lookup, not frame arity. If the id
    // matches a request we sent, it's the response. Otherwise it's
    // supervisor-initiated (the greeting). This works because the
    // greeting always arrives before any request is sent, so id=0
    // can never collide with a pending request.
    const pending = this.pendingReplies.get(frame.id);
    if (pending) {
      this.pendingReplies.delete(frame.id);
      pending(frame);
      return;
    }
    this.deliverSupervisorFrame(frame);
  }

  private deliverSupervisorFrame(frame: Frame): void {
    // The supervisor's only unprompted frame is the greeting.
    if (!this.greeting.settled) {
      this.greeting.resolve(frame);
      return;
    }
    // Anything else supervisor-initiated is a protocol anomaly —
    // comms.py guarantees "No messages are sent to task process
    // except in response to a request". Surface it; never buffer.
    this.logs?.error("Unexpected supervisor-initiated frame after greeting", {
      id: frame.id,
      type: describeFrameType(frame.body),
    });
  }

  private handleClose(err: Error | null): void {
    if (this.closed) return;
    this.closed = true;
    this.closeError = err;
    if (err) {
      this.logs?.warning("Comm channel closed with error", {
        error: err.message,
        pending_replies: this.pendingReplies.size,
      });
    }
    // Before the greeting this rejects so `connect()` throws;
    // after it, a no-op — the Deferred settles at most once, so no
    // guard is needed here.
    this.greeting.reject(err ?? new Error("Comm channel closed before first frame"));
    for (const [, resolver] of this.pendingReplies) {
      resolver({
        id: -1,
        body: null,
        error: err?.message ?? "closed",
        isResponse: true,
      });
    }
    this.pendingReplies.clear();
  }
}

function describeFrameType(body: unknown): string {
  if (body && typeof body === "object" && "type" in body) {
    const t = (body as { type?: unknown }).type;
    if (typeof t === "string") return t;
  }
  return "unknown";
}
