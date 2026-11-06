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

// Task SDK wire frame codec.
//
// Mirrors the Airflow supervisor's length-prefixed msgpack IPC format.
//
// Frame format on the wire:
//
//     +---------+----------------------------+
//     | len (4) | msgpack payload (variable) |
//     +---------+----------------------------+
//
// `len` is big-endian uint32 — the byte length of the msgpack payload.
// Payload is a msgpack array:
//   - Request:  [id: int, body: map]              (arity 2)
//   - Response: [id: int, body: map, error: map?] (arity 3)
//
// The body is a map with a `type` key naming the message. Both
// supervisor and runtime maintain independent id counters starting
// at 0. Routing uses the pending-request map (not arity) — if the
// id matches a request we sent, it's the response; otherwise it's
// supervisor-initiated. See `comm-channel.ts`.

import { encode, decode } from "@msgpack/msgpack";

/**
 * Maximum frame payload size in bytes (2^32 − 1). The length prefix is a
 * big-endian uint32, so anything larger would silently truncate.
 */
export const MAX_FRAME_SIZE = 0xffff_ffff;

export interface Frame {
  id: number;
  body: unknown;
  error?: unknown;
  /** Whether the frame's msgpack array was arity 3 (response) or 2 (request). */
  isResponse: boolean;
}

export function encodeRequest(id: number, body: unknown): Buffer {
  return encodeFrame(id, body, undefined, /* isResponse */ false);
}

export function encodeResponse(id: number, body?: unknown, error?: unknown): Buffer {
  return encodeFrame(id, body, error, /* isResponse */ true);
}

function encodeFrame(id: number, body: unknown, error: unknown, isResponse: boolean): Buffer {
  const array = isResponse ? [id, body ?? null, error ?? null] : [id, body ?? null];
  const payload = Buffer.from(encode(array));
  if (payload.length > MAX_FRAME_SIZE) {
    throw new RangeError(
      `Frame payload ${payload.length} bytes exceeds MAX_FRAME_SIZE (${MAX_FRAME_SIZE})`,
    );
  }
  const framed = Buffer.alloc(4 + payload.length);
  framed.writeUInt32BE(payload.length, 0);
  payload.copy(framed, 4);
  return framed;
}

/** Decode a single framed payload (length prefix already stripped). */
export function decodePayload(payload: Buffer): Frame {
  const value = decode(payload);
  if (!Array.isArray(value)) {
    throw new Error(`Expected msgpack array frame, got ${typeof value}`);
  }
  const arity = value.length;
  if (arity < 2) {
    throw new Error(`Unexpected Task SDK frame arity ${arity}`);
  }
  const [id, body, error] = value as [number, unknown, unknown?];
  if (typeof id !== "number") {
    throw new Error(`Frame id must be number, got ${typeof id}`);
  }
  // Specific per-failure messages above are deliberate — this is a
  // cross-language wire boundary and a vague decode error there costs
  // hours. Construction itself is a single expression: error is
  // omitted (not set to null) when absent so `"error" in frame`
  // stays a faithful arity signal.
  return {
    id,
    body,
    isResponse: arity >= 3,
    ...(error != null ? { error } : {}),
  };
}

/**
 * Try to consume one full frame (length prefix + payload) from `buf`.
 * Returns the frame plus any trailing bytes belonging to the next frame,
 * or `null` if `buf` doesn't yet contain a complete frame.
 */
export function tryTakeFrame(buf: Buffer): { frame: Frame; rest: Buffer } | null {
  if (buf.length < 4) return null;
  const len = buf.readUInt32BE(0);
  if (len > MAX_FRAME_SIZE) {
    throw new RangeError(
      `Incoming frame length ${len} bytes exceeds MAX_FRAME_SIZE (${MAX_FRAME_SIZE})`,
    );
  }
  if (buf.length < 4 + len) return null;
  const payload = buf.subarray(4, 4 + len);
  const rest = buf.subarray(4 + len);
  return {
    frame: decodePayload(Buffer.from(payload)),
    rest: Buffer.from(rest),
  };
}

/** Accumulate bytes across socket reads and emit complete frames. */
export class FrameReader {
  private buf: Buffer = Buffer.alloc(0);

  push(chunk: Buffer): Frame[] {
    this.buf = this.buf.length === 0 ? chunk : Buffer.concat([this.buf, chunk]);
    const frames: Frame[] = [];
    while (true) {
      const taken = tryTakeFrame(this.buf);
      if (!taken) break;
      frames.push(taken.frame);
      this.buf = taken.rest;
    }
    return frames;
  }

  get pending(): number {
    return this.buf.length;
  }
}
