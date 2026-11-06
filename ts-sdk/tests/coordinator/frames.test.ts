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

import { describe, expect, it } from "vitest";
import {
  decodePayload,
  encodeRequest,
  encodeResponse,
  FrameReader,
  tryTakeFrame,
} from "../../src/coordinator/frames.js";

function stripFramePrefix(buf: Buffer): Buffer {
  return buf.subarray(4);
}

describe("frames", () => {
  it("round-trips a request frame", () => {
    const encoded = encodeRequest(7, { type: "GetVariable", key: "foo" });
    const prefixedLen = encoded.readUInt32BE(0);
    expect(encoded.length).toBe(4 + prefixedLen);
    const decoded = decodePayload(stripFramePrefix(encoded));
    expect(decoded.id).toBe(7);
    expect(decoded.body).toEqual({ type: "GetVariable", key: "foo" });
    expect(decoded.error).toBeUndefined();
    expect(decoded.isResponse).toBe(false);
  });

  it("round-trips a response frame with error", () => {
    const encoded = encodeResponse(3, null, { error: "NotFound", detail: "x" });
    const decoded = decodePayload(stripFramePrefix(encoded));
    expect(decoded.id).toBe(3);
    expect(decoded.body).toBeNull();
    expect(decoded.error).toEqual({ error: "NotFound", detail: "x" });
    expect(decoded.isResponse).toBe(true);
  });

  it("FrameReader stitches fragmented chunks", () => {
    const full = Buffer.concat([
      encodeRequest(0, { type: "A", n: 1 }),
      encodeRequest(1, { type: "B", n: 2 }),
      encodeRequest(2, { type: "C", n: 3 }),
    ]);
    const reader = new FrameReader();
    const split1 = full.subarray(0, 3);
    const firstLen = full.readUInt32BE(0);
    const split2 = full.subarray(3, 4 + firstLen);
    const split3 = full.subarray(4 + firstLen);
    const frames = [...reader.push(split1), ...reader.push(split2), ...reader.push(split3)];
    expect(frames.map((f) => f.id)).toEqual([0, 1, 2]);
    expect(reader.pending).toBe(0);
  });

  it("tryTakeFrame returns null when buffer is short", () => {
    const encoded = encodeRequest(0, { type: "X" });
    expect(tryTakeFrame(encoded.subarray(0, 3))).toBeNull();
    expect(tryTakeFrame(encoded.subarray(0, 5))).toBeNull();
    expect(tryTakeFrame(encoded)).not.toBeNull();
  });

  it("rejects non-array payloads", () => {
    const bogus = Buffer.from([0x2a]); // msgpack fixint 42
    expect(() => decodePayload(bogus)).toThrow(/array frame/);
  });
});
