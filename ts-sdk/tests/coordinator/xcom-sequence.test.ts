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

import { describe, expect, it, vi } from "vitest";

import type { CoordinatorClient } from "../../src/coordinator/client.js";
import {
  holdsXComSequence,
  LazyXComSequence,
  materializeXComSequences,
} from "../../src/coordinator/xcom-sequence.js";

function makeSequence(values: unknown[]) {
  const getXComSequenceSlice = vi.fn(async () => values);
  const client = { getXComSequenceSlice } as unknown as CoordinatorClient;
  const sequence = new LazyXComSequence(client, { taskId: "t" } as never);
  return { client, sequence, getXComSequenceSlice };
}

describe("materializeXComSequences", () => {
  it("reads each sequence in arrays and plain objects once", async () => {
    const { sequence, getXComSequenceSlice } = makeSequence([1, 2]);

    expect(await materializeXComSequences({ runs: [sequence, { again: sequence }], n: 3 })).toEqual(
      { runs: [[1, 2], { again: [1, 2] }], n: 3 },
    );
    expect(getXComSequenceSlice).toHaveBeenCalledTimes(1);
  });

  it("refuses a value that contains itself", async () => {
    const { sequence } = makeSequence([]);
    const value: Record<string, unknown> = { sequence };
    value["self"] = value;

    await expect(materializeXComSequences(value)).rejects.toThrow(
      "An XCom value cannot contain itself",
    );
  });
});

describe("holdsXComSequence", () => {
  it("finds a nested sequence, and ends on a self-referencing value without one", () => {
    const { client, sequence } = makeSequence([]);
    const value: Record<string, unknown> = { rows: [1, 2, 3] };
    value["self"] = value;

    expect(holdsXComSequence(client, { deep: [{ sequence }] })).toBe(true);
    expect(holdsXComSequence(client, value)).toBe(false);
  });

  it("does not search a value pushed through a client that handed out no sequence", () => {
    const { sequence } = makeSequence([]);

    expect(holdsXComSequence({} as CoordinatorClient, { sequence })).toBe(false);
  });
});
