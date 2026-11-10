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

// The runtime side of `XComSequence`: every read is a supervisor call, and the
// server decides which producer rows are live and how they are ordered.

import type { CoordinatorClient, XComSequenceOpts } from "./client.js";
import type { XComSequence } from "../sdk/xcom-sequence.js";

/** The clients that handed a task a sequence, so a push from any other skips looking for one. */
const clientsWithSequences = new WeakSet<CoordinatorClient>();

/** Internal: a sequence over the values `opts` names, fetched only when read. */
export class LazyXComSequence<T> implements XComSequence<T> {
  readonly #client: CoordinatorClient;
  readonly #opts: XComSequenceOpts;
  // The values cannot change while the reading task runs: every producer it reads has finished.
  #length: Promise<number> | undefined;

  constructor(client: CoordinatorClient, opts: XComSequenceOpts) {
    this.#client = client;
    this.#opts = opts;
    clientsWithSequences.add(client);
  }

  length(): Promise<number> {
    this.#length ??= this.#client.getXComCount(this.#opts).catch((err: unknown) => {
      // A failed count is not remembered, so a later call asks again.
      this.#length = undefined;
      throw err;
    });
    return this.#length;
  }

  async at(index: number): Promise<T> {
    if (!Number.isSafeInteger(index)) {
      throw new RangeError(`An XComSequence index is an integer, not ${String(index)}`);
    }
    const entry = await this.#client.getXComSequenceItem({ ...this.#opts, offset: index });
    if (!entry.found) {
      throw new RangeError(
        `XComSequence of task "${this.#opts.taskId}" has no value at index ${index}`,
      );
    }
    return entry.value as T;
  }

  async slice(start?: number, stop?: number): Promise<T[]> {
    for (const [name, bound] of [
      ["start", start],
      ["stop", stop],
    ] as const) {
      if (bound !== undefined && !Number.isSafeInteger(bound)) {
        throw new RangeError(`An XComSequence slice ${name} is an integer, not ${String(bound)}`);
      }
    }
    return (await this.#client.getXComSequenceSlice({
      ...this.#opts,
      ...(start !== undefined && { start }),
      ...(stop !== undefined && { stop }),
    })) as T[];
  }

  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    const length = await this.length();
    for (let index = 0; index < length; index += 1) {
      yield await this.at(index);
    }
  }
}

/**
 * `value` with every `XComSequence` in it read into an array, as Python's serde reads a
 * `LazyXComSequence` into a list when it is pushed. Pushed as it is, a sequence has no fields of its
 * own to send. Each sequence is read once, with one slice.
 */
export async function materializeXComSequences(value: unknown): Promise<unknown> {
  return materialize(value, new Map(), new Set());
}

async function materialize(
  value: unknown,
  read: Map<LazyXComSequence<unknown>, Promise<unknown[]>>,
  ancestors: Set<object>,
): Promise<unknown> {
  if (value instanceof LazyXComSequence) {
    let values = read.get(value);
    if (values === undefined) read.set(value, (values = value.slice()));
    return values;
  }
  if (typeof value !== "object" || value === null) return value;
  if (!Array.isArray(value) && !isPlainObject(value)) return value;
  if (ancestors.has(value)) throw new TypeError("An XCom value cannot contain itself");
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return await Promise.all(value.map((item) => materialize(item, read, ancestors)));
    }
    const entries = await Promise.all(
      Object.entries(value).map(async ([key, item]) => [
        key,
        await materialize(item, read, ancestors),
      ]),
    );
    return Object.fromEntries(entries);
  } finally {
    ancestors.delete(value);
  }
}

/**
 * Whether `value` holds a sequence `client` handed out, in an array or plain object. A client that
 * handed out none is not searched.
 */
export function holdsXComSequence(client: CoordinatorClient, value: unknown): boolean {
  return clientsWithSequences.has(client) && findXComSequence(value, new Set());
}

function findXComSequence(value: unknown, seen: Set<object>): boolean {
  if (value instanceof LazyXComSequence) return true;
  if (typeof value !== "object" || value === null || seen.has(value)) return false;
  seen.add(value);
  const items = Array.isArray(value) ? value : isPlainObject(value) ? Object.values(value) : [];
  return items.some((item) => findXComSequence(item, seen));
}

function isPlainObject(value: object): boolean {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
