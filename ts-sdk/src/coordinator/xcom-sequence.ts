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

/** Internal: a sequence over the values `opts` names, fetched only when read. */
export class LazyXComSequence<T> implements XComSequence<T> {
  readonly #client: CoordinatorClient;
  readonly #opts: XComSequenceOpts;
  // The values cannot change while the reading task runs: every producer it reads has finished.
  #length: Promise<number> | undefined;

  constructor(client: CoordinatorClient, opts: XComSequenceOpts) {
    this.#client = client;
    this.#opts = opts;
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
