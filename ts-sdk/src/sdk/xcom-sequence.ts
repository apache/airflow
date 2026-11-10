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

/**
 * Every result a task produced, read on demand rather than delivered whole. Each value is JSON, as
 * XCom stores it.
 *
 * What a task outside a loop receives for an input given as a loop task's `all()`: one value per
 * iteration, in iteration order. Nothing is fetched until it is asked for, as Python's
 * `LazyXComSequence` does, so a handler that only needs the last value reads one.
 *
 * ```ts
 * const report = dag.task(async function report({ history }: { history: XComSequence<Metrics> }) {
 *   const last = await history.at(-1);
 *   for await (const metrics of history) console.log(metrics.error);
 * });
 * report({ history: evaluated.all() });
 * ```
 */
export interface XComSequence<T> extends AsyncIterable<T> {
  /** How many values there are. */
  length(): Promise<number>;
  /**
   * The value at `index`, counting from 0; a negative index counts back from the end.
   *
   * @throws a `RangeError` when there is no value at `index`.
   */
  at(index: number): Promise<T>;
  /**
   * The values from `start` up to, not including, `stop`, as `Array.prototype.slice` takes them:
   * either may be negative, and an omitted `stop` reads to the end.
   */
  slice(start?: number, stop?: number): Promise<T[]>;
}
