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

// Coordinator-scoped (like `tcp-connect.ts`): its only caller is
// `comm-channel.ts`, so it lives next to it rather than in a shared util.

/**
 * A promise whose settlement is triggered from *outside* its executor
 * (the classic "deferred"). Use it when the producer and consumer of a
 * one-time value are decoupled in time and code location — e.g. a value
 * that arrives on a socket but is awaited elsewhere.
 *
 * Owns the settle-at-most-once invariant: `resolve`/`reject` after the
 * first settlement are no-ops, so callers never hand-maintain that rule.
 */
export class Deferred<T> {
  readonly promise: Promise<T>;
  private done = false;
  private res!: (v: T) => void;
  private rej!: (e: Error) => void;
  private readonly onSettleFns: Array<() => void> = [];

  constructor() {
    this.promise = new Promise<T>((res, rej) => {
      this.res = res;
      this.rej = rej;
    });
  }

  /** Whether `resolve`/`reject` has fired — for callers that need to
   *  branch on it. Not needed to guard `resolve`/`reject`; those are
   *  already idempotent. */
  get settled(): boolean {
    return this.done;
  }

  /** Run `fn` once, when this settles either way — the deferred's
   *  `finally`. Runs immediately if already settled. Returns `this` so a
   *  timeout / cleanup can be attached fluently at construction. */
  onSettle(fn: () => void): this {
    if (this.done) {
      fn();
    } else {
      this.onSettleFns.push(fn);
    }
    return this;
  }

  /** Reject with `makeError()` after `ms`, unless already settled. The
   *  timer auto-clears on settle, so a fulfilled deferred never fires it. */
  rejectAfter(ms: number, makeError: () => Error): this {
    const timer = setTimeout(() => this.reject(makeError()), ms);
    return this.onSettle(() => clearTimeout(timer));
  }

  resolve(v: T): void {
    this.settle(() => this.res(v));
  }

  reject(e: Error): void {
    this.settle(() => this.rej(e));
  }

  private settle(apply: () => void): void {
    if (this.done) return;
    this.done = true;
    try {
      apply();
    } finally {
      for (const fn of this.onSettleFns) {
        fn();
      }
      this.onSettleFns.length = 0;
    }
  }
}
