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

// Coordinator-scoped on purpose (like `tcp-connect.ts`): the only
// caller today is `comm-channel.ts`. It's a fully generic primitive —
// if something outside coordinator mode ever needs it, hoist then;
// don't pre-hoist for tidiness.

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

  resolve(v: T): void {
    if (!this.done) {
      this.done = true;
      this.res(v);
    }
  }

  reject(e: Error): void {
    if (!this.done) {
      this.done = true;
      this.rej(e);
    }
  }
}
