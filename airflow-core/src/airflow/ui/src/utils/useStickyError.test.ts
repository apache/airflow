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
import { renderHook } from "@testing-library/react";
import { describe, it, expect } from "vitest";

import { useStickyError } from "./useStickyError";

// The props are declared rather than inferred from the first render: a pass that reports no
// error is the whole point here, and inference from an initial Error would reject it.
type Props = { err: unknown; hasData: boolean; subject?: string };

const renderSticky = (initialProps: Props) =>
  renderHook<unknown, Props>(({ err, hasData, subject = "run_a" }) => useStickyError(err, hasData, subject), {
    initialProps,
  });

describe("useStickyError", () => {
  it("reports the error it is given", () => {
    const error = new Error("request failed");
    const { result } = renderSticky({ err: error, hasData: false });

    expect(result.current).toBe(error);
  });

  // The case the hook exists for: React Query clears the error and returns a query to pending
  // whenever it starts a fetch with no data to show meanwhile, so a request that never succeeds
  // loses the reason it failed on every refetch. Reading the error alone would flip the header
  // between the failure and its skeleton for as long as the page is open.
  it("holds the last failure while the next attempt is in flight", () => {
    const error = new Error("request failed");
    const { rerender, result } = renderSticky({ err: error, hasData: false });

    expect(result.current).toBe(error);

    rerender({ err: undefined, hasData: false });

    expect(result.current).toBe(error);
  });

  it("clears the held failure once data arrives", () => {
    const error = new Error("request failed");
    const { rerender, result } = renderSticky({ err: error, hasData: false });

    rerender({ err: undefined, hasData: true });

    expect(result.current).toBeUndefined();

    // And stays clear on a later pass that is merely pending again.
    rerender({ err: undefined, hasData: false });

    expect(result.current).toBeUndefined();
  });

  it("replaces a held failure with a newer one", () => {
    const first = new Error("first failure");
    const second = new Error("second failure");
    const { rerender, result } = renderSticky({ err: first, hasData: false });

    rerender({ err: undefined, hasData: false });
    rerender({ err: second, hasData: false });

    expect(result.current).toBe(second);
  });

  // Route elements stay mounted when only a param changes, so the hook outlives the run it was
  // holding a failure for.
  it("drops a held failure when the subject changes", () => {
    const error = new Error("run_a not found");
    const { rerender, result } = renderSticky({ err: error, hasData: false, subject: "run_a" });

    rerender({ err: undefined, hasData: false, subject: "run_b" });

    expect(result.current).toBeUndefined();
  });

  it("reports nothing when a query is merely pending and has never failed", () => {
    const { result } = renderSticky({ err: undefined, hasData: false });

    expect(result.current).toBeUndefined();
  });
});
