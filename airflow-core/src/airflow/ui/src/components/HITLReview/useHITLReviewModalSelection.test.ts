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
import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { HITLDetail } from "openapi/requests/types.gen.ts";

import { useHITLReviewModalSelection } from "./useHITLReviewModalSelection";

const hitl = (id: string) =>
  ({
    task_instance: { id },
  }) as HITLDetail;

describe("useHITLReviewModalSelection", () => {
  it("does not select a HITL detail when the list is empty", () => {
    const { result } = renderHook(() => useHITLReviewModalSelection({ hitlDetails: [] }));

    expect(result.current.selectedDetail).toBeUndefined();
  });

  it("selects the first HITL detail by default", () => {
    const details = [hitl("first"), hitl("second")];

    const { result } = renderHook(() => useHITLReviewModalSelection({ hitlDetails: details }));

    expect(result.current.selectedDetail).toBe(details[0]);
  });

  it("moves selection forward", () => {
    const details = [hitl("first"), hitl("second")];

    const { result } = renderHook(() => useHITLReviewModalSelection({ hitlDetails: details }));

    act(() => result.current.onNext());
    expect(result.current.selectedDetail).toBe(details[1]);
  });

  it("falls back to the first HITL detail when the selected key is no longer visible", () => {
    const first = hitl("first");
    const second = hitl("second");
    const details = [first, second];

    const { rerender, result } = renderHook(
      ({ hitlDetails }: { readonly hitlDetails: Array<HITLDetail> }) =>
        useHITLReviewModalSelection({ hitlDetails }),
      { initialProps: { hitlDetails: details } },
    );

    act(() => result.current.onNext());
    expect(result.current.selectedDetail).toBe(second);

    rerender({ hitlDetails: [first] });
    expect(result.current.selectedDetail).toBe(first);
  });
});
