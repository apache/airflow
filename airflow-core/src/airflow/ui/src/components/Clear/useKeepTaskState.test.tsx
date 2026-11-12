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
import { afterEach, describe, expect, it } from "vitest";

import { CLEAR_KEEP_TASK_STATE_KEY } from "src/constants/localStorage";

import { useKeepTaskState } from "./useKeepTaskState";

afterEach(() => {
  localStorage.clear();
});

describe("useKeepTaskState", () => {
  it.each([false, true])("resets to the stored default %s when the dialog reopens", (stored) => {
    localStorage.setItem(CLEAR_KEEP_TASK_STATE_KEY, JSON.stringify(stored));
    const { rerender, result } = renderHook(({ open }) => useKeepTaskState(open), {
      initialProps: { open: true },
    });

    expect(result.current[0]).toBe(stored);
    act(() => result.current[1](!stored));
    rerender({ open: false });
    rerender({ open: true });

    expect(result.current[0]).toBe(stored);
  });
});
