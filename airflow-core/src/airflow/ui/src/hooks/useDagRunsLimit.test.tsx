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
import type { PropsWithChildren } from "react";

import { act, renderHook } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { beforeEach, describe, expect, it } from "vitest";

import { dagRunsLimitKey } from "src/constants/localStorage";

import { useDagRunsLimit } from "./useDagRunsLimit";

const createWrapper =
  (initialEntry: string) =>
  ({ children }: PropsWithChildren) => (
    <MemoryRouter initialEntries={[initialEntry]}>{children}</MemoryRouter>
  );

const renderLimit = (dagId: string, initialEntry: string) =>
  renderHook(() => ({ ...useDagRunsLimit(dagId), search: useLocation().search }), {
    wrapper: createWrapper(initialEntry),
  });

describe("useDagRunsLimit", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("defaults to 10", () => {
    expect(renderLimit("my_dag", "/dags/my_dag").result.current.limit).toBe(10);
  });

  it("keeps the chosen limit after the URL param is dropped by navigation", () => {
    const first = renderLimit("my_dag", "/dags/my_dag?limit=50");

    expect(first.result.current.limit).toBe(50);

    act(() => first.result.current.setLimit(50));
    first.unmount();

    expect(renderLimit("my_dag", "/dags/my_dag/runs").result.current.limit).toBe(50);
  });

  it("stores the limit per Dag and clears the URL param", () => {
    const { result } = renderLimit("my_dag", "/dags/my_dag?limit=25&state=failed");

    act(() => result.current.setLimit(100));

    expect(result.current.limit).toBe(100);
    expect(result.current.search).toBe("?state=failed");
    expect(localStorage.getItem(dagRunsLimitKey("my_dag"))).toBe("100");
    expect(renderLimit("other_dag", "/dags/other_dag").result.current.limit).toBe(10);
  });

  it("lets the URL param override the remembered value", () => {
    localStorage.setItem(dagRunsLimitKey("my_dag"), "100");

    expect(renderLimit("my_dag", "/dags/my_dag?limit=25").result.current.limit).toBe(25);
  });
});
