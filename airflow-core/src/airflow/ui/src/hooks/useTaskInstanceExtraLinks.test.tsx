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
import { beforeEach, describe, expect, it, vi } from "vitest";

import * as queries from "openapi/queries";

import { BaseWrapper } from "src/utils/Wrapper";

import { useTaskInstanceExtraLinks } from "./useTaskInstanceExtraLinks";

vi.mock("openapi/queries");

const params = {
  dagId: "example_dag",
  dagRunId: "example_run",
  taskId: "example_task",
};

describe("useTaskInstanceExtraLinks", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(queries.useTaskInstanceServiceGetExtraLinks).mockReturnValue({
      data: { extra_links: {} },
    } as ReturnType<typeof queries.useTaskInstanceServiceGetExtraLinks>);
  });

  it("passes the task instance coordinates and enabled flag to the query", () => {
    renderHook(
      () =>
        useTaskInstanceExtraLinks({
          ...params,
          enabled: true,
          mapIndex: 3,
          regionId: "region",
          regionIndex: 2,
          tryNumber: 4,
        }),
      { wrapper: BaseWrapper },
    );

    expect(queries.useTaskInstanceServiceGetExtraLinks).toHaveBeenCalledWith(
      {
        ...params,
        mapIndex: 3,
        regionId: "region",
        regionIndex: 2,
        tryNumber: 4,
      },
      undefined,
      { enabled: true },
    );
  });

  it("returns only safe links with a value", () => {
    vi.mocked(queries.useTaskInstanceServiceGetExtraLinks).mockReturnValue({
      data: {
        extra_links: {
          "Missing URL": null,
          "Safe Link": "https://example.com/logs",
          "Unsafe Link": "javascript:alert(1)",
        },
      },
    } as unknown as ReturnType<typeof queries.useTaskInstanceServiceGetExtraLinks>);

    const { result } = renderHook(() => useTaskInstanceExtraLinks(params), { wrapper: BaseWrapper });

    expect(result.current).toEqual([{ name: "Safe Link", url: "https://example.com/logs" }]);
  });

  it("returns no links while the query has no data", () => {
    vi.mocked(queries.useTaskInstanceServiceGetExtraLinks).mockReturnValue(
      {} as ReturnType<typeof queries.useTaskInstanceServiceGetExtraLinks>,
    );

    const { result } = renderHook(() => useTaskInstanceExtraLinks(params), { wrapper: BaseWrapper });

    expect(result.current).toEqual([]);
  });
});
