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

/* global describe, expect, jest, test, moment */
import { act, renderHook } from "@testing-library/react";

import { RouterWrapper } from "src/utils/testUtils";
import type { DagRun, RunState } from "src/types";

declare global {
  namespace NodeJS {
    interface Global {
      defaultDagRunDisplayNumber: number;
      filtersOptions: {
        dagStates: RunState[];
        runTypes: DagRun["runType"][];
      };
    }
  }
}

const date = new Date();
date.setMilliseconds(0);
jest.useFakeTimers().setSystemTime(date);

// eslint-disable-next-line import/first
import useFilters, {
  FilterHookReturn,
  Filters,
  FilterTasksProps,
  UtilFunctions,
} from "./useFilters";

describe("Test useFilters hook", () => {
  test("Initial values when url does not have query params", async () => {
    const { result } = renderHook<FilterHookReturn, undefined>(
      () => useFilters(),
      { wrapper: RouterWrapper }
    );
    const {
      filters: {
        baseDate,
        numRuns,
        runType,
        runState,
        root,
        filterUpstream,
        filterDownstream,
      },
    } = result.current;

    expect(baseDate).toBe(date.toISOString());
    expect(numRuns).toBe(global.defaultDagRunDisplayNumber.toString());
    expect(runType).toEqual([]);
    expect(runState).toEqual([]);
    expect(root).toBeUndefined();
    expect(filterUpstream).toBeUndefined();
    expect(filterDownstream).toBeUndefined();
  });

  test.each([
    {
      fnName: "onBaseDateChange" as keyof UtilFunctions,
      paramName: "baseDate" as keyof Filters,
      // @ts-ignore
      paramValue: moment.utc().format(),
    },
    {
      fnName: "onNumRunsChange" as keyof UtilFunctions,
      paramName: "numRuns" as keyof Filters,
      paramValue: "10",
    },
    {
      fnName: "onRunTypeChange" as keyof UtilFunctions,
      paramName: "runType" as keyof Filters,
      paramValue: ["manual"],
    },
    {
      fnName: "onRunTypeChange" as keyof UtilFunctions,
      paramName: "runType" as keyof Filters,
      paramValue: ["manual", "backfill"],
    },
    {
      fnName: "onRunStateChange" as keyof UtilFunctions,
      paramName: "runState" as keyof Filters,
      paramValue: ["success"],
    },
    {
      fnName: "onRunStateChange" as keyof UtilFunctions,
      paramName: "runState" as keyof Filters,
      paramValue: ["success", "failed", "queued"],
    },
  ])("Test $fnName functions", async ({ fnName, paramName, paramValue }) => {
    const { result } = renderHook<FilterHookReturn, undefined>(
      () => useFilters(),
      { wrapper: RouterWrapper }
    );

    await act(async () => {
      result.current[fnName](
        paramValue as "string" & string[] & FilterTasksProps
      );
    });

    expect(result.current.filters[paramName]).toEqual(paramValue);

    // clearFilters
    await act(async () => {
      result.current.clearFilters();
    });

    if (paramName === "baseDate") {
      expect(result.current.filters[paramName]).toBe(date.toISOString());
    } else if (paramName === "numRuns") {
      expect(result.current.filters[paramName]).toBe(
        global.defaultDagRunDisplayNumber.toString()
      );
    } else {
      expect(result.current.filters[paramName]).toEqual([]);
    }
  });

  test("Test onFilterTasksChange ", async () => {
    const { result } = renderHook<FilterHookReturn, undefined>(
      () => useFilters(),
      { wrapper: RouterWrapper }
    );

    await act(async () => {
      result.current.onFilterTasksChange({
        root: "test",
        filterUpstream: true,
        filterDownstream: false,
      });
    });

    expect(result.current.filters.root).toBe("test");
    expect(result.current.filters.filterUpstream).toBe(true);
    expect(result.current.filters.filterDownstream).toBe(false);

    // sending same info clears filters
    await act(async () => {
      result.current.onFilterTasksChange({
        root: "test",
        filterUpstream: true,
        filterDownstream: false,
      });
    });

    expect(result.current.filters.root).toBeUndefined();
    expect(result.current.filters.filterUpstream).toBeUndefined();
    expect(result.current.filters.filterDownstream).toBeUndefined();

    await act(async () => {
      result.current.onFilterTasksChange({
        root: "test",
        filterUpstream: true,
        filterDownstream: false,
      });
    });

    expect(result.current.filters.root).toBe("test");
    expect(result.current.filters.filterUpstream).toBe(true);
    expect(result.current.filters.filterDownstream).toBe(false);

    // clearFilters
    await act(async () => {
      result.current.resetRoot();
    });

    expect(result.current.filters.root).toBeUndefined();
    expect(result.current.filters.filterUpstream).toBeUndefined();
    expect(result.current.filters.filterDownstream).toBeUndefined();
  });
});
