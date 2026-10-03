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
import { createElement, Fragment } from "react";

import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import type { TFunction } from "i18next";
import { describe, expect, it } from "vitest";

import type { StructuredLogMessage, TaskInstanceResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { getLogLineText, parseLogs } from "./useLogs";

const translate = ((key: string) => key) as unknown as TFunction;

describe("getLogLineText", () => {
  it("renders a structured line with timestamp and level, stripping ANSI codes", () => {
    const logMessage = {
      event: "\u001B[31mfailed\u001B[0m to run",
      level: "error",
      timestamp: "2026-01-01T00:00:00Z",
    } as StructuredLogMessage;

    expect(getLogLineText({ logMessage, showSource: false, showTimestamp: true, translate })).toBe(
      "[2026-01-01T00:00:00Z] ERROR - failed to run",
    );
  });

  it("omits the timestamp when showTimestamp is false", () => {
    const logMessage = {
      event: "task done",
      level: "info",
      timestamp: "2026-01-01T00:00:00Z",
    } as StructuredLogMessage;

    expect(getLogLineText({ logMessage, showSource: false, showTimestamp: false, translate })).toBe(
      "INFO - task done",
    );
  });

  it("omits the level when showLogLevel is false", () => {
    const logMessage = {
      event: "task done",
      level: "info",
      timestamp: "2026-01-01T00:00:00Z",
    } as StructuredLogMessage;

    expect(
      getLogLineText({ logMessage, showLogLevel: false, showSource: false, showTimestamp: true, translate }),
    ).toBe("[2026-01-01T00:00:00Z] task done");
  });

  it("strips ANSI codes from plain string lines", () => {
    expect(getLogLineText({ logMessage: "plain \u001B[32mok\u001B[0m line", translate })).toBe(
      "plain ok line",
    );
  });
});

describe("parseLogs line links", () => {
  const line = { event: "hello", level: "info", timestamp: "2026-01-01T00:00:00Z" } as StructuredLogMessage;
  const baseTaskInstance = {
    dag_id: "dag",
    dag_run_id: "run",
    map_index: -1,
    task_id: "task",
  };

  it.each([
    [
      "omits region coordinates for a task instance outside any region",
      {},
      "/dags/dag/runs/run/tasks/task/logs?try_number=1#0",
    ],
    [
      "carries the region coordinates of a loop or mapped execution",
      { region_id: "11111111-1111-4111-8111-111111111111", region_index: 2 },
      "/dags/dag/runs/run/tasks/task/logs?try_number=1&region_id=11111111-1111-4111-8111-111111111111&region_index=2#0",
    ],
  ])("%s", (_name, region, href) => {
    const { parsedLogs } = parseLogs({
      data: [line],
      taskInstance: { ...baseTaskInstance, ...region } as TaskInstanceResponse,
      translate,
      tryNumber: 1,
    });

    render(createElement(Fragment, undefined, parsedLogs?.[0]?.element), { wrapper: Wrapper });

    expect(screen.getByRole("link", { name: "0" })).toHaveAttribute("href", href);
  });
});
