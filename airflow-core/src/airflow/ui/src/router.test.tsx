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
import { matchRoutes } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { getTabPath } from "src/utils/links";

import { routerConfig } from "./router";

const getAdditionalPath = (pathname: string) => {
  const matches = matchRoutes(routerConfig, pathname) ?? [];

  return getTabPath(
    matches.map((match) => ({
      handle: "handle" in match.route ? match.route.handle : undefined,
      params: match.params,
    })),
    "dag",
  );
};

describe("Dag route handles", () => {
  it.each(["runs", "tasks", "calendar", "backfills", "events", "code", "details"])(
    "preserves the %s Dag tab",
    (tab) => {
      expect(getAdditionalPath(`/dags/example/${tab}`)).toBe(`/${tab}`);
    },
  );

  it("preserves the complete matched plugin route", () => {
    expect(getAdditionalPath("/dags/example/plugin/test/nested/detail/42")).toBe(
      "/plugin/test/nested/detail/42",
    );
  });

  it("preserves encoded reserved characters in a nested plugin route", () => {
    expect(getAdditionalPath("/dags/example/plugin/test/nested%3Fmode%3D1/section%23details")).toBe(
      "/plugin/test/nested%3Fmode%3D1/section%23details",
    );
  });

  it("does not double-encode named plugin params", () => {
    expect(getAdditionalPath("/dags/example/plugin/my%20plugin/details")).toBe("/plugin/my%20plugin/details");
  });

  it.each([
    "/assets/1",
    "/dags/example/required_actions",
    "/dags/example/backfills/12",
    "/dags/example/runs/run_1",
    "/dags/example/tasks/task_1",
    "/dags/example/tasks/group/group_1",
    "/dags/example/runs/run_1/tasks/task_1/details",
    "/dags/example/unknown",
  ])("does not preserve a tab from %s", (pathname) => {
    expect(getAdditionalPath(pathname)).toBe("");
  });
});
