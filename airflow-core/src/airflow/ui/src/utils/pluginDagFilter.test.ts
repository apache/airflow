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
import { describe, expect, it } from "vitest";

import type { DAGResponse, ExternalViewResponse } from "openapi/requests/types.gen";

import { matchesDagFilter } from "./pluginDagFilter";

const makeDag = (dagId: string, tagNames: Array<string>): DAGResponse =>
  ({
    dag_id: dagId,
    tags: tagNames.map((name) => ({ dag_display_name: dagId, dag_id: dagId, name })),
  }) as DAGResponse;

const makeView = (overrides: Partial<ExternalViewResponse>): ExternalViewResponse => ({
  destination: "dag_run",
  href: "/plugin/example",
  name: "Example",
  url_route: "example",
  ...overrides,
});

describe("matchesDagFilter", () => {
  const dag = makeDag("etl_sales", ["ml", "prod"]);

  it("shows the tab when no filter is set", () => {
    expect(matchesDagFilter(makeView({}), dag)).toBe(true);
  });

  it("ignores filters when the Dag is unavailable", () => {
    expect(matchesDagFilter(makeView({ dag_ids: ["other"] }), undefined)).toBe(true);
  });

  it("matches on an overlapping tag", () => {
    expect(matchesDagFilter(makeView({ dag_tags: ["ml"] }), dag)).toBe(true);
  });

  it("hides the tab when no tag overlaps", () => {
    expect(matchesDagFilter(makeView({ dag_tags: ["finance"] }), dag)).toBe(false);
  });

  it("matches on an exact dag_id in the list", () => {
    expect(matchesDagFilter(makeView({ dag_ids: ["etl_sales", "etl_orders"] }), dag)).toBe(true);
  });

  it("hides the tab when the dag_id is not in the list", () => {
    expect(matchesDagFilter(makeView({ dag_ids: ["etl_orders"] }), dag)).toBe(false);
  });

  it("matches on a glob pattern", () => {
    expect(matchesDagFilter(makeView({ dag_id_pattern: "etl_*" }), dag)).toBe(true);
  });

  it("treats glob special characters literally outside * and ?", () => {
    expect(matchesDagFilter(makeView({ dag_id_pattern: "etl.sales" }), dag)).toBe(false);
    expect(matchesDagFilter(makeView({ dag_id_pattern: "etl_sale?" }), dag)).toBe(true);
  });

  it("hides the tab when the glob pattern does not match", () => {
    expect(matchesDagFilter(makeView({ dag_id_pattern: "report_*" }), dag)).toBe(false);
  });

  it("combines filters with OR (matches when only one filter matches)", () => {
    const view = makeView({ dag_ids: ["etl_orders"], dag_tags: ["ml"] });

    expect(matchesDagFilter(view, dag)).toBe(true);
  });

  it("hides the tab when no configured filter matches", () => {
    const view = makeView({ dag_id_pattern: "report_*", dag_ids: ["etl_orders"], dag_tags: ["finance"] });

    expect(matchesDagFilter(view, dag)).toBe(false);
  });
});
