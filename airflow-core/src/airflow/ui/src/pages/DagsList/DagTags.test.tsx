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
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import type { DagTagResponse } from "openapi-gen/requests/types.gen";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { DagTags } from "./DagTags";

afterEach(cleanup);

const tags: Array<DagTagResponse> = [
  { dag_display_name: "etl_dag", dag_id: "etl_dag", name: "finance" },
  { dag_display_name: "etl_dag", dag_id: "etl_dag", name: "daily" },
];

const renderDagTags = (path: string) =>
  render(
    <BaseWrapper>
      <MemoryRouter initialEntries={[path]}>
        <DagTags tags={tags} />
      </MemoryRouter>
    </BaseWrapper>,
  );

const getLinkParams = (name: string) => {
  const href = screen.getByRole("link", { name }).getAttribute("href") ?? "";

  return [...new URL(href, "http://localhost").searchParams.entries()].sort(
    ([keyA, valueA], [keyB, valueB]) => keyA.localeCompare(keyB) || valueA.localeCompare(valueB),
  );
};

describe("DagTags in the Dags list", () => {
  it.each([
    {
      clicked: "finance",
      description: "keeps other filters when no tag is selected",
      expected: [
        ["owners", "alice"],
        ["paused", "false"],
        ["tags", "finance"],
      ],
      search: "?owners=alice&paused=false",
    },
    {
      clicked: "daily",
      description: "adds a tag to the selected tags",
      expected: [
        ["owners", "alice"],
        ["tags", "daily"],
        ["tags", "finance"],
      ],
      search: "?tags=finance&owners=alice",
    },
    {
      clicked: "daily",
      description: "keeps the match mode the user set",
      expected: [
        ["owners", "alice"],
        ["tags", "daily"],
        ["tags", "finance"],
        ["tags_match_mode", "all"],
      ],
      search: "?tags=finance&tags_match_mode=all&owners=alice",
    },
    {
      clicked: "daily",
      description: "removes a tag that is already selected",
      expected: [
        ["owners", "alice"],
        ["tags", "finance"],
        ["tags_match_mode", "all"],
      ],
      search: "?tags=finance&tags=daily&tags_match_mode=all&owners=alice",
    },
    {
      clicked: "finance",
      description: "drops the match mode once no tag remains",
      expected: [["owners", "alice"]],
      search: "?tags=finance&tags_match_mode=all&owners=alice",
    },
    {
      clicked: "finance",
      description: "goes back to the first page",
      expected: [
        ["owners", "alice"],
        ["tags", "finance"],
      ],
      search: "?owners=alice&offset=50",
    },
  ])("$description", ({ clicked, expected, search }) => {
    renderDagTags(`/dags${search}`);

    expect(getLinkParams(clicked)).toEqual(expected);
  });
});

describe("DagTags outside the Dags list", () => {
  it("links to the Dags list filtered by that tag only", () => {
    renderDagTags("/dags/etl_dag/runs?sort=-start_date&tags=daily");

    expect(screen.getByRole("link", { name: "finance" })).toHaveAttribute("href", "/dags?tags=finance");
  });
});
