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

import { TabEntity, TabName } from "src/constants/tab";

import { getTabPath } from "./tab";

const getDagMatches = (tab: TabName) => [{ handle: { entity: TabEntity.Dag, tab } }];

describe("getTabPath", () => {
  it.each(Object.values(TabName))("preserves the %s Dag tab", (tab) => {
    expect(getTabPath(getDagMatches(tab), TabEntity.Dag)).toBe(tab === TabName.Overview ? "" : `/${tab}`);
  });

  it("supports more than one compatible entity", () => {
    expect(
      getTabPath(
        [{ handle: { entity: TabEntity.Task, tab: TabName.Events } }],
        [TabEntity.Task, TabEntity.TaskInstance],
      ),
    ).toBe("/events");
  });

  it.each([
    { matches: [] },
    { matches: [{ handle: { entity: "asset", tab: "events" } }] },
    { matches: [{ handle: { entity: TabEntity.Task, tab: TabName.Details } }] },
    { matches: [{ handle: { entity: TabEntity.Dag } }] },
    { matches: [{ handle: { entity: TabEntity.Dag, tab: 42 } }] },
    { matches: [{ handle: { entity: TabEntity.Dag, tab: "unknown" } }] },
    { matches: [{ handle: undefined }] },
    { matches: [{ handle: { entity: 42, tab: TabName.Details } }] },
    { matches: [{ handle: null }] },
  ])("does not preserve unmatched or non-Dag routes", (matches) => {
    expect(getTabPath(matches.matches, TabEntity.Dag)).toBe("");
  });
});
