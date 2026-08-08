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
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { TagFilter } from "./TagFilter";

describe("TagFilter", () => {
  it("gives the any/all match-mode switch an accessible name", () => {
    render(
      <TagFilter
        onMenuScrollToBottom={vi.fn()}
        onMenuScrollToTop={vi.fn()}
        onSelectTagsChange={vi.fn()}
        onTagModeChange={vi.fn()}
        onUpdate={vi.fn()}
        selectedTags={["team-a", "critical"]}
        tagFilterMode="any"
        tags={["team-a", "critical"]}
      />,
      { wrapper: Wrapper },
    );

    const matchMode = screen.getByRole("checkbox", { name: "table.tagMode.label" });

    expect(matchMode).not.toBeChecked();
  });
});
