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
import "@testing-library/jest-dom";
import { cleanup, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { BaseWrapper } from "src/utils/Wrapper";

import { PanelButtons } from "./PanelButtons";

// The options under test live in PanelButtons itself; its children fetch data or need a flow canvas.
vi.mock("@xyflow/react", () => ({ useReactFlow: () => ({ fitView: vi.fn() }) }));
vi.mock("src/components/DagVersionSelect", () => ({ DagVersionSelect: () => null }));
vi.mock("src/components/Graph/DirectionDropdown", () => ({ DirectionDropdown: () => null }));
vi.mock("src/components/GraphTaskFilters", () => ({ GraphTaskFilters: () => null }));
vi.mock("./DagRunSelect", () => ({ DagRunSelect: () => null }));
vi.mock("./Grid/RunTypeLegend", () => ({ RunTypeLegend: () => null }));
vi.mock("./GridFilters", () => ({ GridFilters: () => null }));
vi.mock("./TaskStreamFilter", () => ({ TaskStreamFilter: () => null }));
vi.mock("./ToggleGroups", () => ({ ToggleGroups: () => null }));
vi.mock("./VersionIndicatorSelect", () => ({ VersionIndicatorSelect: () => null }));

afterEach(() => cleanup());

describe("PanelButtons", () => {
  it("leaves the saved limit alone when it exceeds what the gantt width allows", () => {
    const setLimit = vi.fn();

    render(
      <PanelButtons
        containerWidth={300}
        dagView="gantt"
        limit={50}
        panelGroupRef={{ current: null }}
        setDagView={vi.fn()}
        setLimit={setLimit}
        setShowVersionIndicatorMode={vi.fn()}
        showVersionIndicatorMode={VersionIndicatorOptions.ALL}
      />,
      {
        wrapper: ({ children }) => (
          <BaseWrapper>
            <MemoryRouter>{children}</MemoryRouter>
          </BaseWrapper>
        ),
      },
    );

    expect(screen.getByRole("button", { name: /options/iu })).toBeInTheDocument();
    expect(setLimit).not.toHaveBeenCalled();
  });
});
