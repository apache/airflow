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
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { GridCell } from "src/context/gridSelection";
import { Wrapper } from "src/utils/Wrapper";

import { GridSelectionActionBar } from "./GridSelectionActionBar";
import { getCellKey } from "./useGridSelection";

vi.mock("./useGridSelectedTaskInstances", () => ({
  useGridSelectedTaskInstances: () => ({ data: [], isLoading: false }),
}));
vi.mock("src/pages/TaskInstances/BulkClearTaskInstancesButton", () => ({ default: () => undefined }));
vi.mock("src/pages/TaskInstances/BulkDeleteTaskInstancesButton", () => ({ default: () => undefined }));
// Reports `group.b` in run r1 as updated, the way the bulk actions name task instances.
vi.mock("src/pages/TaskInstances/BulkMarkTaskInstancesAsButton", () => ({
  default: ({ deselectKeys }: { readonly deselectKeys: (keys: Array<string>) => void }) => (
    <button onClick={() => deselectKeys(["dag.r1.group.b[-1]"])} type="button">
      mark
    </button>
  ),
}));

const selectionOf = (cells: Array<GridCell>) =>
  new Map(cells.map((cell) => [getCellKey(cell.runId, cell.taskId), cell]));

describe("GridSelectionActionBar", () => {
  it("deselects a group cell once one of its task instances is updated", () => {
    const deselectCells = vi.fn();

    render(
      <GridSelectionActionBar
        clearSelection={vi.fn()}
        dagId="dag"
        deselectCells={deselectCells}
        selectableGroups={new Map([["group", ["group.a", "group.b"]]])}
        selection={selectionOf([
          { runId: "r1", taskId: "group" },
          { runId: "r2", taskId: "group" },
          { runId: "r1", taskId: "group.a" },
        ])}
      />,
      { wrapper: Wrapper },
    );

    fireEvent.click(screen.getByRole("button", { name: "mark" }));

    expect(deselectCells).toHaveBeenCalledWith([getCellKey("r1", "group")]);
  });
});
