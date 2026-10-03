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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { XcomService } from "openapi/requests";

import { Wrapper } from "src/utils/Wrapper";

import XComModal from "./XComModal";

vi.mock("src/components/JsonEditor", () => ({
  JsonEditor: ({ value }: { readonly value: string }) => <div data-testid="editor">{value}</div>,
}));
afterEach(() => vi.restoreAllMocks());

it("edits the selected regional XCom without using the colliding public map coordinate", async () => {
  const regionId = "11111111-1111-1111-1111-111111111111";
  const value = {
    dag_display_name: "dag",
    dag_id: "dag",
    key: "return_value",
    logical_date: null,
    map_index: -1,
    region_id: regionId,
    region_index: 3,
    run_after: "2026-01-01T00:00:00Z",
    run_id: "run",
    task_display_name: "member",
    task_id: "member",
    timestamp: "2026-01-01T00:00:00Z",
    value: "third pass",
  };
  const fetch = vi.spyOn(XcomService, "getXcomEntry").mockResolvedValue(value);
  const update = vi.spyOn(XcomService, "updateXcomEntry").mockResolvedValue(value);

  render(
    <XComModal
      dagId="dag"
      isOpen
      mapIndex={-1}
      mode="edit"
      onClose={vi.fn()}
      regionId={regionId}
      regionIndex={3}
      runId="run"
      taskId="member"
      xcomKey="return_value"
    />,
    { wrapper: Wrapper },
  );

  await waitFor(() => expect(screen.getByTestId("editor")).toHaveTextContent("third pass"));
  fireEvent.click(screen.getByRole("button", { name: "common:modal.save" }));
  await waitFor(() =>
    expect(update).toHaveBeenCalledWith({
      dagId: "dag",
      dagRunId: "run",
      requestBody: { map_index: -1, region_id: regionId, region_index: 3, value: "third pass" },
      taskId: "member",
      xcomKey: "return_value",
    }),
  );
  expect(fetch).toHaveBeenCalledWith(expect.objectContaining({ regionId, regionIndex: 3 }));
});
