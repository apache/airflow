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
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { BaseWrapper } from "src/utils/Wrapper";

import { TogglePauseWatcher } from "./TogglePauseWatcher";

const mocks = vi.hoisted(() => ({ isPending: false, mutate: vi.fn() }));

vi.mock("src/queries/useTogglePauseWatcher", () => ({
  useTogglePauseWatcher: () => ({ isPending: mocks.isPending, mutate: mocks.mutate }),
}));

afterEach(() => {
  cleanup();
  mocks.isPending = false;
  mocks.mutate.mockReset();
});

describe("TogglePauseWatcher", () => {
  it("pauses an active watcher when toggled", () => {
    render(
      <TogglePauseWatcher assetId={1} isPaused={false} watcherName="my_watcher" />,
      { wrapper: BaseWrapper },
    );

    const switchControl = screen.getByRole("checkbox");

    expect(switchControl).toBeChecked();

    fireEvent.click(switchControl);

    expect(mocks.mutate).toHaveBeenCalledWith({
      assetId: 1,
      requestBody: { is_paused: true },
      watcherName: "my_watcher",
    });
  });

  it("unpauses a paused watcher when toggled", () => {
    render(
      <TogglePauseWatcher assetId={1} isPaused={true} watcherName="my_watcher" />,
      { wrapper: BaseWrapper },
    );

    const switchControl = screen.getByRole("checkbox");

    expect(switchControl).not.toBeChecked();

    fireEvent.click(switchControl);

    expect(mocks.mutate).toHaveBeenCalledWith({
      assetId: 1,
      requestBody: { is_paused: false },
      watcherName: "my_watcher",
    });
  });
});
