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
import type { ComponentProps } from "react";

import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { ReactFlowProvider } from "@xyflow/react";
import { describe, expect, it } from "vitest";

import "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { AssetWatcherNode } from "./AssetWatcherNode";

const renderNode = (data: Record<string, unknown>) =>
  render(
    <Wrapper>
      <ReactFlowProvider>
        <AssetWatcherNode
          // The xyflow NodeProps surface is large; the component only reads `data`.
          {...({
            data: {
              height: 80,
              id: "asset-watcher:1:7",
              label: "orders_queue",
              type: "asset-watcher",
              width: 140,
              ...data,
            },
          } as unknown as ComponentProps<typeof AssetWatcherNode>)}
        />
      </ReactFlowProvider>
    </Wrapper>,
  );

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("AssetWatcherNode", () => {
  it.each([
    { label: "triggers.status.failing", status: "failing" },
    { label: "states.running", status: "running" },
  ])("links the $status watcher to its trigger and shows its status", ({ label, status }) => {
    renderNode({ triggerId: 7, triggerStatus: status });

    expect(screen.getByRole("link", { name: "orders_queue" })).toHaveAttribute("href", "/triggers/7");
    expect(screen.getByTestId("trigger-status-badge")).toHaveTextContent(label);
  });

  it("shows no status it was not given", () => {
    renderNode({ triggerId: 7 });

    expect(screen.queryByTestId("trigger-status-badge")).not.toBeInTheDocument();
  });
});
