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
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { useTaskStateStoreServiceGetTaskStateStore } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests";

import { Wrapper } from "src/utils/Wrapper";

import { DurableBanner } from "./DurableBanner";

vi.mock("openapi/queries", async () => ({
  ...(await vi.importActual("openapi/queries")),
  useTaskStateStoreServiceGetTaskStateStore: vi.fn(),
}));

const TRIES = [
  { decision: "submitted", external_id: "run_1", saved_seconds: null, try_number: 1 },
  { decision: "reconnected", external_id: "run_1", saved_seconds: 90, try_number: 2 },
  { decision: "already_succeeded", external_id: "run_1", saved_seconds: null, try_number: 3 },
  { decision: "reconnected", external_id: "run_1", saved_seconds: null, try_number: 4 },
];

const renderFor = (tryNumber: number) => {
  vi.mocked(useTaskStateStoreServiceGetTaskStateStore).mockReturnValue({
    data: { value: { tries: TRIES } },
  } as unknown as ReturnType<typeof useTaskStateStoreServiceGetTaskStateStore>);

  return render(
    <Wrapper>
      <DurableBanner
        taskInstance={{ dag_id: "d", dag_run_id: "r", map_index: -1, task_id: "t" } as TaskInstanceResponse}
        tryNumber={tryNumber}
      />
    </Wrapper>,
  );
};

describe("DurableBanner", () => {
  it.each([
    [2, "logs.durable.title.reconnected", "logs.durable.detail.reconnected"],
    [3, "logs.durable.title.already_succeeded", "logs.durable.detail.already_succeeded"],
    [4, "logs.durable.title.reconnected", "logs.durable.detail.reconnectedRunning"],
  ])("shows what durable execution did on try %i", (tryNumber, title, detail) => {
    renderFor(tryNumber);

    expect(screen.getByText(title)).toBeInTheDocument();
    expect(screen.getByText(detail)).toBeInTheDocument();
  });

  it("shows nothing for a try that submitted a new job", () => {
    renderFor(1);

    expect(screen.queryByText("logs.durable", { exact: false })).not.toBeInTheDocument();
  });
});
