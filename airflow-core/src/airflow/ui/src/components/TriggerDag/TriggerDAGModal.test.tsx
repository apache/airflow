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
import type { ReactNode } from "react";

import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import type * as Ui from "src/system-components";

import { Wrapper } from "src/utils/Wrapper";

import TriggerDAGModal from "./TriggerDAGModal";

vi.mock("src/system-components", async (importOriginal) => {
  const actual = await importOriginal<typeof Ui>();

  return {
    ...actual,
    Modal: ({ children, open }: { readonly children?: ReactNode; readonly open?: boolean }) =>
      open ? <div>{children}</div> : undefined,
  };
});

vi.mock("./TriggerDAGForm", () => ({
  default: ({ isPaused }: { readonly isPaused: boolean }) => <div>{`form isPaused=${String(isPaused)}`}</div>,
}));

vi.mock("src/queries/useTrigger", () => ({
  useTrigger: () => ({ error: undefined, isPending: false, triggerDagRun: vi.fn() }),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return { ...actual, useDagServiceGetDag: vi.fn() };
});

const { useDagServiceGetDag } = await import("openapi/queries");

const DAG_ID = "unpaused_since";

describe("TriggerDAGModal", () => {
  beforeEach(() => {
    vi.mocked(useDagServiceGetDag).mockReturnValue({
      data: { dag_id: DAG_ID, is_backfillable: false, is_paused: false, timetable_summary: null },
      isError: false,
      isLoading: false,
    } as unknown as ReturnType<typeof useDagServiceGetDag>);
  });

  it("uses the paused state fetched on open over the one the caller had cached", () => {
    render(<TriggerDAGModal dagDisplayName={DAG_ID} dagId={DAG_ID} isPaused onClose={vi.fn()} open />, {
      wrapper: Wrapper,
    });

    expect(screen.getByText("form isPaused=false")).toBeInTheDocument();
    expect(useDagServiceGetDag).toHaveBeenCalledWith(
      { dagId: DAG_ID },
      undefined,
      expect.objectContaining({ enabled: true, staleTime: 0 }),
    );
  });
});
