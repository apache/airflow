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
import { render, screen, waitFor, fireEvent } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { TimezoneProvider } from "src/context/timezone";
import { BaseWrapper } from "src/utils/Wrapper";

import MarkGroupTaskInstanceAsDialog from "./MarkGroupTaskInstanceAsDialog";

// Mock useParams to return the route params we need
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");

  return {
    ...actual,
    useParams: vi.fn(() => ({ dagId: "test-dag", runId: "test-run" })),
  };
});

// Mock the hooks
vi.mock("src/queries/useBulkUpdateTaskInstances", () => ({
  useBulkUpdateTaskInstances: vi.fn(),
}));

vi.mock("src/queries/useBulkUpdateTaskInstancesDryRun", () => ({
  useBulkUpdateTaskInstancesDryRun: vi.fn(),
}));

vi.mock("openapi/queries", () => ({
  useTaskInstanceServiceGetTaskInstances: vi.fn(),
}));

const DEFAULT_INITIAL_ENTRIES = ["/dags/test-dag/grid/test-run"];

// Custom wrapper with initialEntries support
const TestWrapper = ({
  children,
  initialEntries = DEFAULT_INITIAL_ENTRIES,
}: { readonly initialEntries?: Array<string> } & PropsWithChildren) => (
  <BaseWrapper>
    <MemoryRouter initialEntries={initialEntries}>
      <TimezoneProvider>{children}</TimezoneProvider>
    </MemoryRouter>
  </BaseWrapper>
);

type MockGroupTaskInstance = {
  readonly child_states: Record<string, number> | null;
  readonly max_end_date: string | null;
  readonly min_start_date: string;
  readonly state: TaskInstanceState | null;
  readonly task_id: string;
};

type MockTaskInstance = {
  readonly map_index: number;
  readonly state: string;
  readonly task_id: string;
};

type MockTaskInstances = {
  readonly task_instances: Array<MockTaskInstance>;
  readonly total_entries: number;
};

const setupMockMutation = async () => {
  const { useBulkUpdateTaskInstances } = await import("src/queries/useBulkUpdateTaskInstances");

  type HookOptions = Parameters<typeof useBulkUpdateTaskInstances>[0];
  let capturedOnSuccess: (() => Promise<void> | void) | undefined;
  const testMockMutateAsync = vi.fn().mockImplementation(async () => {
    await Promise.resolve();
    if (capturedOnSuccess) {
      await capturedOnSuccess();
    }

    return {};
  });

  vi.mocked(useBulkUpdateTaskInstances).mockImplementation((options: HookOptions) => {
    if (options.onSuccess) {
      capturedOnSuccess = options.onSuccess;
    }

    return {
      context: undefined,
      data: undefined,
      error: null,
      failureCount: 0,
      failureReason: null,
      isError: false,
      isIdle: true,
      isPaused: false,
      isPending: false,
      isSuccess: false,
      mutate: vi.fn(),
      mutateAsync: testMockMutateAsync,
      reset: vi.fn(),
      status: "idle",
      submittedAt: 0,
      variables: undefined,
    } as ReturnType<typeof useBulkUpdateTaskInstances>;
  });

  return testMockMutateAsync;
};

describe("MarkGroupTaskInstanceAsDialog", () => {
  const mockOnClose = vi.fn();
  const mockMutateAsync = vi.fn();
  const mockGroupTaskInstance: MockGroupTaskInstance = {
    child_states: null,
    max_end_date: null,
    min_start_date: "2024-01-01T00:00:00Z",
    state: "running" as TaskInstanceState,
    task_id: "test-task-group",
  };

  const mockTaskInstances: MockTaskInstances = {
    task_instances: [
      { map_index: -1, state: "running", task_id: "test-task-1" },
      { map_index: 0, state: "running", task_id: "test-task-2" },
    ],
    total_entries: 2,
  };

  beforeEach(async () => {
    vi.clearAllMocks();
    const { useBulkUpdateTaskInstances } = await import("src/queries/useBulkUpdateTaskInstances");
    const { useBulkUpdateTaskInstancesDryRun } = await import("src/queries/useBulkUpdateTaskInstancesDryRun");
    const { useTaskInstanceServiceGetTaskInstances } = await import("openapi/queries");

    vi.mocked(useBulkUpdateTaskInstances).mockReturnValue({
      context: undefined,
      data: undefined,
      error: null,
      failureCount: 0,
      failureReason: null,
      isError: false,
      isIdle: true,
      isPaused: false,
      isPending: false,
      isSuccess: false,
      mutate: vi.fn(),
      mutateAsync: mockMutateAsync,
      reset: vi.fn(),
      status: "idle",
      submittedAt: 0,
      variables: undefined,
    } as ReturnType<typeof useBulkUpdateTaskInstances>);

    vi.mocked(useBulkUpdateTaskInstancesDryRun).mockReturnValue({
      data: { task_instances: [], total_entries: 0 },
      isPending: false,
      isSuccess: true,
    } as ReturnType<typeof useBulkUpdateTaskInstancesDryRun>);

    vi.mocked(useTaskInstanceServiceGetTaskInstances).mockReturnValue({
      data: mockTaskInstances,
      isPending: false,
    } as ReturnType<typeof useTaskInstanceServiceGetTaskInstances>);
  });

  it("should render dialog when open is true and not when false", () => {
    const { rerender } = render(
      <MarkGroupTaskInstanceAsDialog
        groupTaskInstance={mockGroupTaskInstance}
        onClose={mockOnClose}
        open={true}
        state="success"
      />,
      { wrapper: TestWrapper },
    );

    expect(screen.getByText(/test-task-group/iu)).toBeInTheDocument();

    rerender(
      <MarkGroupTaskInstanceAsDialog
        groupTaskInstance={mockGroupTaskInstance}
        onClose={mockOnClose}
        open={false}
        state="success"
      />,
    );

    expect(screen.queryByText(/test-task-group/iu)).not.toBeInTheDocument();
  });

  it("should call dry run hook with correct parameters", async () => {
    const { useBulkUpdateTaskInstancesDryRun } = await import("src/queries/useBulkUpdateTaskInstancesDryRun");

    type DryRunProps = Parameters<typeof useBulkUpdateTaskInstancesDryRun>[0];
    const mockDryRunHook = vi.fn().mockReturnValue({
      data: { task_instances: [], total_entries: 0 },
      isPending: false,
      isSuccess: true,
    } as ReturnType<typeof useBulkUpdateTaskInstancesDryRun>);

    vi.mocked(useBulkUpdateTaskInstancesDryRun).mockImplementation(mockDryRunHook);

    render(
      <MarkGroupTaskInstanceAsDialog
        groupTaskInstance={mockGroupTaskInstance}
        onClose={mockOnClose}
        open={true}
        state="success"
      />,
      { wrapper: TestWrapper },
    );

    await waitFor(() => {
      const [callArgs] = mockDryRunHook.mock.calls[0] as [DryRunProps];

      expect(callArgs.dagId).toBe("test-dag");
      expect(callArgs.dagRunId).toBe("test-run");
      expect(callArgs.options?.enabled).toBe(true);
    });
  });

  it("should call mutateAsync with correct bulk body including map_index", async () => {
    const { useBulkUpdateTaskInstances } = await import("src/queries/useBulkUpdateTaskInstances");

    type MutateAsyncParams = Parameters<ReturnType<typeof useBulkUpdateTaskInstances>["mutateAsync"]>[0];
    const testMockMutateAsync = await setupMockMutation();

    render(
      <MarkGroupTaskInstanceAsDialog
        groupTaskInstance={mockGroupTaskInstance}
        onClose={mockOnClose}
        open={true}
        state="success"
      />,
      { wrapper: TestWrapper },
    );

    await waitFor(() => {
      expect(screen.getByText(/modal.confirm/iu)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/modal.confirm/iu));

    await waitFor(() => {
      const [callArgs] = testMockMutateAsync.mock.calls[0] as [MutateAsyncParams];
      const [action] = callArgs.requestBody.actions;

      if (action?.action !== "update") {
        throw new Error("Expected update action");
      }
      const [entity1, entity2] = action.entities;

      if (!entity1 || !entity2 || typeof entity1 === "string" || typeof entity2 === "string") {
        throw new Error("Expected entity objects");
      }

      expect(callArgs.dagId).toBe("test-dag");
      expect(callArgs.dagRunId).toBe("test-run");
      expect(action.action).toBe("update");
      expect(action.entities).toHaveLength(2);
      expect(entity1.task_id).toBe("test-task-1");
      expect(entity1.map_index).toBe(-1);
      expect(entity2.task_id).toBe("test-task-2");
      expect(entity2.map_index).toBe(0);
    });
  });

  it("should call onClose after successful mutation", async () => {
    await setupMockMutation();

    render(
      <MarkGroupTaskInstanceAsDialog
        groupTaskInstance={mockGroupTaskInstance}
        onClose={mockOnClose}
        open={true}
        state="success"
      />,
      { wrapper: TestWrapper },
    );

    await waitFor(() => {
      expect(screen.getByText(/modal.confirm/iu)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/modal.confirm/iu));
    await waitFor(
      () => {
        expect(mockOnClose).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });
});
