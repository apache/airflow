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
vi.mock("src/queries/usePatchTaskGroup", () => ({
  usePatchTaskGroup: vi.fn(),
}));

vi.mock("src/queries/usePatchTaskGroupDryRun", () => ({
  usePatchTaskGroupDryRun: vi.fn(),
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

const setupMockMutation = async () => {
  const { usePatchTaskGroup: usePatchTaskGroupHook } = await import("src/queries/usePatchTaskGroup");

  type HookOptions = Parameters<typeof usePatchTaskGroupHook>[0];
  let capturedOnSuccess: (() => Promise<void> | void) | null;
  const testMockMutateAsync = vi.fn().mockImplementation(async () => {
    await Promise.resolve();
    if (capturedOnSuccess) {
      await capturedOnSuccess();
    }

    return {
      task_instances: [],
      total_entries: 0,
    };
  });

  vi.mocked(usePatchTaskGroupHook).mockImplementation((options: HookOptions) => {
    if (options.onSuccess) {
      capturedOnSuccess = options.onSuccess;
    }

    return {
      context: null,
      data: null,
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
      variables: null,
    } as unknown as ReturnType<typeof usePatchTaskGroupHook>;
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

  beforeEach(async () => {
    vi.clearAllMocks();
    const { usePatchTaskGroup: usePatchTaskGroupHook } = await import("src/queries/usePatchTaskGroup");
    const { usePatchTaskGroupDryRun } = await import("src/queries/usePatchTaskGroupDryRun");

    vi.mocked(usePatchTaskGroupHook).mockReturnValue({
      context: null,
      data: null,
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
      variables: null,
    } as unknown as ReturnType<typeof usePatchTaskGroupHook>);

    vi.mocked(usePatchTaskGroupDryRun).mockReturnValue({
      data: { task_instances: [{ state: "running", task_id: "test-task-1" }], total_entries: 1 },
      isPending: false,
      isSuccess: true,
    } as ReturnType<typeof usePatchTaskGroupDryRun>);
  });

  it("should render dialog when open is true and not when false", async () => {
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

    await waitFor(() => {
      expect(screen.queryByText(/test-task-group/iu)).not.toBeInTheDocument();
    });
  });

  it("should call dry run hook with correct parameters", async () => {
    const { usePatchTaskGroupDryRun } = await import("src/queries/usePatchTaskGroupDryRun");

    type DryRunProps = Parameters<typeof usePatchTaskGroupDryRun>[0];
    const mockDryRunHook = vi.fn().mockReturnValue({
      data: { task_instances: [{ state: "running", task_id: "test-task-1" }], total_entries: 1 },
      isPending: false,
      isSuccess: true,
    } as ReturnType<typeof usePatchTaskGroupDryRun>);

    vi.mocked(usePatchTaskGroupDryRun).mockImplementation(mockDryRunHook);

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
      expect(callArgs.taskGroupId).toBe("test-task-group");
      expect(callArgs.options?.enabled).toBe(true);
      expect(callArgs.requestBody.new_state).toBe("success");
    });
  });

  it("should call mutateAsync with correct request body", async () => {
    const { usePatchTaskGroup: usePatchTaskGroupHook } = await import("src/queries/usePatchTaskGroup");

    type MutateAsyncParams = Parameters<ReturnType<typeof usePatchTaskGroupHook>["mutateAsync"]>[0];
    const mockMutateAsyncFromSetup = await setupMockMutation();

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
      const [callArgs] = mockMutateAsyncFromSetup.mock.calls[0] as [MutateAsyncParams];

      expect(callArgs.dagId).toBe("test-dag");
      expect(callArgs.dagRunId).toBe("test-run");
      expect(callArgs.taskGroupId).toBe("test-task-group");
      expect(callArgs.requestBody.new_state).toBe("success");
      expect(callArgs.requestBody.include_downstream).toBe(false);
      expect(callArgs.requestBody.include_future).toBe(false);
      expect(callArgs.requestBody.include_past).toBe(false);
      expect(callArgs.requestBody.include_upstream).toBe(false);
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
