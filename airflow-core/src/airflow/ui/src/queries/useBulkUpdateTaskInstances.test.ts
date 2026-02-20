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
import { renderHook, waitFor } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { BaseWrapper } from "src/utils/Wrapper";

import { useBulkUpdateTaskInstances } from "./useBulkUpdateTaskInstances";

// Mock the toaster
vi.mock("src/components/ui", () => ({
  toaster: {
    create: vi.fn(),
  },
}));

// Mock the openapi queries
vi.mock("openapi/queries", () => ({
  UseGridServiceGetGridRunsKeyFn: vi.fn(() => ["grid-runs-key"]),
  useGridServiceGetGridTiSummariesKey: "grid-ti-summaries-key",
  UseGridServiceGetGridTiSummariesKeyFn: vi.fn(() => ["grid-ti-summaries-key"]),
  useTaskInstanceServiceBulkTaskInstances: vi.fn(),
  useTaskInstanceServiceGetTaskInstancesKey: "test-key",
}));

describe("useBulkUpdateTaskInstances", () => {
  let mockMutateAsync: ReturnType<typeof vi.fn>;
  const mockOnSuccess = vi.fn();

  beforeEach(async () => {
    vi.clearAllMocks();
    mockMutateAsync = vi.fn();
    const { useTaskInstanceServiceBulkTaskInstances } = await import("openapi/queries");

    vi.mocked(useTaskInstanceServiceBulkTaskInstances).mockReturnValue({
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
    } as ReturnType<typeof useTaskInstanceServiceBulkTaskInstances>);
  });

  it("should call mutateAsync with correct parameters", async () => {
    const { result } = renderHook(
      () =>
        useBulkUpdateTaskInstances({
          affectsMultipleRuns: false,
          dagId: "test-dag",
          dagRunId: "test-run",
          onSuccess: mockOnSuccess,
        }),
      {
        wrapper: BaseWrapper,
      },
    );

    const bulkBody = {
      actions: [
        {
          action: "update" as const,
          entities: [
            {
              map_index: -1,
              new_state: "success" as TaskInstanceState,
              task_id: "test-task",
            },
          ],
        },
      ],
    };

    await result.current.mutateAsync({
      dagId: "test-dag",
      dagRunId: "test-run",
      requestBody: bulkBody,
    });

    await waitFor(() => {
      expect(mockMutateAsync).toHaveBeenCalledWith({
        dagId: "test-dag",
        dagRunId: "test-run",
        requestBody: bulkBody,
      });
    });
  });

  it("should call onSuccess callback after successful mutation", async () => {
    const { useTaskInstanceServiceBulkTaskInstances } = await import("openapi/queries");

    // Capture the onSuccess callback passed to the hook
    let capturedOnSuccess: (() => Promise<void> | void) | undefined;
    let capturedMutateAsync: ReturnType<typeof vi.fn> | undefined;

    type HookOptions = Parameters<typeof useTaskInstanceServiceBulkTaskInstances>[0];

    const createMockMutateAsync = (onSuccessCallback: (() => Promise<void> | void) | undefined) =>
      vi.fn().mockImplementation(async () => {
        const result = {};

        await Promise.resolve();
        if (onSuccessCallback) {
          await onSuccessCallback();
        }

        return result;
      });

    // Set up the mock BEFORE rendering the hook
    vi.mocked(useTaskInstanceServiceBulkTaskInstances).mockImplementation((options: HookOptions) => {
      // Capture the onSuccess callback - TanStack Query will call it with (data, variables, onMutateResult, context)
      if (options?.onSuccess) {
        capturedOnSuccess = async () => {
          // Call with proper TanStack Query v5 signature
          const data = {};
          const variables = { dagId: "test-dag", dagRunId: "test-run", requestBody: { actions: [] } };

          await options.onSuccess?.(data, variables, undefined, {} as never);
        };
      }

      // Create mutateAsync that calls onSuccess after resolving
      capturedMutateAsync = createMockMutateAsync(capturedOnSuccess);

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
        mutateAsync: capturedMutateAsync,
        reset: vi.fn(),
        status: "idle",
        submittedAt: 0,
        variables: undefined,
      } as ReturnType<typeof useTaskInstanceServiceBulkTaskInstances>;
    });

    const { result: hookResult } = renderHook(
      () =>
        useBulkUpdateTaskInstances({
          affectsMultipleRuns: false,
          dagId: "test-dag",
          dagRunId: "test-run",
          onSuccess: mockOnSuccess,
        }),
      {
        wrapper: BaseWrapper,
      },
    );

    await hookResult.current.mutateAsync({
      dagId: "test-dag",
      dagRunId: "test-run",
      requestBody: { actions: [] },
    });

    // Wait for async query invalidation to complete
    await waitFor(
      () => {
        expect(mockOnSuccess).toHaveBeenCalled();
      },
      { timeout: 3000 },
    );
  });

  it("should handle errors and show error toaster", async () => {
    const error = new Error("Test error");

    mockMutateAsync.mockRejectedValue(error);

    const { useTaskInstanceServiceBulkTaskInstances } = await import("openapi/queries");
    const { toaster } = await import("src/components/ui");

    type HookOptions = Parameters<typeof useTaskInstanceServiceBulkTaskInstances>[0];

    // Mock the hook to call onError when mutation fails
    vi.mocked(useTaskInstanceServiceBulkTaskInstances).mockImplementation((options: HookOptions) => {
      // Simulate the mutation failing and calling onError
      if (options?.onError) {
        // Call onError asynchronously to simulate real behavior
        // onError signature: (error, variables, onMutateResult, context) => void
        setTimeout(() => {
          const variables = { dagId: "test-dag", dagRunId: "test-run", requestBody: { actions: [] } };

          options.onError?.(error, variables, undefined, {} as never);
        }, 0);
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
        mutateAsync: mockMutateAsync,
        reset: vi.fn(),
        status: "idle",
        submittedAt: 0,
        variables: undefined,
      } as ReturnType<typeof useTaskInstanceServiceBulkTaskInstances>;
    });

    const { result } = renderHook(
      () =>
        useBulkUpdateTaskInstances({
          affectsMultipleRuns: false,
          dagId: "test-dag",
          dagRunId: "test-run",
        }),
      {
        wrapper: BaseWrapper,
      },
    );

    try {
      await result.current.mutateAsync({
        dagId: "test-dag",
        dagRunId: "test-run",
        requestBody: { actions: [] },
      });
    } catch {
      // Expected to throw
    }

    await waitFor(
      () => {
        expect(toaster.create).toHaveBeenCalledWith(
          expect.objectContaining({
            type: "error",
          }),
        );
      },
      { timeout: 3000 },
    );
  });
});
