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

import { BaseWrapper } from "src/utils/Wrapper";

import {
  useBulkUpdateTaskInstancesDryRun,
  useBulkUpdateTaskInstancesDryRunKey,
} from "./useBulkUpdateTaskInstancesDryRun";

// Mock the openapi request
vi.mock("openapi/requests/core/request", () => ({
  request: vi.fn(),
}));

type RequestConfig = {
  readonly body?: {
    readonly actions?: Array<{
      readonly entities?: Array<{ readonly task_id?: string }>;
    }>;
  };
};

describe("useBulkUpdateTaskInstancesDryRun", () => {
  const mockRequest = vi.fn();

  beforeEach(async () => {
    vi.clearAllMocks();
    const { request } = await import("openapi/requests/core/request");

    vi.mocked(request).mockImplementation(mockRequest);
  });

  it("should have correct query key", () => {
    expect(useBulkUpdateTaskInstancesDryRunKey).toBe("bulkUpdateTaskInstancesDryRun");
  });

  it("should call request with dry_run=true query parameter", async () => {
    const mockResponse = {
      task_instances: [],
      total_entries: 0,
    };

    mockRequest.mockResolvedValue(mockResponse);

    const { result } = renderHook(
      () =>
        useBulkUpdateTaskInstancesDryRun({
          dagId: "test-dag",
          dagRunId: "test-run",
          requestBody: {
            actions: [
              {
                action: "update",
                entities: [
                  {
                    map_index: -1,
                    new_state: "success",
                    task_id: "test-task",
                  },
                ],
              },
            ],
          },
        }),
      {
        wrapper: BaseWrapper,
      },
    );

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    expect(mockRequest).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        method: "PATCH",
        path: {
          dag_id: "test-dag",
          dag_run_id: "test-run",
        },
        query: {
          dry_run: true,
        },
      }),
    );
  });

  it("should use default empty actions when requestBody is not provided", async () => {
    const mockResponse = {
      task_instances: [],
      total_entries: 0,
    };

    mockRequest.mockResolvedValue(mockResponse);

    const { result } = renderHook(
      () =>
        useBulkUpdateTaskInstancesDryRun({
          dagId: "test-dag",
          dagRunId: "test-run",
        }),
      {
        wrapper: BaseWrapper,
      },
    );

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    expect(mockRequest).toHaveBeenCalledWith(
      expect.anything(),
      expect.objectContaining({
        body: { actions: [] },
      }),
    );
  });

  it("should include requestBody in query key for proper caching", async () => {
    const mockResponse1 = {
      task_instances: [{ task_id: "task1" }],
      total_entries: 1,
    };
    const mockResponse2 = {
      task_instances: [{ task_id: "task2" }],
      total_entries: 1,
    };

    // Mock request to return different responses for different request bodies
    mockRequest.mockImplementation((_openapi: unknown, config: RequestConfig) => {
      const requestBody = config.body;
      const taskId = requestBody?.actions?.[0]?.entities?.[0]?.task_id;

      if (taskId === "task1") {
        return Promise.resolve(mockResponse1);
      }

      return Promise.resolve(mockResponse2);
    });

    const { result: result1 } = renderHook(
      () =>
        useBulkUpdateTaskInstancesDryRun({
          dagId: "test-dag",
          dagRunId: "test-run",
          requestBody: {
            actions: [
              {
                action: "update",
                entities: [{ new_state: "success", task_id: "task1" }],
              },
            ],
          },
        }),
      {
        wrapper: BaseWrapper,
      },
    );

    const { result: result2 } = renderHook(
      () =>
        useBulkUpdateTaskInstancesDryRun({
          dagId: "test-dag",
          dagRunId: "test-run",
          requestBody: {
            actions: [
              {
                action: "update",
                entities: [{ new_state: "failed", task_id: "task2" }],
              },
            ],
          },
        }),
      {
        wrapper: BaseWrapper,
      },
    );

    await waitFor(() => {
      expect(result1.current.isSuccess).toBe(true);
      expect(result2.current.isSuccess).toBe(true);
    });

    // Query keys should be different due to different requestBody, so data should be different
    expect(result1.current.data).not.toBe(result2.current.data);
    expect(result1.current.data?.task_instances[0]?.task_id).toBe("task1");
    expect(result2.current.data?.task_instances[0]?.task_id).toBe("task2");
  });

  it("should handle query options correctly", () => {
    const mockResponse = {
      task_instances: [],
      total_entries: 0,
    };

    mockRequest.mockResolvedValue(mockResponse);

    const { result } = renderHook(
      () =>
        useBulkUpdateTaskInstancesDryRun({
          dagId: "test-dag",
          dagRunId: "test-run",
          options: {
            enabled: false,
          },
        }),
      {
        wrapper: BaseWrapper,
      },
    );

    // Query should not run when enabled is false
    expect(result.current.isFetching).toBe(false);
  });
});
