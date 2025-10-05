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
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import axios from "axios";

import { useRecentConfigurations } from "src/queries/useRecentConfigurations";

// Mock axios
vi.mock("axios");
const mockedAxios = vi.mocked(axios);

const createWrapper = () => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
};

describe("useRecentConfigurations", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should fetch recent configurations successfully", async () => {
    const mockData = {
      configurations: [
        {
          run_id: "manual_1",
          conf: { param1: "value1" },
          logical_date: "2024-01-01T10:00:00Z",
          start_date: "2024-01-01T10:00:00Z",
        },
        {
          run_id: "manual_2",
          conf: { param1: "value2" },
          logical_date: "2024-01-01T09:00:00Z",
          start_date: "2024-01-01T09:00:00Z",
        },
      ],
    };

    mockedAxios.get.mockResolvedValueOnce({ data: mockData });

    const { result } = renderHook(() => useRecentConfigurations("test_dag", 5), {
      wrapper: createWrapper(),
    });

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    expect(result.current.data).toEqual(mockData);
    expect(mockedAxios.get).toHaveBeenCalledWith(
      "/api/v1/dags/test_dag/dagRuns/recent-configurations",
      { params: { limit: 5 } }
    );
  });

  it("should handle API errors", async () => {
    const error = new Error("API Error");
    mockedAxios.get.mockRejectedValueOnce(error);

    const { result } = renderHook(() => useRecentConfigurations("test_dag"), {
      wrapper: createWrapper(),
    });

    await waitFor(() => {
      expect(result.current.isError).toBe(true);
    });

    expect(result.current.error).toEqual(error);
  });

  it("should not fetch when dagId is empty", () => {
    const { result } = renderHook(() => useRecentConfigurations(""), {
      wrapper: createWrapper(),
    });

    expect(result.current.isLoading).toBe(false);
    expect(mockedAxios.get).not.toHaveBeenCalled();
  });

  it("should use default limit when not provided", async () => {
    const mockData = { configurations: [] };
    mockedAxios.get.mockResolvedValueOnce({ data: mockData });

    renderHook(() => useRecentConfigurations("test_dag"), {
      wrapper: createWrapper(),
    });

    await waitFor(() => {
      expect(mockedAxios.get).toHaveBeenCalledWith(
        "/api/v1/dags/test_dag/dagRuns/recent-configurations",
        { params: { limit: 5 } }
      );
    });
  });

  it("should use custom limit when provided", async () => {
    const mockData = { configurations: [] };
    mockedAxios.get.mockResolvedValueOnce({ data: mockData });

    renderHook(() => useRecentConfigurations("test_dag", 10), {
      wrapper: createWrapper(),
    });

    await waitFor(() => {
      expect(mockedAxios.get).toHaveBeenCalledWith(
        "/api/v1/dags/test_dag/dagRuns/recent-configurations",
        { params: { limit: 10 } }
      );
    });
  });
});
