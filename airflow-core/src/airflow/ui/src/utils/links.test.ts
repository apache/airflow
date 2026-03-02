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
import { describe, it, expect } from "vitest";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";

import { buildTaskInstanceUrl, getTaskInstanceAdditionalPath, getTaskInstanceLink } from "./links";

describe("getTaskInstanceLink", () => {
  const testCases = [
    // Individual parameters tests
    {
      description: "individual parameters without map index",
      expected: "/dags/my_dag/runs/run_123/tasks/task_1",
      input: { dagId: "my_dag", dagRunId: "run_123", mapIndex: -1, taskId: "task_1" },
    },
    {
      description: "individual parameters with map index",
      expected: "/dags/my_dag/runs/run_123/tasks/task_1/mapped/5",
      input: { dagId: "my_dag", dagRunId: "run_123", mapIndex: 5, taskId: "task_1" },
    },
    {
      description: "individual parameters with map index of 0",
      expected: "/dags/test_dag/runs/test_run/tasks/mapped_task/mapped/0",
      input: { dagId: "test_dag", dagRunId: "test_run", mapIndex: 0, taskId: "mapped_task" },
    },
    {
      description: "individual parameters without mapIndex property (defaults to -1)",
      expected: "/dags/my_dag/runs/run_123/tasks/task_1",
      input: { dagId: "my_dag", dagRunId: "run_123", taskId: "task_1" },
    },
    // TaskInstanceResponse object tests
    {
      description: "TaskInstanceResponse object without map index",
      expected: "/dags/my_dag/runs/run_123/tasks/task_1",
      input: {
        dag_id: "my_dag",
        dag_run_id: "run_123",
        map_index: -1,
        task_id: "task_1",
      } as TaskInstanceResponse,
    },
    {
      description: "TaskInstanceResponse object with map index",
      expected: "/dags/my_dag/runs/run_123/tasks/task_1/mapped/5",
      input: {
        dag_id: "my_dag",
        dag_run_id: "run_123",
        map_index: 5,
        task_id: "task_1",
      } as TaskInstanceResponse,
    },
    {
      description: "TaskInstanceResponse object with map index of 0",
      expected: "/dags/test_dag/runs/test_run/tasks/mapped_task/mapped/0",
      input: {
        dag_id: "test_dag",
        dag_run_id: "test_run",
        map_index: 0,
        task_id: "mapped_task",
      } as TaskInstanceResponse,
    },
  ];

  it.each(testCases)("should handle $description", ({ expected, input }) => {
    const result = getTaskInstanceLink(input);

    expect(result).toBe(expected);
  });
});

describe("getTaskInstanceAdditionalPath", () => {
  it("should return empty string for basic task path", () => {
    const result = getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/task_1");

    expect(result).toBe("");
  });

  it("should extract sub-route from regular task path", () => {
    const result = getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/task_1/details");

    expect(result).toBe("/details");
  });

  it("should extract sub-route from group task path", () => {
    const result = getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/group/my_group/xcom");

    expect(result).toBe("/xcom");
  });

  it("should extract sub-route from mapped task with index", () => {
    const result = getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/task_1/mapped/5/events");

    expect(result).toBe("/events");
  });

  it("should handle all known task instance routes", () => {
    const knownRoutes = [
      "events",
      "xcom",
      "code",
      "details",
      "rendered_templates",
      "task_instances",
      "asset_events",
      "required_actions",
    ];

    for (const route of knownRoutes) {
      const result = getTaskInstanceAdditionalPath(`/dags/test/runs/run_1/tasks/task_1/${route}`);

      expect(result).toBe(`/${route}`);
    }
  });

  it("should handle various path scenarios", () => {
    // Plugin routes
    expect(getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/task_1/plugin/custom-view")).toBe(
      "/plugin/custom-view",
    );

    // Unknown sub-routes should return empty
    expect(getTaskInstanceAdditionalPath("/dags/my_dag/runs/run_1/tasks/task_1/unknown_route")).toBe("");

    // Complex plugin paths
    expect(
      getTaskInstanceAdditionalPath("/dags/test/runs/run_1/tasks/group/group_1/plugin/my-plugin/nested/path"),
    ).toBe("/plugin/my-plugin/nested/path");

    // Routes with special characters
    expect(
      getTaskInstanceAdditionalPath("/dags/my-dag_v2/runs/run_1-test/tasks/task.1/rendered_templates"),
    ).toBe("/rendered_templates");
  });
});

describe("buildTaskInstanceUrl", () => {
  it("should build basic URL types", () => {
    // Basic task instance URL
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/other_dag/runs/other_run/tasks/other_task",
        dagId: "my_dag",
        runId: "run_123",
        taskId: "task_1",
      }),
    ).toBe("/dags/my_dag/runs/run_123/tasks/task_1");

    // Group task instance URL
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/some/path",
        dagId: "my_dag",
        isGroup: true,
        runId: "run_123",
        taskId: "group_1",
      }),
    ).toBe("/dags/my_dag/runs/run_123/tasks/group/group_1");

    // Mapped task without map index
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/some/path",
        dagId: "my_dag",
        isMapped: true,
        runId: "run_123",
        taskId: "mapped_task",
      }),
    ).toBe("/dags/my_dag/runs/run_123/tasks/mapped_task/mapped");

    // Mapped task with map index
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/some/path",
        dagId: "my_dag",
        isMapped: true,
        mapIndex: "5",
        runId: "run_123",
        taskId: "mapped_task",
      }),
    ).toBe("/dags/my_dag/runs/run_123/tasks/mapped_task/mapped/5");
  });

  it("should handle advanced scenarios", () => {
    // Preserve sub-routes from current pathname
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/old_dag/runs/old_run/tasks/old_task/details",
        dagId: "new_dag",
        runId: "new_run",
        taskId: "new_task",
      }),
    ).toBe("/dags/new_dag/runs/new_run/tasks/new_task/details");

    // Preserve sub-routes for mapped tasks
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/old_dag/runs/old_run/tasks/old_task/mapped/2/xcom",
        dagId: "new_dag",
        isMapped: true,
        mapIndex: "7",
        runId: "new_run",
        taskId: "new_task",
      }),
    ).toBe("/dags/new_dag/runs/new_run/tasks/new_task/mapped/7/xcom");

    // Handle mapIndex of -1
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/some/path",
        dagId: "my_dag",
        isMapped: true,
        mapIndex: "-1",
        runId: "run_123",
        taskId: "mapped_task",
      }),
    ).toBe("/dags/my_dag/runs/run_123/tasks/mapped_task/mapped");

    // Groups should never preserve tabs (only have "Task Instances" tab)
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/old/runs/old/tasks/old_task/rendered_templates",
        dagId: "new_dag",
        isGroup: true,
        runId: "new_run",
        taskId: "new_group",
      }),
    ).toBe("/dags/new_dag/runs/new_run/tasks/group/new_group");

    // Groups should never preserve tabs even for mapped groups
    expect(
      buildTaskInstanceUrl({
        currentPathname: "/dags/old/runs/old/tasks/group/old_group/events",
        dagId: "new_dag",
        isGroup: true,
        isMapped: true,
        mapIndex: "3",
        runId: "new_run",
        taskId: "new_group",
      }),
    ).toBe("/dags/new_dag/runs/new_run/tasks/group/new_group/mapped/3");
  });
});
