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

/**
 * HITL (Human-in-the-Loop) API helpers and end-to-end scenario.
 */
import { expect } from "@playwright/test";

import { baseUrl, getRequestContext, type RequestLike } from "../shared";
import { apiTriggerDagRun, waitForDagReady, waitForDagRunStatus, waitForTaskInstanceState } from "./dag-runs";

/**
 * Respond to a HITL (Human-in-the-Loop) task via the API.
 * 409 is treated as success (already responded).
 */
export async function apiRespondToHITL(
  source: RequestLike,
  options: {
    chosenOptions: Array<string>;
    dagId: string;
    mapIndex?: number;
    paramsInput?: Record<string, unknown>;
    runId: string;
    taskId: string;
  },
): Promise<void> {
  const { chosenOptions, dagId, runId, taskId } = options;
  const mapIndex = options.mapIndex ?? -1;
  const paramsInput = options.paramsInput ?? {};
  const request = getRequestContext(source);

  await expect(async () => {
    const response = await request.patch(
      `${baseUrl}/api/v2/dags/${dagId}/dagRuns/${runId}/taskInstances/${taskId}/${mapIndex}/hitlDetails`,
      {
        data: { chosen_options: chosenOptions, params_input: paramsInput },
        headers: { "Content-Type": "application/json" },
        timeout: 10_000,
      },
    );

    // 409 = already responded; acceptable.
    if (response.status() !== 409 && !response.ok()) {
      throw new Error(`HITL response failed (${response.status()})`);
    }
  }).toPass({ intervals: [2000, 3000, 5000], timeout: 60_000 });
}

/**
 * Run the full HITL flow entirely via API — no browser needed.
 *
 * The example_hitl_operator Dag has 4 parallel HITL tasks, then an approval
 * task, then a branch task. This function triggers the Dag, responds to each
 * task via the API, and waits for the Dag run to complete.
 */
export async function setupHITLFlowViaAPI(
  source: RequestLike,
  dagId: string,
  approve: boolean,
): Promise<string> {
  const request = getRequestContext(source);

  await waitForDagReady(request, dagId);
  await request.patch(`${baseUrl}/api/v2/dags/${dagId}`, { data: { is_paused: false } });

  const { dagRunId } = await apiTriggerDagRun(request, dagId);

  // wait_for_default_option auto-resolves (1s timeout, defaults=["option 7"]).
  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    taskId: "wait_for_default_option",
  });

  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "awaiting_input",
    runId: dagRunId,
    taskId: "wait_for_input",
  });
  await apiRespondToHITL(request, {
    chosenOptions: ["OK"],
    dagId,
    paramsInput: { information: "Approved by test" },
    runId: dagRunId,
    taskId: "wait_for_input",
  });

  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "awaiting_input",
    runId: dagRunId,
    taskId: "wait_for_option",
  });
  await apiRespondToHITL(request, {
    chosenOptions: ["option 1"],
    dagId,
    runId: dagRunId,
    taskId: "wait_for_option",
  });

  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "awaiting_input",
    runId: dagRunId,
    taskId: "wait_for_multiple_options",
  });
  await apiRespondToHITL(request, {
    chosenOptions: ["option 4", "option 5"],
    dagId,
    runId: dagRunId,
    taskId: "wait_for_multiple_options",
  });

  // Wait for all parallel tasks to succeed before the approval task starts.
  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    taskId: "wait_for_input",
  });
  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    taskId: "wait_for_option",
  });
  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    taskId: "wait_for_multiple_options",
  });

  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "awaiting_input",
    runId: dagRunId,
    taskId: "valid_input_and_options",
  });
  await apiRespondToHITL(request, {
    chosenOptions: [approve ? "Approve" : "Reject"],
    dagId,
    runId: dagRunId,
    taskId: "valid_input_and_options",
  });
  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    taskId: "valid_input_and_options",
  });

  if (approve) {
    await waitForTaskInstanceState(request, {
      dagId,
      expectedState: "awaiting_input",
      runId: dagRunId,
      taskId: "choose_a_branch_to_run",
    });
    await apiRespondToHITL(request, {
      chosenOptions: ["task_1"],
      dagId,
      runId: dagRunId,
      taskId: "choose_a_branch_to_run",
    });
  }

  await waitForDagRunStatus(request, {
    dagId,
    expectedState: "success",
    runId: dagRunId,
    timeout: 120_000,
  });

  return dagRunId;
}

export async function setupPendingHITLFlowViaAPI(source: RequestLike, dagId: string): Promise<string> {
  const request = getRequestContext(source);

  await waitForDagReady(request, dagId);
  const response = await request.patch(`${baseUrl}/api/v2/dags/${dagId}`, { data: { is_paused: false } });

  if (!response.ok()) {
    throw new Error(`Failed to unpause Dag ${dagId} (${response.status()})`);
  }

  const { dagRunId } = await apiTriggerDagRun(request, dagId);

  await waitForTaskInstanceState(request, {
    dagId,
    expectedState: "awaiting_input",
    runId: dagRunId,
    taskId: "wait_for_input",
  });

  return dagRunId;
}
