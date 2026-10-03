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

// Mirrors `TriggerDagRunOperator.execute` and `execute_complete` in the standard provider.

import { setTimeout as sleep } from "node:timers/promises";
import type { CoordinatorClient } from "./client.js";
import type { LogChannel } from "./log-channel.js";
import type {
  RuntimeDeferTask,
  RuntimeRetryTask,
  RuntimeSucceedTask,
  RuntimeTaskState,
  StartupDetails,
} from "./protocol.js";
import { getBooleanEnv, type TriggerDagRunTask } from "../sdk/trigger-dag-run.js";

export type TriggerOutcome =
  RuntimeSucceedTask | RuntimeRetryTask | RuntimeTaskState | RuntimeDeferTask;

/** A failure the task's retries apply to, as `_handle_current_task_failed` decides. */
export type FailTask = (message: string) => RuntimeRetryTask | RuntimeTaskState;

const DAG_STATE_TRIGGER = "airflow.providers.standard.triggers.external_task.DagStateTrigger";
/** `TriggerDagRunLink().xcom_key`: the XCom the "Triggered DAG" extra link reads. */
const LINK_XCOM_KEY = "_link_TriggerDagRunLink";
const RUN_ID_XCOM_KEY = "trigger_run_id";
/** `TRIGGER_FAIL_REPR`: the `next_method` a failed or timed-out trigger resumes with. */
const TRIGGER_FAIL = "__fail__";
const EXECUTE_COMPLETE = "execute_complete";

export async function runTriggerDagRun(
  details: StartupDetails,
  trigger: TriggerDagRunTask,
  client: CoordinatorClient,
  logs: LogChannel,
  signal: AbortSignal,
  fail: FailTask,
): Promise<TriggerOutcome> {
  const nextMethod = details.ti_context.next_method;
  if (nextMethod) return resume(nextMethod, details.ti_context.next_kwargs, trigger, logs, fail);

  const logicalDate = new Date();
  const runId = trigger.runId ?? `manual__${pythonIsoformat(logicalDate)}`;

  if (trigger.failWhenDagIsPaused && (await client.isDagPaused(trigger.dagId))) {
    return fail(`Dag ${trigger.dagId} is paused`);
  }

  await client.setXCom({ key: LINK_XCOM_KEY, value: dagRunUrl(trigger.dagId, runId) });

  logs.info("Triggering Dag Run.", { trigger_dag_id: trigger.dagId });
  const triggered = await client.triggerDagRun({
    dag_id: trigger.dagId,
    run_id: runId,
    logical_date: logicalDate.toISOString(),
    run_after: null,
    conf: (trigger.conf as Record<string, unknown> | undefined) ?? null,
    reset_dag_run: trigger.resetDagRun,
    note: trigger.note ?? null,
  });
  if (triggered === "already_exists") {
    if (trigger.skipWhenAlreadyExists) {
      logs.info(
        "Dag Run already exists, skipping task as skip_when_already_exists is set to True.",
        { dag_id: trigger.dagId },
      );
      return { type: "TaskState", state: "skipped", end_date: new Date().toISOString() };
    }
    logs.error("Dag Run already exists, marking task as failed.", { dag_id: trigger.dagId });
    return { type: "TaskState", state: "failed", end_date: new Date().toISOString() };
  }
  logs.info("Dag Run triggered successfully.", { trigger_dag_id: trigger.dagId });
  await client.setXCom({ key: RUN_ID_XCOM_KEY, value: runId });

  if (!trigger.waitForCompletion) {
    if (trigger.deferrable) {
      logs.info(
        "Ignoring deferrable=True because wait_for_completion=False. " +
          "Task will complete immediately without waiting for the triggered DAG run.",
        { trigger_dag_id: trigger.dagId },
      );
    }
    return succeeded();
  }

  if (trigger.deferrable) {
    logs.info("Pausing task as DEFERRED.", { trigger_dag_id: trigger.dagId, run_id: runId });
    return {
      type: "DeferTask",
      state: "deferred",
      classpath: DAG_STATE_TRIGGER,
      // `DagStateTrigger.serialize()`, key for key.
      trigger_kwargs: {
        dag_id: trigger.dagId,
        states: [...trigger.allowedStates, ...trigger.failedStates],
        poll_interval: trigger.pokeInterval,
        run_ids: [runId],
        execution_dates: null,
      },
      trigger_timeout: null,
      // `_defer_task` hands the trigger the task's queue only when triggerer queues are enabled.
      queue: getBooleanEnv("AIRFLOW__TRIGGERER__QUEUES_ENABLED", false)
        ? (details.ti.queue ?? null)
        : null,
      next_method: EXECUTE_COMPLETE,
      next_kwargs: {},
    };
  }

  while (true) {
    logs.info("Waiting for dag run to complete execution in allowed state.", {
      dag_id: trigger.dagId,
      run_id: runId,
      allowed_state: trigger.allowedStates,
    });
    await sleep(trigger.pokeInterval * 1000, undefined, { signal });
    const state = await client.getDagRunState(trigger.dagId, runId);
    if (includes(trigger.failedStates, state)) {
      logs.error("DagRun finished with failed state.", { dag_id: trigger.dagId, state });
      return fail(`${trigger.dagId} failed with failed state ${state}`);
    }
    if (includes(trigger.allowedStates, state)) {
      logs.info("DagRun finished with allowed state.", { dag_id: trigger.dagId, state });
      return succeeded();
    }
    logs.debug("DagRun not yet in allowed or failed state.", { dag_id: trigger.dagId, state });
  }
}

/** `BaseOperator.resume_execution` for this task: `__fail__` or `execute_complete`. */
function resume(
  nextMethod: string,
  nextKwargs: unknown,
  trigger: TriggerDagRunTask,
  logs: LogChannel,
  fail: FailTask,
): TriggerOutcome {
  const kwargs = isRecord(nextKwargs) ? nextKwargs : {};
  if (nextMethod === TRIGGER_FAIL) {
    const traceback = kwargs["traceback"];
    if (Array.isArray(traceback)) logs.error(`Trigger failed:\n${traceback.join("\n")}`);
    return fail(String(kwargs["error"] ?? "Unknown"));
  }
  if (nextMethod !== EXECUTE_COMPLETE) {
    return fail(`Task cannot resume with next_method "${nextMethod}"`);
  }
  const eventData = decodeEvent(kwargs["event"]);
  const runIds = eventData?.["run_ids"];
  if (eventData === undefined || !Array.isArray(runIds)) {
    return fail(`Task resumed with an event it cannot read: ${JSON.stringify(kwargs["event"])}`);
  }
  const failedRunIds: string[] = [];
  for (const runId of runIds) {
    const state = eventData[String(runId)];
    if (includes(trigger.failedStates, state)) {
      failedRunIds.push(String(runId));
      continue;
    }
    if (includes(trigger.allowedStates, state)) {
      logs.info("Triggered Dag run finished with allowed state.", {
        dag_id: trigger.dagId,
        state,
        run_id: runId,
      });
    }
  }
  if (failedRunIds.length > 0) {
    return fail(
      `${trigger.dagId} failed with failed states ${JSON.stringify(trigger.failedStates)} ` +
        `for run_ids ${JSON.stringify(failedRunIds)}`,
    );
  }
  return succeeded();
}

/**
 * The payload `DagStateTrigger` fired with: `(classpath, data)`. The triggerer
 * stores it with serde, which encodes a tuple as `{__classname__, __data__}`.
 */
function decodeEvent(event: unknown): Record<string, unknown> | undefined {
  const pair =
    isRecord(event) && event["__classname__"] === "builtins.tuple" ? event["__data__"] : event;
  if (!Array.isArray(pair) || pair.length !== 2 || !isRecord(pair[1])) return undefined;
  return pair[1];
}

function succeeded(): RuntimeSucceedTask {
  return {
    type: "SucceedTask",
    end_date: new Date().toISOString(),
    task_outlets: [],
    outlet_events: [],
  };
}

/** `datetime.isoformat()` of a UTC instant, which is how Python spells a run ID's date. */
export function pythonIsoformat(date: Date): string {
  const iso = date.toISOString();
  const seconds = iso.slice(0, 19);
  const millis = date.getUTCMilliseconds();
  const fraction = millis === 0 ? "" : `.${String(millis * 1000).padStart(6, "0")}`;
  return `${seconds}${fraction}+00:00`;
}

/** `build_airflow_dagrun_url`, on `[api] base_url` from the environment, or "/" when unset. */
export function dagRunUrl(dagId: string, runId: string): string {
  const base = process.env["AIRFLOW__API__BASE_URL"] || "/";
  return `${base.replace(/\/+$/, "")}/dags/${dagId}/runs/${runId}`;
}

function includes(states: readonly string[], state: unknown): boolean {
  return typeof state === "string" && states.includes(state);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
