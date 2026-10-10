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

import { randomInt } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { isPlainRecord } from "./dag.js";
import type { OperatorContext, OperatorOutcome } from "./operator.js";
import type { TriggerDagRunTask } from "./trigger-dag-run.js";

const DAG_STATE_TRIGGER = "airflow.providers.standard.triggers.external_task.DagStateTrigger";
/** `TriggerDagRunLink().xcom_key`: the XCom the "Triggered DAG" extra link reads. */
const LINK_XCOM_KEY = "_link_TriggerDagRunLink";
const RUN_ID_XCOM_KEY = "trigger_run_id";

export async function executeTriggerDagRun(
  trigger: TriggerDagRunTask,
  op: OperatorContext,
): Promise<OperatorOutcome> {
  const { client, logs } = op;
  const logicalDate =
    trigger.logicalDate === undefined
      ? trigger.runAfter === undefined
        ? new Date()
        : null
      : trigger.logicalDate;
  const runAfter = trigger.runAfter?.toISOString();
  const runId =
    trigger.runId ??
    `manual__${pythonIsoformat(runAfter ? trigger.runAfter! : (logicalDate ?? new Date()))}` +
      (logicalDate === null ? `_${randomString(8)}` : "");

  if (trigger.failWhenDagIsPaused && (await client.isDagPaused(trigger.dagId))) {
    return op.fail(`Dag ${trigger.dagId} is paused`);
  }

  logs.info("Triggering Dag Run.", { trigger_dag_id: trigger.dagId });
  const [, triggered] = await Promise.all([
    client.setXCom({ key: LINK_XCOM_KEY, value: dagRunUrl(trigger.dagId, runId) }),
    client.triggerDagRun({
      dag_id: trigger.dagId,
      run_id: runId,
      logical_date: logicalDate?.toISOString() ?? null,
      ...(runAfter === undefined ? {} : { run_after: runAfter }),
      conf: (trigger.conf as Record<string, unknown> | undefined) ?? null,
      reset_dag_run: trigger.resetDagRun,
      note: trigger.note ?? null,
    }),
  ]);
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
    return op.succeed();
  }

  if (trigger.deferrable) {
    logs.info("Pausing task as DEFERRED.", { trigger_dag_id: trigger.dagId, run_id: runId });
    return op.defer({
      classpath: DAG_STATE_TRIGGER,
      // `DagStateTrigger.serialize()`, key for key.
      kwargs: {
        dag_id: trigger.dagId,
        states: [...trigger.allowedStates, ...trigger.failedStates],
        poll_interval: trigger.pokeInterval,
        run_ids: [runId],
        execution_dates: null,
      },
    });
  }

  while (true) {
    logs.info("Waiting for dag run to complete execution in allowed state.", {
      dag_id: trigger.dagId,
      run_id: runId,
      allowed_state: trigger.allowedStates,
    });
    await sleep(trigger.pokeInterval * 1000, undefined, { signal: op.ctx.signal });
    const state = await client.getDagRunState(trigger.dagId, runId);
    if (includes(trigger.failedStates, state)) {
      logs.error("DagRun finished with failed state.", { dag_id: trigger.dagId, state });
      return op.fail(`${trigger.dagId} failed with failed state ${state}`);
    }
    if (includes(trigger.allowedStates, state)) {
      logs.info("DagRun finished with allowed state.", { dag_id: trigger.dagId, state });
      return op.succeed();
    }
    logs.debug("DagRun not yet in allowed or failed state.", { dag_id: trigger.dagId, state });
  }
}

/** The run `DagStateTrigger` resumes the task with, after the triggered run reached a state. */
export async function resumeTriggerDagRun(
  trigger: TriggerDagRunTask,
  op: OperatorContext,
  event: unknown,
): Promise<OperatorOutcome> {
  const eventData = decodeEvent(event);
  const runIds = eventData?.["run_ids"];
  if (eventData === undefined || !Array.isArray(runIds)) {
    return op.fail(`Task resumed with an event it cannot read: ${JSON.stringify(event)}`);
  }
  const failedRunIds: string[] = [];
  for (const runId of runIds) {
    const state = eventData[String(runId)];
    if (includes(trigger.failedStates, state)) {
      failedRunIds.push(String(runId));
      continue;
    }
    if (includes(trigger.allowedStates, state)) {
      op.logs.info("Triggered Dag run finished with allowed state.", {
        dag_id: trigger.dagId,
        state,
        run_id: runId,
      });
    }
  }
  if (failedRunIds.length > 0) {
    return op.fail(
      `${trigger.dagId} failed with failed states ${JSON.stringify(trigger.failedStates)} ` +
        `for run_ids ${JSON.stringify(failedRunIds)}`,
    );
  }
  return op.succeed();
}

/**
 * The payload `DagStateTrigger` fired with: `(classpath, data)`. The triggerer
 * stores it with serde, which encodes a tuple as `{__classname__, __data__}`.
 */
function decodeEvent(event: unknown): Record<string, unknown> | undefined {
  const pair =
    isPlainRecord(event) && event["__classname__"] === "builtins.tuple" ? event["__data__"] : event;
  if (!Array.isArray(pair) || pair.length !== 2 || !isPlainRecord(pair[1])) return undefined;
  return pair[1];
}

/** `datetime.isoformat()` of a UTC instant, which is how Python spells a run ID's date. */
function pythonIsoformat(date: Date): string {
  const iso = date.toISOString();
  const seconds = iso.slice(0, 19);
  const millis = date.getUTCMilliseconds();
  const fraction = millis === 0 ? "" : `.${String(millis * 1000).padStart(6, "0")}`;
  return `${seconds}${fraction}+00:00`;
}

function randomString(length: number): string {
  const choices = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  return Array.from({ length }, () => choices[randomInt(choices.length)]).join("");
}

/** `build_airflow_dagrun_url`, on `[api] base_url` from the environment, or "/" when unset. */
function dagRunUrl(dagId: string, runId: string): string {
  const base = process.env["AIRFLOW__API__BASE_URL"] || "/";
  return `${base.replace(/\/+$/, "")}/dags/${dagId}/runs/${runId}`;
}

function includes(states: readonly string[], state: unknown): boolean {
  return typeof state === "string" && states.includes(state);
}
