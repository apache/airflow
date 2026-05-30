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

// Worker entry point. Composes clients + registry + poll loop.
// TODO(pr2): per-TI heartbeat, token refresh, state-transition retry loop.
// TODO(pr3): XCom / Variables / Connections in TaskHandlerArgs.
// TODO(pr4): log forwarding to Edge API.
// TODO(pr5): subprocess isolation per task.

import { userInfo } from "node:os";
import {
    makeEdgeClient,
    WORKER_STATE,
    JOB_STATE,
    type EdgeClient,
    type WorkerStateBody,
    type EdgeJobFetched,
} from "./edge-client.js";
import { makeExecutionClient } from "./execution-client.js";
import { ExecutionApiError, formatError } from "./errors.js";
import { getRegisteredTask } from "./registry.js";
import type { StartWorkerOptions, TaskContext } from "./types.js";
import { sleepAbortable } from "./utils/retry.js";
import { resolveWorkerOptions } from "./worker-options.js";

const AIRFLOW_VERSION = process.env.AIRFLOW__EDGE__AIRFLOW_VERSION ?? "3.3.0";
const EDGE_PROVIDER_VERSION = process.env.AIRFLOW__EDGE__PROVIDER_VERSION ?? "3.5.0";

/** Start the worker. Resolves when the worker has shut down cleanly */
export async function startWorker(opts: StartWorkerOptions): Promise<void> {
    const cfg = resolveWorkerOptions(opts);
    const edge = makeEdgeClient({
        baseUrl: cfg.baseUrl,
        secret: cfg.secret,
        workerName: cfg.workerName,
        jwtIssuer: cfg.jwtIssuer,
    });

    // Worker state shared across heartbeatLoop + pollLoop.
    let jobsActive = 0;
    let heartbeatFailures = 0;
    const ac = new AbortController();

    const makeWorkerState = (state: WorkerStateBody["state"]): WorkerStateBody => ({
        state,
        jobs_active: jobsActive,
        queues: cfg.queues,
        sysinfo: {
            airflow_version: AIRFLOW_VERSION,
            edge_provider_version: EDGE_PROVIDER_VERSION,
            // Single-task worker. Concurrent dispatch deferred (see TODOs at top of file).
            concurrency: 1,
            free_concurrency: jobsActive > 0 ? 0 : 1,
        },
    });

    await edge.register(makeWorkerState(WORKER_STATE.STARTING));
    console.log(`[worker] ${cfg.workerName} registered on queues=${cfg.queues.join(",")}`);

    const onSignal = () => {
        if (!ac.signal.aborted) {
            console.log("[worker] signal received, initiating drain");
            ac.abort();
        }
    };
    process.on("SIGTERM", onSignal);
    process.on("SIGINT", onSignal);

    const heartbeatLoop = async (): Promise<void> => {
        while (!ac.signal.aborted) {
            await sleepAbortable(cfg.heartbeatIntervalInMs, ac.signal).catch(() => undefined);
            if (ac.signal.aborted) break;
            try {
                // RUNNING if busy with a job; IDLE if waiting (Go SDK _currentState pattern).
                const state = jobsActive > 0 ? WORKER_STATE.RUNNING : WORKER_STATE.IDLE;
                const resp = await edge.updateState(makeWorkerState(state));
                heartbeatFailures = 0; // reset on any success
                if (resp?.state === WORKER_STATE.SHUTDOWN_REQUEST) {
                    console.log("[worker] server requested shutdown");
                    ac.abort();
                    break;
                }
            } catch (err) {
                heartbeatFailures += 1;
                console.error(
                    `[worker] heartbeat failed (${heartbeatFailures}/${cfg.heartbeatFailureThreshold}): ${formatError(err)}`,
                );
                if (heartbeatFailures >= cfg.heartbeatFailureThreshold) {
                    console.error("[worker] circuit breaker tripped — aborting");
                    ac.abort();
                    break;
                }
            }
        }
    };

    const pollLoop = async (): Promise<void> => {
        while (!ac.signal.aborted) {
            await sleepAbortable(cfg.pollIntervalInMs, ac.signal).catch(() => undefined);
            if (ac.signal.aborted) break;

            // Single-task: executeJob is awaited, so jobsActive is binary 0/1.
            let job: EdgeJobFetched | null;
            try {
                job = await edge.fetchJob({
                    queues: cfg.queues,
                    free_concurrency: jobsActive > 0 ? 0 : 1,
                });
            } catch (err) {
                console.error(`[worker] poll failed: ${formatError(err)}`);
                continue;
            }
            if (!job) continue; // queue empty

            jobsActive += 1;
            try {
                await executeJob(job, {
                    baseUrl: cfg.baseUrl,
                    workerName: cfg.workerName,
                    edge,
                    signal: ac.signal,
                });
            } catch (err) {
                if (err instanceof ExecutionApiError && (err.isNotFound || err.status === 409)) {
                    console.log(`[worker] TI not runnable (${err.status}); ack failed`);
                } else {
                    console.error(`[worker] executeJob crashed: ${formatError(err)}`);
                }
                try {
                    await ackEdgeJob(edge, job, JOB_STATE.FAILED); // every fetched job must be acked
                } catch (ackErr) {
                    console.error(`[worker] failsafe ack failed: ${formatError(ackErr)}`);
                }
            } finally {
                jobsActive -= 1;
            }
        }
    };

    await Promise.all([heartbeatLoop(), pollLoop()]);

    // Drain: final offline PATCH — best-effort; don't block exit on failure.
    try {
        await edge.updateState(makeWorkerState(WORKER_STATE.TERMINATING));
    } catch (err) {
        console.error(`[worker] terminating PATCH failed: ${formatError(err)}`);
    }
    try {
        await edge.updateState(makeWorkerState(WORKER_STATE.OFFLINE));
        console.log("[worker] offline, bye");
    } catch (err) {
        console.error(`[worker] offline PATCH failed: ${formatError(err)}`);
    }
}

interface ExecuteJobContext {
    baseUrl: string;
    workerName: string;
    edge: EdgeClient;
    signal: AbortSignal;
}

async function executeJob(job: EdgeJobFetched, ctx: ExecuteJobContext): Promise<void> {
    const tiId = job.command.ti.id;
    const label = `${job.dag_id}/${job.task_id}/${job.run_id} try=${job.try_number}`;
    console.log(`[executeJob] ${label} ti=${tiId}`);

    const exec = makeExecutionClient({
        baseUrl: ctx.baseUrl,
        taskInstanceId: tiId,
        token: job.command.token,
    });

    // 1. Transition TI to running. Any error propagates to pollLoop's
    //    failsafe so the edge job is always acked, even on crash.
    await exec.enterRunning({
        hostname: ctx.workerName,
        unixname: userInfo().username,
        pid: process.pid,
        start_date: new Date().toISOString(),
    });

    // 2. Look up the handler.
    const handler = getRegisteredTask(job.task_id);
    if (!handler) {
        console.error(`[executeJob] no handler registered for task_id="${job.task_id}"`);
        await exec.markFailed({ end_date: new Date().toISOString() });
        await ackEdgeJob(ctx.edge, job, JOB_STATE.FAILED);
        return;
    }

    // 3. Invoke handler.
    const taskContext: TaskContext = {
        dagId: job.dag_id,
        taskId: job.task_id,
        runId: job.run_id,
        tryNumber: job.try_number,
        mapIndex: job.map_index,
        taskInstanceId: tiId,
        signal: ctx.signal,
    };
    // 4. Run handler, mark TI, ack edge. Mark/ack failures propagate to the
    //    pollLoop failsafe — handler-success + mark-failure naturally
    //    downgrades to ack-failed (so edge state always converges).
    try {
        await handler({ ctx: taskContext, job });
    } catch (err) {
        console.error(`[executeJob] ${label} handler threw: ${formatError(err)}`);
        await exec.markFailed({ end_date: new Date().toISOString() });
        await ackEdgeJob(ctx.edge, job, JOB_STATE.FAILED);
        console.log(`[executeJob] ${label} → failed`);
        return;
    }

    await exec.markSuccess({ end_date: new Date().toISOString() });
    await ackEdgeJob(ctx.edge, job, JOB_STATE.SUCCESS);
    console.log(`[executeJob] ${label} → success`);
}

/** Ack the edge job's terminal state — pure path-param translation around
 *  `edge.reportJobState`. Errors propagate so the pollLoop failsafe knows
 *  the regular ack failed and can attempt a failsafe ack. */
async function ackEdgeJob(
    edge: EdgeClient,
    job: EdgeJobFetched,
    state: "success" | "failed",
): Promise<void> {
    await edge.reportJobState({
        dagId: job.dag_id,
        taskId: job.task_id,
        runId: job.run_id,
        tryNumber: job.try_number,
        mapIndex: job.map_index,
        state,
    });
}
