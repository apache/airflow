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

// Coordinator runtime entrypoint.
//
// Invoked by Airflow's coordinator subprocess path:
//
//     node my-bundle.mjs --comm=host:port --logs=host:port
//
// where `my-bundle.mjs` is a user-bundled Node script that imports
// the SDK, calls `registerTask(...)` for each handler, then calls
// `startCoordinator()`.
//
// Lifecycle:
//   1. Parse --comm / --logs from argv
//   2. Connect both TCP sockets
//   3. Read the first frame from comm:
//        - DagFileParseRequest → respond with DagFileParsingResult, exit
//        - StartupDetails      → run task, respond Succeed or Fail, exit
//
import { createCoordinatorClient } from "./client.js";
import { CommChannel } from "./comm-channel.js";
import { LogChannel } from "./log-channel.js";
import {
  AIRFLOW_METADATA_FLAG,
  AIRFLOW_METADATA_SENTINEL,
  buildBundleManifest,
} from "./manifest.js";
import {
  asMsgFromSupervisor,
  SUPERVISOR_API_VERSION,
  type RuntimeDagFileParsingResult,
  type RuntimeRetryTask,
  type RuntimeSucceedTask,
  type RuntimeTaskState,
  type StartupDetails,
} from "./protocol.js";
import { getRegisteredTask, listRegisteredTasks } from "../sdk/registry.js";
import type { TaskContext, TaskHandlerArgs } from "../sdk/task.js";
import type { JsonValue } from "../sdk/client-types.js";

export const ABORT_GRACE_PERIOD_MS = 30_000;
export const COORDINATOR_RESPONSE_TIMEOUT_MS = 30_000;

/** Options for `startCoordinator()`. */
export interface StartCoordinatorOptions {
  /** Comm socket address (host:port). Must be supplied together with `logsAddr`; otherwise parsed from argv. */
  commAddr?: string;
  /** Logs socket address (host:port). Must be supplied together with `commAddr`; otherwise parsed from argv. */
  logsAddr?: string;
  /** Source argv. Defaults to `process.argv`. */
  argv?: readonly string[];
}

interface ParsedArgs {
  commAddr: string;
  logsAddr: string;
}

type SignalListener = NodeJS.SignalsListener;

interface ProcessSignalSource {
  on(signal: NodeJS.Signals, listener: SignalListener): ProcessSignalSource;
  off(signal: NodeJS.Signals, listener: SignalListener): ProcessSignalSource;
}

interface RuntimeAbortOptions {
  signalSource?: ProcessSignalSource;
  exitProcess?: (code: number) => never;
}

export interface RuntimeAbort {
  readonly signal: AbortSignal;
  dispose(): void;
}

const ABORT_SIGNALS: readonly NodeJS.Signals[] = ["SIGTERM", "SIGINT"];
const ABORT_FORCE_EXIT_CODE = 1;

export function parseArgs(argv: readonly string[]): ParsedArgs {
  let commAddr: string | null = null;
  let logsAddr: string | null = null;
  for (const arg of argv) {
    if (arg.startsWith("--comm=")) {
      commAddr = arg.slice("--comm=".length);
    } else if (arg.startsWith("--logs=")) {
      logsAddr = arg.slice("--logs=".length);
    }
  }
  if (!commAddr) throw new Error("Missing --comm=host:port");
  if (!logsAddr) throw new Error("Missing --logs=host:port");
  return { commAddr, logsAddr };
}

/** Start the coordinator runtime. Resolves when the subprocess has
 *  delivered its terminal frame and closed both sockets. */
export async function startCoordinator(opts: StartCoordinatorOptions = {}): Promise<void> {
  const argv = opts.argv ?? process.argv;
  if (argv.includes(AIRFLOW_METADATA_FLAG)) {
    process.stdout.write(`${AIRFLOW_METADATA_SENTINEL}${JSON.stringify(buildBundleManifest())}\n`);
    return;
  }
  const parsed =
    opts.commAddr && opts.logsAddr
      ? { commAddr: opts.commAddr, logsAddr: opts.logsAddr }
      : parseArgs(argv);

  let logs: LogChannel | null = null;
  let comm: CommChannel | null = null;
  let runtimeAbort: RuntimeAbort | null = null;

  try {
    // Connect log channel first so early failures are captured.
    // Root logger is `ts-sdk`; subsystems use child names (`ts-sdk.runtime`,
    // `ts-sdk.comm`, `ts-sdk.client`) so structlog's ConsoleRenderer prints
    // them as a distinct `[name]` column on the supervisor side.
    logs = await LogChannel.connect(parsed.logsAddr);
    const runtimeLogs = logs.child("runtime");
    const tasks = listRegisteredTasks();
    runtimeLogs.info("Coordinator runtime started", {
      registered_tasks: tasks,
      count: tasks.length,
      // Cadwyn schema version this SDK was generated against. Logged
      // for operator visibility; not sent on the wire.
      supervisor_api_version: SUPERVISOR_API_VERSION,
    });

    const connection = await CommChannel.connect(parsed.commAddr, logs.child("comm"));
    comm = connection.channel;
    const firstFrame = connection.firstFrame;
    runtimeLogs.debug("Connected comm socket", { commAddr: parsed.commAddr });
    runtimeAbort = createRuntimeAbort(runtimeLogs);

    const body = asMsgFromSupervisor(firstFrame.body);

    if (body.type === "DagFileParseRequest") {
      runtimeLogs.info("Received Dag parse request", {
        file: body.file,
        bundle_path: body.bundle_path,
      });
      const response = handleParse(body, runtimeLogs);
      await sendSupervisorResponse(firstFrame.id, response, comm, runtimeLogs);
    } else if (body.type === "StartupDetails") {
      runtimeLogs.info("Received task startup details", {
        dag_id: body.ti.dag_id,
        task_id: body.ti.task_id,
        run_id: body.ti.run_id,
        try_number: body.ti.try_number,
        map_index: body.ti.map_index ?? -1,
        bundle: body.bundle_info.name,
      });
      const response = await handleTask(
        body,
        comm,
        runtimeLogs,
        logs.child("client"),
        runtimeAbort.signal,
      );
      await sendSupervisorResponse(firstFrame.id, response, comm, runtimeLogs);
      if (response.type === "SucceedTask") {
        runtimeLogs.info("Task succeeded", { task_id: body.ti.task_id });
      }
    } else {
      const errMsg = `First frame must be DagFileParseRequest or StartupDetails, got ${body.type}`;
      runtimeLogs.error("Unexpected first frame", { type: body.type });
      await sendSupervisorResponse(firstFrame.id, null, comm, runtimeLogs, {
        error: "protocol_error",
        detail: errMsg,
      });
    }
  } finally {
    runtimeAbort?.dispose();
    await comm?.close();
    await logs?.close();
  }
}

export function createRuntimeAbort(
  logs: LogChannel | null,
  opts: RuntimeAbortOptions = {},
): RuntimeAbort {
  const controller = new AbortController();
  const signalSource = opts.signalSource ?? process;
  const exitProcess = opts.exitProcess ?? ((code: number): never => process.exit(code));
  let disposed = false;
  let forceExitTimer: ReturnType<typeof setTimeout> | null = null;
  const signalListeners = new Map<NodeJS.Signals, SignalListener>();

  const handleSignal = (signal: NodeJS.Signals): void => {
    if (disposed) return;
    if (controller.signal.aborted) {
      logs?.warning("Additional abort signal received", { signal });
      return;
    }

    logs?.warning("Abort signal received", {
      signal,
      grace_period_ms: ABORT_GRACE_PERIOD_MS,
    });
    controller.abort(new Error(`Task aborted by ${signal}`));
    forceExitTimer = setTimeout(() => {
      logs?.error("Abort grace period expired; forcing process exit", {
        signal,
        grace_period_ms: ABORT_GRACE_PERIOD_MS,
      });
      exitProcess(ABORT_FORCE_EXIT_CODE);
    }, ABORT_GRACE_PERIOD_MS);
  };

  for (const signal of ABORT_SIGNALS) {
    const listener: SignalListener = () => handleSignal(signal);
    signalListeners.set(signal, listener);
    signalSource.on(signal, listener);
  }

  return {
    signal: controller.signal,
    dispose: () => {
      disposed = true;
      for (const [signal, listener] of signalListeners) {
        signalSource.off(signal, listener);
      }
      signalListeners.clear();
      if (forceExitTimer !== null) {
        clearTimeout(forceExitTimer);
        forceExitTimer = null;
      }
    },
  };
}

function handleParse(
  request: { file: string; bundle_path: string },
  logs: LogChannel,
): RuntimeDagFileParsingResult {
  // TypeScript-native Dag parsing is not yet supported.
  // Respond with an empty result so the Python-stub-Dag workflow works.
  logs.info("Parse-mode response (TS Dag parsing not yet supported)", {
    registered_tasks: listRegisteredTasks(),
  });
  const response: RuntimeDagFileParsingResult = {
    type: "DagFileParsingResult",
    fileloc: request.file,
    serialized_dags: [],
  };
  return response;
}

async function handleTask(
  details: StartupDetails,
  comm: CommChannel,
  logs: LogChannel,
  clientLogs: LogChannel,
  signal: AbortSignal,
): Promise<RuntimeSucceedTask | RuntimeRetryTask | RuntimeTaskState> {
  const ti = details.ti;
  const handler = getRegisteredTask(ti.dag_id, ti.task_id);

  if (!handler) {
    logs.warning("No handler registered for task", {
      dag_id: ti.dag_id,
      task_id: ti.task_id,
      available: listRegisteredTasks(),
    });
    // A missing handler means this bundle cannot run the task, so retrying the
    // same bundle/configuration mismatch would not help.
    const response: RuntimeTaskState = {
      type: "TaskState",
      state: "removed",
      end_date: new Date().toISOString(),
    };
    return response;
  }

  const ctx = buildContext(details, signal);
  const client = createCoordinatorClient(comm, ctx, clientLogs);
  const args: TaskHandlerArgs = { ctx, client };
  // Startup-details fields already logged above (`Received task
  // startup details`); this line just marks the handler-call boundary.
  logs.debug("Dispatching to handler", { task_id: ctx.taskId });

  try {
    const result = await handler(args);
    if (result !== undefined) {
      await client.setXCom({ key: "return_value", value: result as JsonValue });
    }
    // SucceedTask MUST include task_outlets and outlet_events as
    // empty lists — the Execution API's TISuccessStatePayload
    // tagged-union validator rejects null for these fields.
    const response: RuntimeSucceedTask = {
      type: "SucceedTask",
      end_date: new Date().toISOString(),
      task_outlets: [],
      outlet_events: [],
    };
    return response;
  } catch (err) {
    const message = (err as Error).message ?? String(err);
    logs.error("Task failed", {
      task_id: ctx.taskId,
      error: message,
      stack: (err as Error).stack ?? null,
    });
    return buildFailureResponse(details, message);
  }
}

async function sendSupervisorResponse(
  id: number,
  body: unknown,
  comm: CommChannel,
  logs: LogChannel,
  error?: unknown,
): Promise<void> {
  try {
    await comm.sendResponse(id, body, error, { timeoutMs: COORDINATOR_RESPONSE_TIMEOUT_MS });
  } catch (err) {
    logs.error("Failed to send response to supervisor", {
      response_type: responseType(body),
      error: (err as Error).message ?? String(err),
    });
    throw err;
  }
}

function buildFailureResponse(
  details: StartupDetails,
  message: string,
): RuntimeRetryTask | RuntimeTaskState {
  const endDate = new Date().toISOString();
  if (details.ti_context.should_retry) {
    return {
      type: "RetryTask",
      end_date: endDate,
      retry_reason: message.slice(0, 500),
    };
  }
  return {
    type: "TaskState",
    state: "failed",
    end_date: endDate,
  };
}

function responseType(body: unknown): string {
  if (body && typeof body === "object" && "type" in body) {
    const type = (body as { type?: unknown }).type;
    if (typeof type === "string") return type;
  }
  return "unknown";
}

function buildContext(details: StartupDetails, signal: AbortSignal): TaskContext {
  return {
    dagId: details.ti.dag_id,
    taskId: details.ti.task_id,
    runId: details.ti.run_id,
    tryNumber: details.ti.try_number,
    mapIndex: details.ti.map_index ?? -1,
    signal,
  };
}
