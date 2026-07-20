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

import { describe, expect, it } from "vitest";
import { asMsgFromSupervisor } from "../../src/coordinator/protocol.js";
import { parseArgs, startCoordinator } from "../../src/coordinator/runtime.js";

describe("protocol decode", () => {
  it("accepts StartupDetails", () => {
    const raw = {
      type: "StartupDetails",
      ti: {
        id: "u",
        dag_version_id: "dag-version-1",
        task_id: "t",
        dag_id: "d",
        run_id: "r",
        try_number: 1,
        map_index: -1,
        hostname: "test-host",
        queue: "default",
      },
      dag_rel_path: "dags/my.mjs",
      bundle_info: { name: "my", version: "1" },
      start_date: "2026-04-23T00:00:00Z",
      ti_context: {},
    };
    expect(asMsgFromSupervisor(raw).type).toBe("StartupDetails");
  });

  it("accepts DagFileParseRequest", () => {
    const raw = {
      type: "DagFileParseRequest",
      file: "/x.mjs",
      bundle_path: "/",
    };
    expect(asMsgFromSupervisor(raw).type).toBe("DagFileParseRequest");
  });

  it("rejects missing type", () => {
    expect(() => asMsgFromSupervisor({ ti: {} })).toThrow(/missing string 'type'/);
  });

  it("rejects unknown type", () => {
    expect(() => asMsgFromSupervisor({ type: "WeirdMsg" })).toThrow(
      /Unsupported supervisor message type/,
    );
  });
});

describe("runtime arg parser", () => {
  it("parses --comm and --logs from argv", () => {
    const r = parseArgs(["node", "bundle.mjs", "--comm=127.0.0.1:5001", "--logs=127.0.0.1:5002"]);
    expect(r.commAddr).toBe("127.0.0.1:5001");
    expect(r.logsAddr).toBe("127.0.0.1:5002");
  });

  it("throws when --comm is missing", () => {
    expect(() => parseArgs(["node", "bundle.mjs", "--logs=127.0.0.1:5002"])).toThrow(
      /Missing --comm/,
    );
  });

  it("throws when --logs is missing", () => {
    expect(() => parseArgs(["node", "bundle.mjs", "--comm=127.0.0.1:5001"])).toThrow(
      /Missing --logs/,
    );
  });

  it("requires commAddr and logsAddr overrides to be supplied together", async () => {
    await expect(
      startCoordinator({
        commAddr: "127.0.0.1:5001",
        argv: ["node", "bundle.mjs", "--logs=127.0.0.1:5002"],
      }),
    ).rejects.toThrow(/Missing --comm/);

    await expect(
      startCoordinator({
        logsAddr: "127.0.0.1:5002",
        argv: ["node", "bundle.mjs", "--comm=127.0.0.1:5001"],
      }),
    ).rejects.toThrow(/Missing --logs/);
  });
});
