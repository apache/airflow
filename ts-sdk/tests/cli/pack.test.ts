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

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  EMBEDDED_LAYOUT_PREFIX,
  EMBEDDED_METADATA_PREFIX,
  encodeBundle,
} from "../../src/cli/bundle-encoder.js";
import { parsePackArgs, runPack } from "../../src/cli/pack.js";
import { SUPERVISOR_API_VERSION } from "../../src/coordinator/protocol.js";
import { AIRFLOW_METADATA_SENTINEL } from "../../src/coordinator/manifest.js";

const FIXTURE_ENTRY = fileURLToPath(new URL("fixtures/entry.ts", import.meta.url));
const GOLDEN_BUNDLE = fileURLToPath(new URL("fixtures/bundle-v1.mjs", import.meta.url));
const NOISY_ENTRY = fileURLToPath(new URL("fixtures/noisy-entry.ts", import.meta.url));
const EMPTY_ENTRY = fileURLToPath(new URL("fixtures/empty-entry.ts", import.meta.url));
const SDK_INDEX = fileURLToPath(new URL("../../src/index.ts", import.meta.url));
const SDK_VERSION = (
  JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf-8")) as {
    version: string;
  }
).version;

interface TestBundleHeader {
  code: { end: string; sha256: string; start: string };
  metadata: { end: string; sha256: string; start: string };
}

function decodeHeader(line: string): TestBundleHeader {
  return JSON.parse(
    Buffer.from(line.slice(EMBEDDED_LAYOUT_PREFIX.length), "base64").toString("utf-8"),
  ) as TestBundleHeader;
}

describe("parsePackArgs", () => {
  it("parses entry with defaults", () => {
    expect(parsePackArgs(["src/main.ts"])).toEqual({
      entry: "src/main.ts",
      outdir: "dist",
      source: "main.ts",
    });
  });

  it("parses --outdir and --source overrides", () => {
    expect(parsePackArgs(["src/main.ts", "--outdir", "build", "--source", "pipeline.ts"])).toEqual({
      entry: "src/main.ts",
      outdir: "build",
      source: "pipeline.ts",
    });
  });

  it.each([
    [[], "Missing entry file"],
    [["--outdir"], "--outdir requires a value"],
    [["a.ts", "b.ts"], "Unexpected argument b.ts"],
    [["a.ts", "--bogus"], "Unknown option --bogus"],
  ])("rejects %j", (argv, message) => {
    expect(() => parsePackArgs(argv)).toThrow(message);
  });
});

describe("encodeBundle", () => {
  it("assembles header, metadata, and executable in physical order", () => {
    const executable = Buffer.from('console.log("hello");\n');
    const bundle = encodeBundle({
      bundleManifest: {
        supervisor_schema_version: "2026-06-16",
        dags: { my_dag: { tasks: ["a", 'b"c'] } },
      },
      sdkVersion: "0.1.0",
      entrypointName: 'we"ird.ts',
      executable,
    });

    const firstNewline = bundle.indexOf("\n");
    const header = decodeHeader(bundle.subarray(0, firstNewline).toString("ascii"));
    const offset = (value: string): number => Number.parseInt(value, 16);
    const metadataStart = offset(header.metadata.start);
    const metadataEnd = offset(header.metadata.end);
    const executableStart = offset(header.code.start);
    const executableEnd = offset(header.code.end);

    expect(metadataStart).toBe(firstNewline + 1 + Buffer.byteLength(EMBEDDED_METADATA_PREFIX));
    expect(executableStart).toBe(metadataEnd + 1);
    expect(executableEnd).toBe(bundle.length);
    expect(bundle.subarray(executableStart, executableEnd)).toEqual(executable);

    const metadata = bundle.subarray(metadataStart, metadataEnd).toString("utf-8");
    expect(metadata).toBe(
      '{"airflow_bundle_metadata_version":"1.0","sdk":{"language":"typescript","version":"0.1.0","supervisor_schema_version":"2026-06-16"},"source":"we\\"ird.ts","dags":{"my_dag":{"tasks":["a","b\\"c"]}}}',
    );

    expect(header).not.toHaveProperty("source");
    expect(header).not.toHaveProperty("version");
    expect(bundle.toString("utf-8")).not.toContain("airflowSource");
  });

  it("matches the golden bundle", () => {
    const executable = readFileSync(EMPTY_ENTRY);
    const bundle = encodeBundle({
      bundleManifest: {
        supervisor_schema_version: "2026-06-16",
        dags: { test_dag: { tasks: ["test_task"] } },
      },
      sdkVersion: "0.1.0",
      entrypointName: "entry.ts",
      executable,
    });

    expect(bundle).toEqual(readFileSync(GOLDEN_BUNDLE));
    const firstNewline = bundle.indexOf("\n");
    const header = decodeHeader(bundle.subarray(0, firstNewline).toString("ascii"));
    for (const section of [header.metadata, header.code]) {
      expect(section.start).toMatch(/^[0-9a-f]{16}$/);
      expect(section.end).toMatch(/^[0-9a-f]{16}$/);
    }
  });

  it("escapes JavaScript line separators inside the metadata comment", () => {
    const bundle = encodeBundle({
      bundleManifest: {
        supervisor_schema_version: "2026-06-16",
        dags: { "line\u2028separator": { tasks: ["paragraph\u2029separator"] } },
      },
      sdkVersion: "0.1.0",
      entrypointName: "entry.ts",
      executable: Buffer.from("export {};\n"),
    });
    const metadataLine = bundle.toString("utf-8").split("\n")[1]!;

    expect(metadataLine).not.toContain("\u2028");
    expect(metadataLine).not.toContain("\u2029");
    expect(metadataLine).toContain("\\u2028");
    expect(metadataLine).toContain("\\u2029");
    expect(JSON.parse(metadataLine.slice(EMBEDDED_METADATA_PREFIX.length))).toHaveProperty(
      "dags.line\u2028separator.tasks",
      ["paragraph\u2029separator"],
    );
  });
});

function readEmbeddedMetadata(bundlePath: string): string {
  const bundle = readFileSync(bundlePath);
  const firstNewline = bundle.indexOf("\n");
  const header = decodeHeader(bundle.subarray(0, firstNewline).toString("utf-8"));
  const start = Number.parseInt(header.metadata.start, 16);
  const end = Number.parseInt(header.metadata.end, 16);
  return bundle.subarray(start, end).toString("utf-8");
}

/** Collect what runPack writes to stderr; returns a reader for the text so far. */
function captureStderr(): () => string {
  const chunks: string[] = [];
  vi.spyOn(process.stderr, "write").mockImplementation((chunk: string | Uint8Array) => {
    chunks.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf-8"));
    return true;
  });
  return () => chunks.join("");
}

describe("runPack", () => {
  let outdir: string;

  afterEach(() => {
    vi.restoreAllMocks();
    if (outdir) rmSync(outdir, { recursive: true, force: true });
  });

  it("bundles the entry and embeds metadata from the bundle's registry", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const nested = path.join(outdir, "dist");
    await runPack([FIXTURE_ENTRY, "--outdir", nested]);

    const bundlePath = path.join(nested, "bundle.mjs");
    expect(existsSync(path.join(nested, "airflow-metadata.yaml"))).toBe(false);

    const [layoutLine, metadataLine] = readFileSync(bundlePath, "utf-8").split("\n");
    expect(layoutLine!.startsWith(EMBEDDED_LAYOUT_PREFIX)).toBe(true);
    expect(metadataLine!.startsWith(EMBEDDED_METADATA_PREFIX)).toBe(true);
    const metadata = JSON.parse(metadataLine!.slice(EMBEDDED_METADATA_PREFIX.length));
    expect(metadata).toEqual({
      airflow_bundle_metadata_version: "1.0",
      sdk: {
        language: "typescript",
        version: SDK_VERSION,
        supervisor_schema_version: SUPERVISOR_API_VERSION,
      },
      source: "entry.ts",
      dags: {
        fixture_dag: { tasks: ["extract", "transform"] },
        other_dag: { tasks: ["solo"] },
      },
    });

    const dumped = execFileSync(process.execPath, [bundlePath, "--airflow-metadata"], {
      encoding: "utf-8",
    });
    expect(dumped.startsWith(AIRFLOW_METADATA_SENTINEL)).toBe(true);
    expect(
      JSON.parse(dumped.slice(AIRFLOW_METADATA_SENTINEL.length)).supervisor_schema_version,
    ).toBe(SUPERVISOR_API_VERSION);
  });

  it("embeds verifiable metadata and code regions", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    await runPack([FIXTURE_ENTRY, "--outdir", outdir]);

    const bundle = readFileSync(path.join(outdir, "bundle.mjs"));
    const firstNewline = bundle.indexOf("\n");
    const layoutLine = bundle.subarray(0, firstNewline).toString("utf-8");
    expect(layoutLine.startsWith(EMBEDDED_LAYOUT_PREFIX)).toBe(true);

    const layout = decodeHeader(layoutLine);
    const offset = (value: string): number => Number.parseInt(value, 16);

    expect(layout).not.toHaveProperty("version");
    const code = bundle.subarray(offset(layout.code.start), offset(layout.code.end));
    expect(createHash("sha256").update(code).digest("hex")).toBe(layout.code.sha256);

    const metadataPayload = bundle.subarray(
      offset(layout.metadata.start),
      offset(layout.metadata.end),
    );
    expect(createHash("sha256").update(metadataPayload).digest("hex")).toBe(layout.metadata.sha256);
    expect(JSON.parse(metadataPayload.toString("utf-8"))).toHaveProperty("dags.fixture_dag");
    expect(layout).not.toHaveProperty("source");
    expect(bundle.toString("utf-8")).not.toContain("airflowSource");
  });

  it("keeps a shebang entry runnable and reads the manifest past import-time logging", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    await runPack([NOISY_ENTRY, "--outdir", outdir]);

    const bundlePath = path.join(outdir, "bundle.mjs");
    const bundle = readFileSync(bundlePath, "utf-8");
    expect(bundle.startsWith(EMBEDDED_LAYOUT_PREFIX)).toBe(true);
    expect(bundle).not.toContain("#!/usr/bin/env node");
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);

    const metadataLine = bundle.split("\n")[1]!;
    const metadata = JSON.parse(metadataLine.slice(EMBEDDED_METADATA_PREFIX.length));
    expect(metadata).toHaveProperty("dags.noisy_dag");

    execFileSync(process.execPath, [bundlePath, "--airflow-metadata"], { encoding: "utf-8" });
  });

  it("leaves no bundle behind when the metadata exceeds the embedded size limit", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "huge-entry.ts");
    writeFileSync(
      entry,
      [
        `import { Dag, DagRegistry, serveDags } from ${JSON.stringify(SDK_INDEX)};`,
        'const bigDag = new Dag("big_dag");',
        'for (let i = 0; i < 5000; i += 1) bigDag.task(String(i).padStart(240, "t"), async () => undefined);',
        "await serveDags(new DagRegistry(bigDag));",
      ].join("\n"),
    );

    await expect(runPack([entry, "--outdir", outdir])).rejects.toThrow(
      "over the 1048576 byte limit",
    );
    expect(existsSync(path.join(outdir, "bundle.mjs"))).toBe(false);
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);
  });

  it("leaves no bundle behind when the entry serves no Dags", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));

    await expect(runPack([EMPTY_ENTRY, "--outdir", outdir])).rejects.toThrow("served no Dags");
    expect(existsSync(path.join(outdir, "bundle.mjs"))).toBe(false);
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);
  });

  it.each([
    {
      label: "Dag",
      dagId: "bad id!",
      taskId: "valid_task",
      expected:
        'warning: dag id "bad id!" must be made of alphanumeric characters, dashes, dots, and underscores; the Airflow server will reject it\n',
    },
    {
      label: "task",
      dagId: "valid_dag",
      taskId: "bad id!",
      expected:
        'warning: task id "bad id!" in dag "valid_dag" must be made of alphanumeric characters, dashes, dots, and underscores; the Airflow server will reject it\n',
    },
  ])("warns on a suspicious $label ID but still packs", async ({ dagId, taskId, expected }) => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "suspicious-id-entry.ts");
    writeFileSync(
      entry,
      [
        `import { Dag, DagRegistry, serveDags } from ${JSON.stringify(SDK_INDEX)};`,
        `const suspiciousDag = new Dag(${JSON.stringify(dagId)});`,
        `suspiciousDag.task(${JSON.stringify(taskId)}, async () => undefined);`,
        "await serveDags(new DagRegistry(suspiciousDag));",
      ].join("\n"),
    );
    const stderr = captureStderr();

    await runPack([entry, "--outdir", outdir]);

    expect(stderr()).toContain(expected);
    expect(existsSync(path.join(outdir, "bundle.mjs"))).toBe(true);
  });

  it("reports the last error from a failed bundle", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "multiple-errors-entry.ts");
    writeFileSync(
      entry,
      ['console.error("Error: earlier failure");', 'throw new Error("final failure");'].join("\n"),
    );

    await expect(runPack([entry, "--outdir", outdir])).rejects.toHaveProperty(
      "message",
      "Error: final failure",
    );
    expect(existsSync(path.join(outdir, "bundle.mjs"))).toBe(false);
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);
  });

  // A bundle can print the sentinel itself, so nothing on that line is trusted.
  it.each([
    ['{ supervisor_schema_version: "1", dags: { broken_dag: {} } }', "malformed entry"],
    [
      '{ supervisor_schema_version: "1", dags: { broken_dag: { tasks: ["ok", 7] } } }',
      "malformed entry",
    ],
    [
      '{ supervisor_schema_version: "1", dags: { broken_dag: { tasks: [""] } } }',
      "malformed entry",
    ],
    ['{ supervisor_schema_version: "1", dags: [{ tasks: ["a"] }] }', "incomplete"],
    // Was read off before the document itself was checked, so it surfaced as a
    // raw TypeError.
    ["null", "incomplete"],
    // Truthy, but not the non-empty string the schema requires.
    ['{ supervisor_schema_version: true, dags: { d: { tasks: ["a"] } } }', "incomplete"],
    ['{ supervisor_schema_version: 20260616, dags: { d: { tasks: ["a"] } } }', "incomplete"],
  ])("rejects the metadata line %s", async (manifest, message) => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "malformed-entry.ts");
    writeFileSync(
      entry,
      `console.log(${JSON.stringify(AIRFLOW_METADATA_SENTINEL)} + JSON.stringify(${manifest}));`,
    );

    await expect(runPack([entry, "--outdir", outdir])).rejects.toThrow(message);
    expect(existsSync(path.join(outdir, "bundle.mjs"))).toBe(false);
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);
  });

  it("warns but still packs a registered Dag with no tasks, as airflow-go-pack does", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "mixed-entry.ts");
    writeFileSync(
      entry,
      [
        `import { Dag, DagRegistry, serveDags } from ${JSON.stringify(SDK_INDEX)};`,
        'const salesDag = new Dag("sales_dag");',
        'salesDag.task("extract", async () => undefined);',
        'await serveDags(new DagRegistry(salesDag, new Dag("empty_dag")));',
      ].join("\n"),
    );
    const stderr = captureStderr();

    await runPack([entry, "--outdir", outdir]);

    expect(stderr()).toContain('warning: dag "empty_dag" has no tasks\n');
    expect(JSON.parse(readEmbeddedMetadata(path.join(outdir, "bundle.mjs")))).toHaveProperty(
      "dags.empty_dag.tasks",
      [],
    );
  });

  it("packs only the Dags the served registry holds", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "forgotten-entry.ts");
    writeFileSync(
      entry,
      [
        `import { Dag, DagRegistry, serveDags } from ${JSON.stringify(SDK_INDEX)};`,
        'const salesDag = new Dag("sales_dag");',
        'salesDag.task("extract", async () => undefined);',
        'const billingDag = new Dag("billing_dag");',
        'billingDag.task("charge", async () => undefined);',
        "await serveDags(new DagRegistry(salesDag));",
      ].join("\n"),
    );

    await runPack([entry, "--outdir", outdir]);

    const metadata = JSON.parse(readEmbeddedMetadata(path.join(outdir, "bundle.mjs")));
    expect(metadata).toHaveProperty("dags.sales_dag");
    expect(metadata).not.toHaveProperty("dags.billing_dag");
  });
});
