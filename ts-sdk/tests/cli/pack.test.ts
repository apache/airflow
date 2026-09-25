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
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  EMBEDDED_LAYOUT_PREFIX,
  EMBEDDED_METADATA_PREFIX,
  EMBEDDED_SOURCE_CLOSE,
  EMBEDDED_SOURCE_MARKER,
  encodeBundle,
} from "../../src/cli/bundle-encoder.js";
import { parsePackArgs, runPack } from "../../src/cli/pack.js";
import { SUPERVISOR_API_VERSION } from "../../src/coordinator/protocol.js";
import { AIRFLOW_METADATA_SENTINEL } from "../../src/coordinator/manifest.js";

const FIXTURE_ENTRY = fileURLToPath(new URL("fixtures/entry.ts", import.meta.url));
const GOLDEN_BUNDLE = fileURLToPath(new URL("fixtures/bundle-v1.min.mjs", import.meta.url));
const NOISY_ENTRY = fileURLToPath(new URL("fixtures/noisy-entry.ts", import.meta.url));
const EMPTY_ENTRY = fileURLToPath(new URL("fixtures/empty-entry.ts", import.meta.url));
const SDK_INDEX = fileURLToPath(new URL("../../src/index.ts", import.meta.url));

// Shaped like what airflow-ts-pack ships: a code region nobody could read, and a source region
// that needs escaping. Real esbuild output is not used here because the assertion is byte-exact
// and would churn on every esbuild bump. The runPack tests cover real output.
const GOLDEN_CODE = Buffer.from(
  [
    'var e=async function(){return"extracted"};await e();',
    // esbuild relocates dependencies' banners to the end, putting a comment terminator in the
    // code region. Only the source region may not hold one, and the fixture pins that.
    "/*! Licensed to the Apache Software Foundation (ASF) under one or more",
    " * contributor license agreements. See the NOTICE file distributed with",
    " * this work for additional information regarding copyright ownership.",
    " */",
    "",
  ].join("\n"),
);

// Carries both escape branches: the doc comment ends in a terminator, and the regex holds a star
// followed by a backslash. The Python reader is then validated against real encoder output.
const GOLDEN_SOURCE = [
  "/** Handlers for the test Dag. */",
  'import { Bundle, Dag } from "apache-airflow-ts-sdk";',
  "",
  "const TERMINATOR = /\\*\\//;",
  'const dag = new Dag("test_dag");',
  'dag.task("test_task", async () => TERMINATOR.source);',
  "",
  "await new Bundle(dag).serve();",
  "",
].join("\n");
const SDK_VERSION = (
  JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf-8")) as {
    version: string;
  }
).version;

interface TestBundleHeader {
  code: { end: string; sha256: string; start: string };
  metadata: { end: string; sha256: string; start: string };
  sources: Array<{ end: string; path: string; sha256: string; start: string }>;
}

function parseHeader(line: string): TestBundleHeader {
  return JSON.parse(line.slice(EMBEDDED_LAYOUT_PREFIX.length)) as TestBundleHeader;
}

function openMarkerFor(path: string): string {
  return `${EMBEDDED_SOURCE_MARKER}${path}\n`;
}

describe("parsePackArgs", () => {
  it("parses entry with defaults", () => {
    expect(parsePackArgs(["src/main.ts"])).toEqual({
      entry: "src/main.ts",
      outfile: path.join("dist", "bundle.min.mjs"),
    });
  });

  it("parses --outdir override", () => {
    expect(parsePackArgs(["src/main.ts", "--outdir", "build"])).toEqual({
      entry: "src/main.ts",
      outfile: path.join("build", "bundle.min.mjs"),
    });
  });

  it("parses --outfile as the exact output path", () => {
    expect(parsePackArgs(["src/main.ts", "--outfile", "out/sales.min.mjs"])).toEqual({
      entry: "src/main.ts",
      outfile: "out/sales.min.mjs",
    });
  });

  it.each([
    [[], "Missing entry file"],
    [["--outdir"], "--outdir requires a value"],
    [["--outfile"], "--outfile requires a value"],
    [["a.ts", "b.ts"], "Unexpected argument b.ts"],
    [["a.ts", "--bogus"], "Unknown option --bogus"],
    [
      ["a.ts", "--outdir", "dist", "--outfile", "dist/sales.min.mjs"],
      "--outdir and --outfile are mutually exclusive",
    ],
    // A bundle written without the suffix would never be found.
    [["a.ts", "--outfile", "dist/sales.mjs"], "--outfile name must end in .min.mjs"],
    [["a.ts", "--outfile", "dist/sales.min.js"], "--outfile name must end in .min.mjs"],
    [["a.ts", "--outfile", "dist/min.mjs.txt"], "--outfile name must end in .min.mjs"],
  ])("rejects %j", (argv, message) => {
    expect(() => parsePackArgs(argv)).toThrow(message);
  });
});

describe("encodeBundle", () => {
  it("assembles header, metadata, and executable in physical order", () => {
    const executable = Buffer.from('console.log("hello");\n');
    const source = "const x = 1;\nconsole.log(x);\n";
    const bundle = encodeBundle({
      bundleManifest: {
        supervisor_schema_version: "2026-06-16",
        task_handlers: { my_dag: { tasks: ["a", 'b"c'] } },
        dag_source_paths: { my_dag: "src/my_dag.ts" },
      },
      sdkVersion: "0.1.0",
      sourceFiles: { "src/my_dag.ts": source },
      executable,
    });

    const firstNewline = bundle.indexOf("\n");
    const header = parseHeader(bundle.subarray(0, firstNewline).toString("ascii"));
    const offset = (value: string): number => Number.parseInt(value, 16);
    const metadataStart = offset(header.metadata.start);
    const metadataEnd = offset(header.metadata.end);
    expect(header.sources).toHaveLength(1);
    const sourceRegion = header.sources[0]!;
    const sourceStart = offset(sourceRegion.start);
    const sourceEnd = offset(sourceRegion.end);
    const executableStart = offset(header.code.start);
    const executableEnd = offset(header.code.end);

    expect(metadataStart).toBe(firstNewline + 1 + Buffer.byteLength(EMBEDDED_METADATA_PREFIX));
    expect(sourceRegion.path).toBe("src/my_dag.ts");
    expect(sourceStart).toBe(metadataEnd + 1 + Buffer.byteLength(openMarkerFor("src/my_dag.ts")));
    expect(executableStart).toBe(sourceEnd + Buffer.byteLength(EMBEDDED_SOURCE_CLOSE));
    expect(executableEnd).toBe(bundle.length);
    expect(bundle.subarray(executableStart, executableEnd)).toEqual(executable);
    expect(bundle.subarray(sourceStart, sourceEnd).toString("utf-8")).toBe(source);

    const metadata = bundle.subarray(metadataStart, metadataEnd).toString("utf-8");
    expect(metadata).toBe(
      '{"airflow_bundle_metadata_version":"1.0","sdk":{"language":"typescript","version":"0.1.0","supervisor_schema_version":"2026-06-16"},"dag_source_paths":{"my_dag":"src/my_dag.ts"},"task_handlers":{"my_dag":{"tasks":["a","b\\"c"]}}}',
    );

    expect(header).not.toHaveProperty("version");
    expect(header).not.toHaveProperty("source");
  });

  it("embeds one source region per author-owned Dag file, each with its path", () => {
    const bundle = encodeBundle({
      bundleManifest: {
        supervisor_schema_version: "2026-06-16",
        task_handlers: {
          orders_dag: { tasks: ["record"] },
          reports_dag: { tasks: ["generate"] },
        },
        dag_source_paths: {
          orders_dag: "src/main.ts",
          reports_dag: "src/dags/reports.ts",
        },
      },
      sdkVersion: "0.1.0",
      sourceFiles: {
        "src/main.ts": "// orders\nconst orders = 1;\n",
        "src/dags/reports.ts": "// reports\nconst reports = 2;\n",
      },
      executable: Buffer.from("export {};\n"),
    });

    const header = parseHeader(bundle.subarray(0, bundle.indexOf("\n")).toString("ascii"));
    expect(header.sources.map((region) => region.path)).toEqual([
      "src/main.ts",
      "src/dags/reports.ts",
    ]);
    const offset = (value: string): number => Number.parseInt(value, 16);
    for (const region of header.sources) {
      const contents = bundle.subarray(offset(region.start), offset(region.end)).toString("utf-8");
      // Each region carries its own file byte-for-byte, and its open marker
      // ends with the region's path — a human scanning the bundle can find it
      // without cross-referencing the header.
      expect(bundle.slice(0, offset(region.start)).toString("utf-8")).toContain(
        openMarkerFor(region.path),
      );
      if (region.path === "src/main.ts") {
        expect(contents).toBe("// orders\nconst orders = 1;\n");
      } else {
        expect(contents).toBe("// reports\nconst reports = 2;\n");
      }
    }
  });

  it("carries no source region when a bundle serves only task handlers", () => {
    const bundle = encodeBundle({
      bundleManifest: {
        supervisor_schema_version: "2026-06-16",
        task_handlers: { py_dag: { tasks: ["stub"] } },
        // Mixed-language: the Dag lives in Python, so no TS source to embed.
        dag_source_paths: {},
      },
      sdkVersion: "0.1.0",
      sourceFiles: {},
      executable: Buffer.from("export {};\n"),
    });
    const header = parseHeader(bundle.subarray(0, bundle.indexOf("\n")).toString("ascii"));

    expect(header.sources).toEqual([]);
    // Executable starts immediately after metadata; no bytes between them.
    const metadataEnd = Number.parseInt(header.metadata.end, 16);
    const codeStart = Number.parseInt(header.code.start, 16);
    // One newline between the metadata payload and the executable.
    expect(codeStart).toBe(metadataEnd + 1);
  });

  it.each([
    { label: "a block comment terminator", source: "/** doc */\nexport {};\n" },
    { label: "an escaped slash after a star", source: 'const s = "*\\\\/";\n' },
    { label: "a double backslash after a star", source: 'const s = "*\\\\\\\\";\n' },
  ])("escapes $label so the source region cannot close early", ({ source }) => {
    const bundle = encodeBundle({
      bundleManifest: {
        supervisor_schema_version: "2026-06-16",
        task_handlers: {},
        dag_source_paths: { d: "entry.ts" },
      },
      sdkVersion: "0.1.0",
      sourceFiles: { "entry.ts": source },
      executable: Buffer.from("export {};\n"),
    });
    const header = parseHeader(bundle.subarray(0, bundle.indexOf("\n")).toString("ascii"));
    const region = header.sources[0]!;
    const payload = bundle
      .subarray(Number.parseInt(region.start, 16), Number.parseInt(region.end, 16))
      .toString("utf-8");

    // Anything else would end the comment where Node reads the file.
    expect(payload).not.toContain("*/");
    // Reversing the escaping recovers the entrypoint byte for byte.
    expect(payload.replaceAll(/\*\\([\\/])/g, "*$1")).toBe(source);
  });

  it("rejects a single source region over the per-file size limit", () => {
    expect(() =>
      encodeBundle({
        bundleManifest: {
          supervisor_schema_version: "2026-06-16",
          task_handlers: {},
          dag_source_paths: { d: "entry.ts" },
        },
        sdkVersion: "0.1.0",
        sourceFiles: { "entry.ts": "x".repeat(1024 * 1024 + 1) },
        executable: Buffer.from("export {};\n"),
      }),
    ).toThrow("over the 1048576 byte limit");
  });

  it("matches the golden bundle", () => {
    const bundle = encodeBundle({
      bundleManifest: {
        supervisor_schema_version: "2026-06-16",
        task_handlers: { test_dag: { tasks: ["test_task"] } },
        dag_source_paths: { test_dag: "entry.ts" },
      },
      sdkVersion: "0.1.0",
      sourceFiles: { "entry.ts": GOLDEN_SOURCE },
      executable: GOLDEN_CODE,
    });

    expect(bundle).toEqual(readFileSync(GOLDEN_BUNDLE));
    const firstNewline = bundle.indexOf("\n");
    const header = parseHeader(bundle.subarray(0, firstNewline).toString("ascii"));
    const offset = (value: string): number => Number.parseInt(value, 16);
    const region = header.sources[0]!;
    const source = bundle.subarray(offset(region.start), offset(region.end));
    // Stored escaped and reversible: the byte-level agreement the Python reader is checked against.
    expect(source.toString("utf-8")).not.toContain("*/");
    expect(source.toString("utf-8")).not.toBe(GOLDEN_SOURCE);
    expect(source.toString("utf-8").replaceAll(/\*\\([\\/])/g, "*$1")).toBe(GOLDEN_SOURCE);
    // A terminator in the code region is fine. Only the source comment cannot hold one.
    expect(
      bundle.subarray(offset(header.code.start), offset(header.code.end)).toString("utf-8"),
    ).toContain("*/");

    for (const section of [header.metadata, region, header.code]) {
      expect(section.start).toMatch(/^[0-9a-f]{16}$/);
      expect(section.end).toMatch(/^[0-9a-f]{16}$/);
    }
  });

  it("escapes JavaScript line separators inside the metadata comment", () => {
    const bundle = encodeBundle({
      bundleManifest: {
        supervisor_schema_version: "2026-06-16",
        task_handlers: { "line\u2028separator": { tasks: ["paragraph\u2029separator"] } },
        dag_source_paths: {},
      },
      sdkVersion: "0.1.0",
      sourceFiles: {},
      executable: Buffer.from("export {};\n"),
    });
    const metadataLine = bundle.toString("utf-8").split("\n")[1]!;

    expect(metadataLine).not.toContain("\u2028");
    expect(metadataLine).not.toContain("\u2029");
    expect(metadataLine).toContain("\\u2028");
    expect(metadataLine).toContain("\\u2029");
    expect(JSON.parse(metadataLine.slice(EMBEDDED_METADATA_PREFIX.length))).toHaveProperty(
      "task_handlers.line\u2028separator.tasks",
      ["paragraph\u2029separator"],
    );
  });
});

function readEmbeddedMetadata(bundlePath: string): string {
  const bundle = readFileSync(bundlePath);
  const firstNewline = bundle.indexOf("\n");
  const header = parseHeader(bundle.subarray(0, firstNewline).toString("utf-8"));
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

  it("bundles the entry and embeds metadata from the bundle's bundle", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const nested = path.join(outdir, "dist");
    await runPack([FIXTURE_ENTRY, "--outdir", nested]);

    const bundlePath = path.join(nested, "bundle.min.mjs");
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
      // The one native Dag declared in the entry file gets its source path
      // recorded. Task-handler-only Dags (`fixture_dag`) live in Python and are
      // absent here.
      dag_source_paths: {
        other_dag: expect.stringMatching(/entry\.ts$/) as unknown as string,
      },
      task_handlers: {
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

    const bundle = readFileSync(path.join(outdir, "bundle.min.mjs"));
    const firstNewline = bundle.indexOf("\n");
    const layoutLine = bundle.subarray(0, firstNewline).toString("utf-8");
    expect(layoutLine.startsWith(EMBEDDED_LAYOUT_PREFIX)).toBe(true);

    const layout = parseHeader(layoutLine);
    const offset = (value: string): number => Number.parseInt(value, 16);

    expect(layout).not.toHaveProperty("version");
    const code = bundle.subarray(offset(layout.code.start), offset(layout.code.end));
    expect(createHash("sha256").update(code).digest("hex")).toBe(layout.code.sha256);

    const metadataPayload = bundle.subarray(
      offset(layout.metadata.start),
      offset(layout.metadata.end),
    );
    expect(createHash("sha256").update(metadataPayload).digest("hex")).toBe(layout.metadata.sha256);
    expect(JSON.parse(metadataPayload.toString("utf-8"))).toHaveProperty(
      "task_handlers.fixture_dag",
    );

    // One source region per author-owned Dag file. The fixture declares one
    // native Dag (`other_dag`) in the entry, so exactly one region ships and
    // its path ends in `entry.ts`.
    expect(layout.sources).toHaveLength(1);
    const region = layout.sources[0]!;
    expect(region.path).toMatch(/entry\.ts$/);
    const source = bundle.subarray(offset(region.start), offset(region.end));
    expect(createHash("sha256").update(source).digest("hex")).toBe(region.sha256);
    // Stored escaped, because the fixture's own license header ends in a comment terminator.
    expect(source.toString("utf-8").replaceAll(/\*\\([\\/])/g, "*$1")).toBe(
      readFileSync(FIXTURE_ENTRY, "utf-8"),
    );
  });

  it("minifies the code region and keeps it runnable", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    await runPack([FIXTURE_ENTRY, "--outdir", outdir]);

    const bundlePath = path.join(outdir, "bundle.min.mjs");
    const bundle = readFileSync(bundlePath);
    const layout = parseHeader(bundle.subarray(0, bundle.indexOf("\n")).toString("utf-8"));
    const code = bundle
      .subarray(Number.parseInt(layout.code.start, 16), Number.parseInt(layout.code.end, 16))
      .toString("utf-8");

    // esbuild indents unminified output; minified output has no indented line.
    expect(code).not.toContain("\n  ");
    // Minification must not strip the dependencies' license banners.
    expect(code).toContain("/*!");
    // A digest over minified bytes only means something if those bytes still execute.
    const dumped = execFileSync(process.execPath, [bundlePath, "--airflow-metadata"], {
      encoding: "utf-8",
    });
    expect(
      JSON.parse(dumped.slice(AIRFLOW_METADATA_SENTINEL.length)).supervisor_schema_version,
    ).toBe(SUPERVISOR_API_VERSION);
  });

  it("writes the bundle to an explicit --outfile", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const target = path.join(outdir, "nested", "sales.min.mjs");

    await runPack([FIXTURE_ENTRY, "--outfile", target]);

    expect(existsSync(target)).toBe(true);
    expect(existsSync(path.join(outdir, "nested", "bundle.min.mjs"))).toBe(false);
    // Staging is written beside the target and cleaned up there.
    expect(existsSync(path.join(outdir, "nested", "bundle.pack-staging.mjs"))).toBe(false);
    expect(readFileSync(target).subarray(0, EMBEDDED_LAYOUT_PREFIX.length).toString()).toBe(
      EMBEDDED_LAYOUT_PREFIX,
    );
    expect(JSON.parse(readEmbeddedMetadata(target))).toHaveProperty("task_handlers.fixture_dag");
  });

  it("keeps a shebang entry runnable and reads the manifest past import-time logging", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    await runPack([NOISY_ENTRY, "--outdir", outdir]);

    const bundlePath = path.join(outdir, "bundle.min.mjs");
    const bundle = readFileSync(bundlePath, "utf-8");
    expect(bundle.startsWith(EMBEDDED_LAYOUT_PREFIX)).toBe(true);
    // The entrypoint is embedded as written and opens with a shebang, so check only the code region.
    const codeRegion = readFileSync(bundlePath).subarray(
      Number.parseInt(parseHeader(bundle.split("\n")[0]!).code.start, 16),
    );
    expect(codeRegion.toString("utf-8")).not.toContain("#!/usr/bin/env node");
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);

    const metadataLine = bundle.split("\n")[1]!;
    const metadata = JSON.parse(metadataLine.slice(EMBEDDED_METADATA_PREFIX.length));
    expect(metadata).toHaveProperty("task_handlers.noisy_dag");

    execFileSync(process.execPath, [bundlePath, "--airflow-metadata"], { encoding: "utf-8" });
  });

  it("leaves no bundle behind when the metadata exceeds the embedded size limit", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "huge-entry.ts");
    writeFileSync(
      entry,
      [
        `import { Bundle, Dag } from ${JSON.stringify(SDK_INDEX)};`,
        'const bigDag = new Dag("big_dag");',
        'for (let i = 0; i < 5000; i += 1) bigDag.task(String(i).padStart(240, "t"), async () => undefined)();',
        "await new Bundle(bigDag).serve();",
      ].join("\n"),
    );

    await expect(runPack([entry, "--outdir", outdir])).rejects.toThrow(
      "over the 1048576 byte limit",
    );
    expect(existsSync(path.join(outdir, "bundle.min.mjs"))).toBe(false);
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);
  });

  it("leaves no bundle behind when the entry serves nothing", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));

    await expect(runPack([EMPTY_ENTRY, "--outdir", outdir])).rejects.toThrow(
      "served nothing; register Dags or task handlers",
    );
    expect(existsSync(path.join(outdir, "bundle.min.mjs"))).toBe(false);
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
        `import { Bundle, Dag } from ${JSON.stringify(SDK_INDEX)};`,
        `const suspiciousDag = new Dag(${JSON.stringify(dagId)});`,
        `suspiciousDag.task(${JSON.stringify(taskId)}, async () => undefined)();`,
        "await new Bundle(suspiciousDag).serve();",
      ].join("\n"),
    );
    const stderr = captureStderr();

    await runPack([entry, "--outdir", outdir]);

    expect(stderr()).toContain(expected);
    expect(existsSync(path.join(outdir, "bundle.min.mjs"))).toBe(true);
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
    expect(existsSync(path.join(outdir, "bundle.min.mjs"))).toBe(false);
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);
  });

  // A bundle can print the sentinel itself, so nothing on that line is trusted.
  it.each([
    [
      '{ supervisor_schema_version: "1", task_handlers: { broken_dag: {} }, dag_source_paths: {} }',
      "malformed entry",
    ],
    [
      '{ supervisor_schema_version: "1", task_handlers: { broken_dag: { tasks: ["ok", 7] } }, dag_source_paths: {} }',
      "malformed entry",
    ],
    [
      '{ supervisor_schema_version: "1", task_handlers: { broken_dag: { tasks: [""] } }, dag_source_paths: {} }',
      "malformed entry",
    ],
    [
      '{ supervisor_schema_version: "1", task_handlers: [{ tasks: ["a"] }], dag_source_paths: {} }',
      "incomplete",
    ],
    // Missing dag_source_paths entirely.
    ['{ supervisor_schema_version: "1", task_handlers: { d: { tasks: ["a"] } } }', "incomplete"],
    // dag_source_paths is not an object — an array passes typeof but should not.
    [
      '{ supervisor_schema_version: "1", task_handlers: { d: { tasks: ["a"] } }, dag_source_paths: [] }',
      "incomplete",
    ],
    // A source path that is empty leaves nothing to read.
    [
      '{ supervisor_schema_version: "1", task_handlers: { d: { tasks: ["a"] } }, dag_source_paths: { d: "" } }',
      "incomplete",
    ],
    // Was read off before the document itself was checked, so it surfaced as a
    // raw TypeError.
    ["null", "incomplete"],
    // Truthy, but not the non-empty string the schema requires.
    [
      '{ supervisor_schema_version: true, task_handlers: { d: { tasks: ["a"] } }, dag_source_paths: {} }',
      "incomplete",
    ],
    [
      '{ supervisor_schema_version: 20260616, task_handlers: { d: { tasks: ["a"] } }, dag_source_paths: {} }',
      "incomplete",
    ],
  ])("rejects the metadata line %s", async (manifest, message) => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "malformed-entry.ts");
    writeFileSync(
      entry,
      `console.log(${JSON.stringify(AIRFLOW_METADATA_SENTINEL)} + JSON.stringify(${manifest}));`,
    );

    await expect(runPack([entry, "--outdir", outdir])).rejects.toThrow(message);
    expect(existsSync(path.join(outdir, "bundle.min.mjs"))).toBe(false);
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);
  });

  it("warns but still packs a registered Dag with no tasks, as airflow-go-pack does", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "mixed-entry.ts");
    writeFileSync(
      entry,
      [
        `import { Bundle, Dag } from ${JSON.stringify(SDK_INDEX)};`,
        'const salesDag = new Dag("sales_dag");',
        'salesDag.task("extract", async () => undefined)();',
        'await new Bundle(salesDag, new Dag("empty_dag")).serve();',
      ].join("\n"),
    );
    const stderr = captureStderr();

    await runPack([entry, "--outdir", outdir]);

    expect(stderr()).toContain('warning: dag "empty_dag" has no tasks\n');
    expect(JSON.parse(readEmbeddedMetadata(path.join(outdir, "bundle.min.mjs")))).toHaveProperty(
      "task_handlers.empty_dag.tasks",
      [],
    );
  });

  it("packs only the Dags the served bundle holds", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "forgotten-entry.ts");
    writeFileSync(
      entry,
      [
        `import { Bundle, Dag } from ${JSON.stringify(SDK_INDEX)};`,
        'const salesDag = new Dag("sales_dag");',
        'salesDag.task("extract", async () => undefined)();',
        'const billingDag = new Dag("billing_dag");',
        'billingDag.task("charge", async () => undefined)();',
        "await new Bundle(salesDag).serve();",
      ].join("\n"),
    );

    await runPack([entry, "--outdir", outdir]);

    const metadata = JSON.parse(readEmbeddedMetadata(path.join(outdir, "bundle.min.mjs")));
    expect(metadata).toHaveProperty("task_handlers.sales_dag");
    expect(metadata).not.toHaveProperty("task_handlers.billing_dag");
  });

  it("embeds one source region per Dag file when Dags are spread across imports", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const dagsDir = path.join(outdir, "src", "dags");
    const entry = path.join(outdir, "src", "main.ts");
    const reports = path.join(dagsDir, "reports.ts");
    mkdirSync(dagsDir, { recursive: true });
    // Layout: `src/main.ts` declares one Dag inline, `src/dags/reports.ts`
    // declares the other. Under the old encoder shape, only the entry file
    // survived in the source region; here both should. Paths use the pack
    // step's cwd (this test's cwd) rather than chdir, so parallel tests do
    // not race on process.cwd().
    writeFileSync(
      entry,
      [
        `import { Bundle, Dag } from ${JSON.stringify(SDK_INDEX)};`,
        'import { reportsDag } from "./dags/reports.js";',
        "",
        'const ordersDag = new Dag("orders_dag");',
        'ordersDag.task("record_order", async () => undefined)();',
        "",
        "await new Bundle(ordersDag, reportsDag).serve();",
      ].join("\n"),
    );
    writeFileSync(
      reports,
      [
        `import { Dag } from ${JSON.stringify(SDK_INDEX)};`,
        "",
        'export const reportsDag = new Dag("reports_dag");',
        'reportsDag.task("generate", async () => undefined)();',
      ].join("\n"),
    );

    await runPack([entry, "--outdir", path.join(outdir, "dist")]);

    const bundlePath = path.join(outdir, "dist", "bundle.min.mjs");
    const bundle = readFileSync(bundlePath);
    const layout = parseHeader(bundle.subarray(0, bundle.indexOf("\n")).toString("utf-8"));

    // Paths in the header come out relative to the pack step's cwd. Assert on
    // the segments the layout imposes rather than a full path prefix so the
    // test does not depend on where vitest runs.
    const paths = layout.sources.map((region) => region.path);
    expect(paths).toHaveLength(2);
    const mainPath = paths.find((p) => p.endsWith("src/main.ts"))!;
    const reportsPath = paths.find((p) => p.endsWith("src/dags/reports.ts"))!;
    expect(mainPath).toBeDefined();
    expect(reportsPath).toBeDefined();

    // Metadata carries the dag-to-source map, so the reader can pick the
    // right source per Dag without guessing.
    const metadata = JSON.parse(readEmbeddedMetadata(bundlePath));
    expect(metadata.dag_source_paths).toEqual({
      orders_dag: mainPath,
      reports_dag: reportsPath,
    });

    // Each region carries the file's original text — a reader with just the
    // bundle can present a Code tab per Dag.
    for (const region of layout.sources) {
      const start = Number.parseInt(region.start, 16);
      const end = Number.parseInt(region.end, 16);
      const content = bundle.subarray(start, end).toString("utf-8");
      if (region.path === mainPath) {
        expect(content).toContain('new Dag("orders_dag")');
      } else {
        expect(content).toContain('new Dag("reports_dag")');
      }
    }
  });
});
