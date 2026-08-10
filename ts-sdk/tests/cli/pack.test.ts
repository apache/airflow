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
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

import {
  EMBEDDED_METADATA_PREFIX,
  parsePackArgs,
  renderMetadataYaml,
  runPack,
} from "../../src/cli/pack.js";
import { SUPERVISOR_API_VERSION } from "../../src/coordinator/protocol.js";
import { AIRFLOW_METADATA_SENTINEL } from "../../src/coordinator/manifest.js";

const FIXTURE_ENTRY = fileURLToPath(new URL("fixtures/entry.ts", import.meta.url));
const NOISY_ENTRY = fileURLToPath(new URL("fixtures/noisy-entry.ts", import.meta.url));
const EMPTY_ENTRY = fileURLToPath(new URL("fixtures/empty-entry.ts", import.meta.url));
const SDK_INDEX = fileURLToPath(new URL("../../src/index.ts", import.meta.url));
const SDK_VERSION = (
  JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf-8")) as {
    version: string;
  }
).version;

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

describe("renderMetadataYaml", () => {
  it("emits schema-conformant YAML with escaped scalars", () => {
    const yaml = renderMetadataYaml({
      airflow_bundle_metadata_version: "1.0",
      sdk: { language: "typescript", version: "0.1.0", supervisor_schema_version: "2026-06-16" },
      source: 'we"ird.ts',
      dags: { my_dag: { tasks: ["a", 'b"c'] } },
    });
    expect(yaml).toBe(
      [
        'airflow_bundle_metadata_version: "1.0"',
        "sdk:",
        '  language: "typescript"',
        '  version: "0.1.0"',
        '  supervisor_schema_version: "2026-06-16"',
        'source: "we\\"ird.ts"',
        "dags:",
        '  "my_dag":',
        '    tasks: ["a", "b\\"c"]',
        "",
      ].join("\n"),
    );
  });
});

describe("runPack", () => {
  let outdir: string;

  afterEach(() => {
    if (outdir) rmSync(outdir, { recursive: true, force: true });
  });

  it("bundles the entry and embeds metadata from the bundle's registry", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const nested = path.join(outdir, "dist");
    await runPack([FIXTURE_ENTRY, "--outdir", nested]);

    const bundlePath = path.join(nested, "bundle.mjs");
    expect(existsSync(path.join(nested, "airflow-metadata.yaml"))).toBe(false);

    const firstLine = readFileSync(bundlePath, "utf-8").split("\n")[0]!;
    expect(firstLine.startsWith(EMBEDDED_METADATA_PREFIX)).toBe(true);
    const metadata = Buffer.from(
      firstLine.slice(EMBEDDED_METADATA_PREFIX.length),
      "base64",
    ).toString("utf-8");
    expect(metadata).toBe(
      [
        'airflow_bundle_metadata_version: "1.0"',
        "sdk:",
        '  language: "typescript"',
        `  version: ${JSON.stringify(SDK_VERSION)}`,
        `  supervisor_schema_version: ${JSON.stringify(SUPERVISOR_API_VERSION)}`,
        'source: "entry.ts"',
        "dags:",
        '  "fixture_dag":',
        '    tasks: ["extract", "transform"]',
        '  "other_dag":',
        '    tasks: ["solo"]',
        "",
      ].join("\n"),
    );

    const dumped = execFileSync(process.execPath, [bundlePath, "--airflow-metadata"], {
      encoding: "utf-8",
    });
    expect(dumped.startsWith(AIRFLOW_METADATA_SENTINEL)).toBe(true);
    expect(
      JSON.parse(dumped.slice(AIRFLOW_METADATA_SENTINEL.length)).supervisor_schema_version,
    ).toBe(SUPERVISOR_API_VERSION);
  });

  it("keeps a shebang entry runnable and reads the manifest past import-time logging", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    await runPack([NOISY_ENTRY, "--outdir", outdir]);

    const bundlePath = path.join(outdir, "bundle.mjs");
    const bundle = readFileSync(bundlePath, "utf-8");
    expect(bundle.startsWith(EMBEDDED_METADATA_PREFIX)).toBe(true);
    expect(bundle).not.toContain("#!/usr/bin/env node");
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);

    const metadata = Buffer.from(
      bundle.split("\n")[0]!.slice(EMBEDDED_METADATA_PREFIX.length),
      "base64",
    ).toString("utf-8");
    expect(metadata).toContain('  "noisy_dag":');

    execFileSync(process.execPath, [bundlePath, "--airflow-metadata"], { encoding: "utf-8" });
  });

  it("leaves no bundle behind when the metadata exceeds the embedded size limit", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));
    const entry = path.join(outdir, "huge-entry.ts");
    writeFileSync(
      entry,
      [
        `import { registerTask, startCoordinator } from ${JSON.stringify(SDK_INDEX)};`,
        'for (let i = 0; i < 4000; i += 1) registerTask({ dagId: "big_dag", taskId: String(i).padStart(240, "t") }, async () => undefined);',
        "await startCoordinator();",
      ].join("\n"),
    );

    await expect(runPack([entry, "--outdir", outdir])).rejects.toThrow(
      "over the 1048576 byte limit",
    );
    expect(existsSync(path.join(outdir, "bundle.mjs"))).toBe(false);
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);
  });

  it("leaves no bundle behind when the entry registers no tasks", async () => {
    outdir = mkdtempSync(path.join(tmpdir(), "ts-pack-"));

    await expect(runPack([EMPTY_ENTRY, "--outdir", outdir])).rejects.toThrow("registered no tasks");
    expect(existsSync(path.join(outdir, "bundle.mjs"))).toBe(false);
    expect(existsSync(path.join(outdir, "bundle.pack-staging.mjs"))).toBe(false);
  });
});
