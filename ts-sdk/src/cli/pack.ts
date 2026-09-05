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

// airflow-ts-pack: bundle a TypeScript entrypoint into the single-file
// artifact NodeCoordinator consumes — `bundle.mjs` with metadata and a
// integrity layout descriptor embedded in JavaScript comments.
//
// Build first, then run the built bundle with --airflow-metadata so the
// manifest comes from the bundle's own Dag registry and schema version,
// never from a hand-written sidecar.

import { execFileSync } from "node:child_process";
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";

import {
  AIRFLOW_METADATA_FLAG,
  AIRFLOW_METADATA_SENTINEL,
  type BundleManifest,
} from "../coordinator/manifest.js";
import { encodeBundle } from "./bundle-encoder.js";
import { warnOnSuspiciousIds } from "./validate.js";

const BUNDLE_FILENAME = "bundle.mjs";
// Write bundle.mjs only after the build and manifest checks succeed, so a
// failed pack cannot leave a partial final artifact.
const STAGING_FILENAME = "bundle.pack-staging.mjs";
const MANIFEST_TIMEOUT_MS = 60_000;
const MANIFEST_MAX_BUFFER_BYTES = 64 * 1024 * 1024;

const USAGE = `Usage: airflow-ts-pack <entry> [--outdir <dir>] [--source <name>]

Bundles <entry> into <outdir>/${BUNDLE_FILENAME} with esbuild and embeds the
airflow metadata generated from the bundle's served Dags.

Options:
  --outdir <dir>   Output directory (default: dist)
  --source <name>  Display name of the primary source file (default: <entry> basename)
`;

export interface PackArgs {
  entry: string;
  outdir: string;
  source: string;
}

function usageError(message: string): Error {
  return new Error(`${message}\n\n${USAGE}`);
}

export function parsePackArgs(argv: readonly string[]): PackArgs {
  let entry: string | null = null;
  let outdir = "dist";
  let source: string | null = null;
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]!;
    if (arg === "--outdir" || arg === "--source") {
      const value = argv[i + 1];
      if (!value) throw usageError(`${arg} requires a value`);
      if (arg === "--outdir") outdir = value;
      else source = value;
      i += 1;
    } else if (arg.startsWith("-")) {
      throw usageError(`Unknown option ${arg}`);
    } else if (entry) {
      throw usageError(`Unexpected argument ${arg}`);
    } else {
      entry = arg;
    }
  }
  if (!entry) throw usageError("Missing entry file");
  return { entry, outdir, source: source ?? path.basename(entry) };
}

function readSdkVersion(): string {
  const packageJsonUrl = new URL("../../package.json", import.meta.url);
  const { version } = JSON.parse(readFileSync(packageJsonUrl, "utf-8")) as { version: string };
  return version;
}

function readBundleManifest(bundlePath: string): BundleManifest {
  let stdout: string;
  try {
    stdout = execFileSync(process.execPath, [bundlePath, AIRFLOW_METADATA_FLAG], {
      encoding: "utf-8",
      timeout: MANIFEST_TIMEOUT_MS,
      maxBuffer: MANIFEST_MAX_BUFFER_BYTES,
    });
  } catch (error) {
    const stderr = (error as { stderr?: string }).stderr ?? "";
    const reported = stderr
      .split("\n")
      .reverse()
      .find((line) => /^\w*Error: /.test(line.trim()))
      ?.trim();
    throw new Error(
      reported ?? `Running the bundle with ${AIRFLOW_METADATA_FLAG} failed: ${String(error)}`,
      { cause: error },
    );
  }

  // Import-time logging from user code lands on stdout too; pick the sentinel line.
  const line = stdout
    .split("\n")
    .reverse()
    .find((candidate) => candidate.startsWith(AIRFLOW_METADATA_SENTINEL));
  if (line === undefined) {
    throw new Error(`Bundle produced no ${AIRFLOW_METADATA_FLAG} output`);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(line.slice(AIRFLOW_METADATA_SENTINEL.length));
  } catch (error) {
    throw new Error(`Bundle produced invalid ${AIRFLOW_METADATA_FLAG} output: ${String(error)}`, {
      cause: error,
    });
  }
  if (!isBundleManifest(parsed)) {
    throw new Error(`Bundle produced incomplete ${AIRFLOW_METADATA_FLAG} output`);
  }
  const manifest = parsed;
  // The line is whatever the bundle printed and nothing downstream re-validates
  // it, so check each Dag entry down to the task-id element.
  for (const [dagId, dag] of Object.entries(manifest.dags)) {
    if (dag == null || !isTaskIdList(dag.tasks)) {
      throw new Error(
        `Bundle produced ${AIRFLOW_METADATA_FLAG} output with a malformed entry for Dag "${dagId}"`,
      );
    }
  }
  return manifest;
}

// The document is checked before anything is read off it: JSON.parse also yields
// null and primitives, and `null.supervisor_schema_version` would surface as a
// raw TypeError rather than a report about the bundle.
function isBundleManifest(value: unknown): value is BundleManifest {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const { supervisor_schema_version: version, dags } = value as Partial<BundleManifest>;
  return (
    // Rendered into the manifest verbatim, where the schema requires a non-empty
    // string, so a truthy number or boolean would travel to Airflow as-is.
    typeof version === "string" &&
    version.length > 0 &&
    typeof dags === "object" &&
    dags !== null &&
    // An array would pass the typeof check and yield Dags named "0", "1", ...
    !Array.isArray(dags)
  );
}

function isTaskIdList(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string" && item.length > 0);
}

async function loadEsbuild(): Promise<typeof import("esbuild")> {
  try {
    return await import("esbuild");
  } catch (error) {
    throw new Error(
      "airflow-ts-pack needs esbuild; install it alongside the SDK (e.g. `npm i -D esbuild`)",
      { cause: error },
    );
  }
}

export async function runPack(argv: readonly string[]): Promise<void> {
  const args = parsePackArgs(argv);
  const bundlePath = path.join(args.outdir, BUNDLE_FILENAME);
  const stagingPath = path.join(args.outdir, STAGING_FILENAME);
  const { build } = await loadEsbuild();

  try {
    await build({
      entryPoints: [args.entry],
      bundle: true,
      platform: "node",
      format: "esm",
      target: "node22",
      outfile: stagingPath,
    });

    const manifest = readBundleManifest(stagingPath);
    const dagEntries = Object.entries(manifest.dags);
    if (dagEntries.length === 0) {
      throw new Error(`${args.entry} served no Dags; pass them to serveDags(new DagRegistry(...))`);
    }
    // Warn rather than fail, as airflow-go-pack does: the shared schema allows a
    // Dag with no tasks.
    for (const [dagId, dag] of dagEntries) {
      if (dag.tasks.length === 0) {
        process.stderr.write(`warning: dag ${JSON.stringify(dagId)} has no tasks\n`);
      }
    }
    warnOnSuspiciousIds(manifest.dags);

    const bundle = encodeBundle({
      bundleManifest: manifest,
      sdkVersion: readSdkVersion(),
      entrypointName: args.source,
      executable: readFileSync(stagingPath),
    });
    writeFileSync(bundlePath, bundle);
  } finally {
    rmSync(stagingPath, { force: true });
  }

  console.log(`Wrote ${bundlePath} (airflow metadata and integrity embedded)`);
}
