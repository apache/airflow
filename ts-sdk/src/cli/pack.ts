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
// artifact NodeCoordinator consumes — `bundle.mjs` with the airflow
// metadata embedded as a leading `//# airflowMetadata=<base64>` comment.
//
// Mirrors airflow-go-pack: build first, then run the built bundle with
// --airflow-metadata so the manifest comes from the bundle's own task
// registry and schema version, never from a hand-written sidecar.

import { execFileSync } from "node:child_process";
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";

import {
  AIRFLOW_METADATA_FLAG,
  AIRFLOW_METADATA_SENTINEL,
  type BundleManifest,
} from "../coordinator/manifest.js";

const AIRFLOW_BUNDLE_METADATA_VERSION = "1.0";
const BUNDLE_FILENAME = "bundle.mjs";
// bundle.mjs is written only after validation, so failures leave no partial artifact.
const STAGING_FILENAME = "bundle.pack-staging.mjs";
const MANIFEST_TIMEOUT_MS = 60_000;
const MANIFEST_MAX_BUFFER_BYTES = 64 * 1024 * 1024;
const EMBEDDED_METADATA_MAX_BYTES = 1024 * 1024;
export const EMBEDDED_METADATA_PREFIX = "//# airflowMetadata=";

const USAGE = `Usage: airflow-ts-pack <entry> [--outdir <dir>] [--source <name>]

Bundles <entry> into <outdir>/${BUNDLE_FILENAME} with esbuild and embeds the
airflow metadata generated from the bundle's registered tasks.

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

export interface PackMetadata {
  airflow_bundle_metadata_version: string;
  sdk: { language: string; version: string; supervisor_schema_version: string };
  source: string;
  dags: BundleManifest["dags"];
}

// JSON string literals are valid YAML double-quoted scalars, so every
// scalar below is emitted through JSON.stringify for correct escaping.
export function renderMetadataYaml(metadata: PackMetadata): string {
  const lines = [
    `airflow_bundle_metadata_version: ${JSON.stringify(metadata.airflow_bundle_metadata_version)}`,
    "sdk:",
    `  language: ${JSON.stringify(metadata.sdk.language)}`,
    `  version: ${JSON.stringify(metadata.sdk.version)}`,
    `  supervisor_schema_version: ${JSON.stringify(metadata.sdk.supervisor_schema_version)}`,
    `source: ${JSON.stringify(metadata.source)}`,
    "dags:",
  ];
  for (const [dagId, dag] of Object.entries(metadata.dags)) {
    lines.push(`  ${JSON.stringify(dagId)}:`);
    lines.push(`    tasks: [${dag.tasks.map((task) => JSON.stringify(task)).join(", ")}]`);
  }
  return `${lines.join("\n")}\n`;
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
    throw new Error(`Running the bundle with ${AIRFLOW_METADATA_FLAG} failed: ${String(error)}`, {
      cause: error,
    });
  }

  // Import-time logging from user code lands on stdout too; pick the sentinel line.
  const line = stdout
    .split("\n")
    .reverse()
    .find((candidate) => candidate.startsWith(AIRFLOW_METADATA_SENTINEL));
  if (line === undefined) {
    throw new Error(`Bundle produced no ${AIRFLOW_METADATA_FLAG} output`);
  }

  let manifest: BundleManifest;
  try {
    manifest = JSON.parse(line.slice(AIRFLOW_METADATA_SENTINEL.length)) as BundleManifest;
  } catch (error) {
    throw new Error(`Bundle produced invalid ${AIRFLOW_METADATA_FLAG} output: ${String(error)}`, {
      cause: error,
    });
  }
  if (!manifest.supervisor_schema_version || !manifest.dags || typeof manifest.dags !== "object") {
    throw new Error(`Bundle produced incomplete ${AIRFLOW_METADATA_FLAG} output`);
  }
  return manifest;
}

// esbuild keeps an entry hashbang as line 1, where the metadata comment must go;
// NodeCoordinator always runs the bundle through `node`, so drop it.
function stripShebang(bundle: string): string {
  if (!bundle.startsWith("#!")) return bundle;
  const newline = bundle.indexOf("\n");
  return newline === -1 ? "" : bundle.slice(newline + 1);
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
    if (Object.keys(manifest.dags).length === 0) {
      throw new Error(
        `${args.entry} registered no tasks; call registerTask(...) before startCoordinator()`,
      );
    }

    const metadataYaml = renderMetadataYaml({
      airflow_bundle_metadata_version: AIRFLOW_BUNDLE_METADATA_VERSION,
      sdk: {
        language: "typescript",
        version: readSdkVersion(),
        supervisor_schema_version: manifest.supervisor_schema_version,
      },
      source: args.source,
      dags: manifest.dags,
    });
    const metadataLine = `${EMBEDDED_METADATA_PREFIX}${Buffer.from(metadataYaml, "utf-8").toString("base64")}\n`;
    if (metadataLine.length > EMBEDDED_METADATA_MAX_BYTES) {
      throw new Error(
        `Embedded airflow metadata is ${metadataLine.length} bytes, ` +
          `over the ${EMBEDDED_METADATA_MAX_BYTES} byte limit; reduce the number of registered tasks`,
      );
    }
    writeFileSync(bundlePath, metadataLine + stripShebang(readFileSync(stagingPath, "utf-8")));
  } finally {
    rmSync(stagingPath, { force: true });
  }

  console.log(`Wrote ${bundlePath} (airflow metadata embedded)`);
}
