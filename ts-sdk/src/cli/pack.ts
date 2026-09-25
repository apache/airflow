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

// airflow-ts-pack: bundle a TypeScript entrypoint into the single artifact NodeCoordinator consumes.
// `bundle.min.mjs` carries the metadata, the entrypoint source, and an integrity layout descriptor
// in JavaScript comments.
//
// Build first, then run the built bundle with --airflow-metadata so the manifest comes from the
// bundle's own Dag registry and schema version, never from a hand-written sidecar.

import { execFileSync } from "node:child_process";
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import { readFile as readFileAsync } from "node:fs/promises";
import path from "node:path";

import {
  AIRFLOW_METADATA_FLAG,
  AIRFLOW_METADATA_SENTINEL,
  type BundleManifest,
} from "../coordinator/manifest.js";
import { MODULE_SOURCE_SLOT_KEY } from "../sdk/module-source.js";
import { encodeBundle } from "./bundle-encoder.js";
import { warnOnSuspiciousIds } from "./validate.js";

// NodeCoordinator discovers bundles by this suffix, so keep the two in step.
const BUNDLE_FILENAME = "bundle.min.mjs";
// Write the bundle only after the build and manifest checks succeed, so a failed pack
// cannot leave a partial artifact.
const STAGING_FILENAME = "bundle.pack-staging.mjs";
const MANIFEST_TIMEOUT_MS = 60_000;
const MANIFEST_MAX_BUFFER_BYTES = 64 * 1024 * 1024;

const USAGE = `Usage: airflow-ts-pack <entry> [--outdir <dir> | --outfile <path>]

Bundles <entry> into a minified ${BUNDLE_FILENAME} with esbuild and embeds the
airflow metadata generated from the bundle's served Dags, plus each Dag-defining
source file verbatim so Airflow has readable text to display per Dag.

Options:
  --outdir <dir>    Output directory, holding ${BUNDLE_FILENAME} (default: dist)
  --outfile <path>  Exact output path; its name must end in .min.mjs
`;

/** A bundle written without this suffix is invisible to NodeCoordinator. */
const REQUIRED_OUTFILE_SUFFIX = ".min.mjs";

export interface PackArgs {
  entry: string;
  outfile: string;
}

function usageError(message: string): Error {
  return new Error(`${message}\n\n${USAGE}`);
}

export function parsePackArgs(argv: readonly string[]): PackArgs {
  let entry: string | null = null;
  let outdir: string | null = null;
  let outfile: string | null = null;
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]!;
    if (arg === "--outdir" || arg === "--outfile") {
      const value = argv[i + 1];
      if (!value) throw usageError(`${arg} requires a value`);
      if (arg === "--outdir") outdir = value;
      else outfile = value;
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
  // Silently preferring one would write the bundle somewhere the caller did not ask for.
  if (outdir !== null && outfile !== null) {
    throw usageError("--outdir and --outfile are mutually exclusive");
  }
  if (outfile !== null && !path.basename(outfile).endsWith(REQUIRED_OUTFILE_SUFFIX)) {
    throw usageError(
      `--outfile name must end in ${REQUIRED_OUTFILE_SUFFIX}; NodeCoordinator finds bundles by that suffix`,
    );
  }
  return {
    entry,
    outfile: outfile ?? path.join(outdir ?? "dist", BUNDLE_FILENAME),
  };
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
  for (const [dagId, dag] of Object.entries(manifest.task_handlers)) {
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
  const {
    supervisor_schema_version: version,
    task_handlers: taskHandlers,
    dag_source_paths: dagSourcePaths,
  } = value as Partial<BundleManifest>;
  return (
    // Rendered into the manifest verbatim, where the schema requires a non-empty
    // string, so a truthy number or boolean would travel to Airflow as-is.
    typeof version === "string" &&
    version.length > 0 &&
    typeof taskHandlers === "object" &&
    taskHandlers !== null &&
    // An array would pass the typeof check and yield Dags named "0", "1", ...
    !Array.isArray(taskHandlers) &&
    isSourcePathMap(dagSourcePaths)
  );
}

function isSourcePathMap(value: unknown): value is Record<string, string> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  for (const entry of Object.values(value)) {
    if (typeof entry !== "string" || entry.length === 0) return false;
  }
  return true;
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

/** Author source files esbuild's onLoad tags with their path. Same filter set the previous
 *  task-id plugin used, which matched every language a Dag can be declared in. */
const AUTHOR_SOURCE_FILTER = /\.[cm]?[jt]sx?$/;

/** Skip anything under node_modules: an installed dependency is not a Dag file, and tagging it
 *  would only cost bundle bytes while overwriting the slot with the wrong path. */
const DEPENDENCY_PATH = /[\\/]node_modules[\\/]/;

/**
 * esbuild plugin: prepend a single line to each author-owned source file that
 * writes the file's path into the SDK's module-source slot. `Dag`'s
 * constructor reads that slot, so a Dag declared in `src/dags/reports.ts`
 * carries that path even though esbuild will soon inline every module into
 * one bundle.
 *
 * The prepend is text-only — no parsing, no AST — so it can never mis-identify
 * a construction site. ES modules hoist imports above non-import statements
 * at execution, so the tag runs *after* imported modules have written their
 * own slots and *before* this module's own top-level statements, which is
 * exactly when its `new Dag(...)` calls fire.
 */
function moduleSourceTagPlugin(cwd: string): import("esbuild").Plugin {
  const slotKey = JSON.stringify(MODULE_SOURCE_SLOT_KEY);
  return {
    name: "airflow-module-source-tag",
    setup(build) {
      build.onLoad({ filter: AUTHOR_SOURCE_FILTER }, async ({ path: file, namespace }) => {
        // Only the `file` namespace has a path on disk to attribute a Dag to;
        // a virtual module from another plugin has nothing to tag.
        if (namespace !== "file" || DEPENDENCY_PATH.test(file)) return undefined;
        const source = await readFileAsync(file, "utf-8");
        // Project-relative so the bundle is portable and readable (no host
        // filesystem prefix), matching how esbuild's own metafile keys sources.
        const relative = path.relative(cwd, file);
        const tag = `globalThis[Symbol.for(${slotKey})]=${JSON.stringify(relative)};\n`;
        return { contents: insertAfterShebang(source, tag), loader: loaderFor(file) };
      });
    },
  };
}

/** A shebang has to be the file's first bytes, so the tag goes after it. */
function insertAfterShebang(source: string, tag: string): string {
  if (!source.startsWith("#!")) return tag + source;
  const newline = source.indexOf("\n");
  if (newline === -1) return `${source}\n${tag}`;
  return `${source.slice(0, newline + 1)}${tag}${source.slice(newline + 1)}`;
}

function loaderFor(file: string): "ts" | "tsx" | "js" | "jsx" {
  if (/\.[cm]?tsx$/.test(file)) return "tsx";
  if (/\.[cm]?ts$/.test(file)) return "ts";
  if (/\.[cm]?jsx$/.test(file)) return "jsx";
  return "js";
}

/** Reads every unique source path the manifest names into a map the encoder embeds. Files
 *  imported by these but declaring no Dag stay out — the Code tab shows what defines each Dag,
 *  not what it depends on. */
function readDagSources(
  dagSourcePaths: BundleManifest["dag_source_paths"],
  cwd: string,
): Record<string, string> {
  const sources: Record<string, string> = {};
  for (const relative of new Set(Object.values(dagSourcePaths))) {
    sources[relative] = readFileSync(path.resolve(cwd, relative), "utf-8");
  }
  return sources;
}

export async function runPack(argv: readonly string[]): Promise<void> {
  const args = parsePackArgs(argv);
  const bundlePath = args.outfile;
  const stagingPath = path.join(path.dirname(bundlePath), STAGING_FILENAME);
  const { build } = await loadEsbuild();
  const cwd = process.cwd();

  try {
    await build({
      entryPoints: [args.entry],
      bundle: true,
      platform: "node",
      format: "esm",
      target: "node22",
      // Whitespace and syntax only. Identifiers stay: `dag.task(handler)` takes
      // the task id from the handler's name at run time, and `keepNames` carries
      // that name through any bundler-collision renames.
      minifyWhitespace: true,
      minifySyntax: true,
      keepNames: true,
      // The tag plugin attributes each `new Dag(...)` to its source file, so
      // one Dag-defining file per source region reaches the encoder.
      plugins: [moduleSourceTagPlugin(cwd)],
      // The manifest is read by running the staged bundle, so the metadata describes what ships.
      outfile: stagingPath,
    });

    const manifest = readBundleManifest(stagingPath);
    const dagEntries = Object.entries(manifest.task_handlers);
    if (dagEntries.length === 0) {
      throw new Error(
        `${args.entry} served nothing; register Dags or task handlers with bundle.register(...)`,
      );
    }
    // Warn rather than fail, as airflow-go-pack does: the shared schema allows a
    // Dag with no tasks.
    for (const [dagId, dag] of dagEntries) {
      if (dag.tasks.length === 0) {
        process.stderr.write(`warning: dag ${JSON.stringify(dagId)} has no tasks\n`);
      }
    }
    warnOnSuspiciousIds(manifest.task_handlers);

    const bundle = encodeBundle({
      bundleManifest: manifest,
      sdkVersion: readSdkVersion(),
      // One source file per native Dag. A bundle with mixed-lang Dags only
      // (owned by Python) has none, and this stays empty.
      sourceFiles: readDagSources(manifest.dag_source_paths, cwd),
      executable: readFileSync(stagingPath),
    });
    writeFileSync(bundlePath, bundle);
  } finally {
    rmSync(stagingPath, { force: true });
  }

  console.log(`Wrote ${bundlePath} (airflow metadata, source, and integrity embedded)`);
}
