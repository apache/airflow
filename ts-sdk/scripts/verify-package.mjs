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

import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

// A packaging check must never be the reason a static-check run hangs: npm install reaches
// the registry, and pnpm pack shells out to a full TypeScript build.
const COMMAND_TIMEOUT_MS = 10 * 60 * 1000;

const requiredRootFiles = ["LICENSE", "NOTICE", "README.md", "package.json"];

// The Dag-authoring entrypoints a consumer must be able to reach from the package root.
const REQUIRED_ROOT_EXPORTS = ["Dag", "DagRegistry", "serveDags"];

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: "utf8",
    timeout: COMMAND_TIMEOUT_MS,
    maxBuffer: 16 * 1024 * 1024,
    ...options,
  });
  if (result.error) {
    throw new Error(`${command} ${args.join(" ")} failed: ${result.error.message}`, {
      cause: result.error,
    });
  }
  if (result.status !== 0) {
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    throw new Error(`${command} ${args.join(" ")} failed with exit code ${result.status}`);
  }
  return result;
}

function isAllowedFile(path) {
  return requiredRootFiles.includes(path) || /^dist\/.+\.(?:js|d\.ts)$/.test(path);
}

/**
 * Every path a consumer can resolve — the `exports` conditions plus the `bin` targets. Derived
 * from package.json so adding an export subpath extends the check instead of silently escaping it.
 */
function collectEntryPoints(packageJson) {
  const targets = Object.values(packageJson.exports ?? {}).flatMap((conditions) =>
    typeof conditions === "string" ? [conditions] : Object.values(conditions),
  );
  targets.push(...Object.values(packageJson.bin ?? {}));
  return targets.map((target) => target.replace(/^\.\//, ""));
}

/**
 * The `exports` subpath keys as specifier suffixes: `"."` becomes `""` and `"./coordinator"`
 * becomes `"/coordinator"`. Presence in the tarball is not resolution — a subpath can ship and
 * still fail to import if its conditions are wrong or an internal import is broken.
 */
function collectExportSubpaths(packageJson) {
  return Object.keys(packageJson.exports ?? { ".": {} }).map((subpath) =>
    subpath === "." ? "" : subpath.replace(/^\./, ""),
  );
}

const temporaryDirectory = mkdtempSync(join(tmpdir(), "airflow-ts-sdk-package-"));

try {
  const packageJson = JSON.parse(readFileSync("package.json", "utf8"));
  const requiredFiles = [...requiredRootFiles, ...collectEntryPoints(packageJson)];

  const packed = run("pnpm", [
    "--silent",
    "pack",
    "--json",
    "--pack-destination",
    temporaryDirectory,
  ]);
  // `--silent` does not suppress the `prepack` lifecycle banner, so stdout is build log followed
  // by the JSON report — take the trailing object rather than parsing the whole stream.
  const metadataMatch = packed.stdout.match(/(?:^|\n)(\{[\s\S]*\})\s*$/);
  if (!metadataMatch) {
    throw new Error("pnpm pack did not return package metadata");
  }
  const metadata = JSON.parse(metadataMatch[1]);
  if (!Array.isArray(metadata.files) || typeof metadata.filename !== "string") {
    throw new Error("pnpm pack returned invalid package metadata");
  }
  const paths = new Set(metadata.files.map(({ path }) => path));

  const missing = requiredFiles.filter((path) => !paths.has(path));
  const unexpected = [...paths].filter((path) => !isAllowedFile(path));
  if (missing.length > 0 || unexpected.length > 0) {
    throw new Error(
      [
        missing.length > 0 ? `missing required files: ${missing.join(", ")}` : "",
        unexpected.length > 0 ? `unexpected files: ${unexpected.join(", ")}` : "",
      ]
        .filter(Boolean)
        .join("; "),
    );
  }

  if (metadata.name !== packageJson.name || metadata.version !== packageJson.version) {
    throw new Error("packed package identity does not match package.json");
  }

  const consumerDirectory = join(temporaryDirectory, "consumer");
  mkdirSync(consumerDirectory);
  writeFileSync(
    join(consumerDirectory, "package.json"),
    JSON.stringify({ name: "airflow-ts-sdk-package-smoke-test", private: true, type: "module" }),
  );
  run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", metadata.filename], {
    cwd: consumerDirectory,
  });
  run(
    "node",
    [
      "--input-type=module",
      "--eval",
      [
        ...collectExportSubpaths(packageJson).map(
          (subpath) => `await import(${JSON.stringify(packageJson.name + subpath)});`,
        ),
        `const sdk = await import(${JSON.stringify(packageJson.name)});`,
        `for (const name of ${JSON.stringify(REQUIRED_ROOT_EXPORTS)}) {`,
        '  if (typeof sdk[name] !== "function") {',
        "    throw new Error(`the root export does not expose ${name}`);",
        "  }",
        "}",
      ].join("\n"),
    ],
    { cwd: consumerDirectory },
  );

  // Spawning the installed bin shim proves it exists, is executable, and resolves its imports.
  // Asserting only "exited non-zero, and blamed itself" keeps this a packaging check — the CLI's
  // own argument handling is covered by its unit tests.
  const [binName] = Object.keys(packageJson.bin ?? {});
  if (!binName) {
    throw new Error("package.json declares no bin entry to verify");
  }
  const executable = join(consumerDirectory, "node_modules", ".bin", binName);
  const cli = spawnSync(executable, [], {
    cwd: consumerDirectory,
    encoding: "utf8",
    timeout: COMMAND_TIMEOUT_MS,
  });
  if (cli.error) {
    throw new Error(`${binName} failed: ${cli.error.message}`, { cause: cli.error });
  }
  if (cli.status === 0 || !cli.stderr.includes(binName)) {
    throw new Error(
      `${binName} did not report a usage error when invoked without arguments ` +
        `(exit ${cli.status}, stderr: ${JSON.stringify(cli.stderr.trim())})`,
    );
  }

  console.log(`Verified ${metadata.name}@${metadata.version} (${paths.size} files)`);
} finally {
  rmSync(temporaryDirectory, { recursive: true, force: true });
}
