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

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";

import { validateReleaseInputs } from "./validate-release-inputs.mjs";

describe("validateReleaseInputs", () => {
  it("accepts stable and prerelease channels", () => {
    assert.deepEqual(validateReleaseInputs({ releaseTag: "ts-sdk/1.0.0", npmTag: "latest" }), {
      version: "1.0.0",
      packageFile: "apache-airflow-ts-sdk-1.0.0.tgz",
    });
    assert.equal(
      validateReleaseInputs({ releaseTag: "ts-sdk/1.0.0-beta1", npmTag: "beta" }).version,
      "1.0.0-beta1",
    );
    assert.equal(
      validateReleaseInputs({ releaseTag: "ts-sdk/1.0.0-beta.2", npmTag: "next" }).version,
      "1.0.0-beta.2",
    );
  });

  it("rejects invalid versions", () => {
    for (const releaseTag of ["1.0.0", "ts-sdk/01.0.0", "ts-sdk/1.0.0-01", "ts-sdk/1.0"]) {
      assert.throws(
        () => validateReleaseInputs({ releaseTag, npmTag: "latest" }),
        /Release tag|not valid SemVer/,
      );
    }
  });

  it("rejects mismatched npm tags", () => {
    assert.throws(
      () => validateReleaseInputs({ releaseTag: "ts-sdk/1.0.0", npmTag: "beta" }),
      /Stable releases must use the latest/,
    );
    assert.throws(
      () => validateReleaseInputs({ releaseTag: "ts-sdk/1.0.0-beta1", npmTag: "latest" }),
      /must use the beta or next/,
    );
    assert.throws(
      () => validateReleaseInputs({ releaseTag: "ts-sdk/1.0.0-beta1", npmTag: "alpha" }),
      /must use the beta or next/,
    );
  });

  it("rejects malformed npm tags", () => {
    for (const npmTag of ["", "1.0.0", "Beta", "bad tag", "-beta"]) {
      assert.throws(
        () => validateReleaseInputs({ releaseTag: "ts-sdk/1.0.0-beta1", npmTag }),
        /npm tag|must use/,
      );
    }
  });

  it("requires a release to advance the selected dist-tag", () => {
    assert.equal(
      validateReleaseInputs({
        releaseTag: "ts-sdk/1.0.0-beta.2",
        npmTag: "beta",
        currentDistTagVersion: "1.0.0-beta.1",
      }).version,
      "1.0.0-beta.2",
    );
    for (const releaseTag of ["ts-sdk/1.0.0-beta.1", "ts-sdk/1.0.0-beta.0"]) {
      assert.throws(
        () =>
          validateReleaseInputs({
            releaseTag,
            npmTag: "beta",
            currentDistTagVersion: "1.0.0-beta.1",
          }),
        /must be newer/,
      );
    }
  });

  it("writes GitHub Actions outputs when run as a command", () => {
    const tempDirectory = mkdtempSync(join(tmpdir(), "ts-sdk-release-inputs-"));
    const outputFile = join(tempDirectory, "github-output");
    try {
      const result = spawnSync(
        process.execPath,
        [fileURLToPath(import.meta.resolve("./validate-release-inputs.mjs"))],
        {
          encoding: "utf8",
          env: {
            ...process.env,
            GITHUB_OUTPUT: outputFile,
            NPM_TAG: "beta",
            RELEASE_TAG: "ts-sdk/1.0.0-beta1",
          },
        },
      );
      assert.equal(result.status, 0, result.stderr);
      assert.equal(
        readFileSync(outputFile, "utf8"),
        "version=1.0.0-beta1\npackage_file=apache-airflow-ts-sdk-1.0.0-beta1.tgz\n",
      );
    } finally {
      rmSync(tempDirectory, { force: true, recursive: true });
    }
  });
});
