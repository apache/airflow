#!/usr/bin/env node
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

import console from "node:console";
import { appendFileSync, readFileSync } from "node:fs";
import process from "node:process";
import { URL, pathToFileURL } from "node:url";

const PACKAGE_NAME = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
).name;
const SEMVER_PATTERN =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?$/;
const NPM_TAG_PATTERN = /^[a-z][a-z0-9._-]*$/;

function parseVersion(version) {
  const match = SEMVER_PATTERN.exec(version);
  if (match === null) {
    throw new Error(`Release version ${JSON.stringify(version)} is not valid SemVer`);
  }
  return {
    version,
    major: BigInt(match[1]),
    minor: BigInt(match[2]),
    patch: BigInt(match[3]),
    prerelease: match[4]?.split(".") ?? [],
  };
}

function compareIdentifiers(left, right) {
  if (left === right) {
    return 0;
  }
  const leftIsNumeric = /^\d+$/.test(left);
  const rightIsNumeric = /^\d+$/.test(right);
  if (leftIsNumeric && rightIsNumeric) {
    return BigInt(left) < BigInt(right) ? -1 : 1;
  }
  if (leftIsNumeric) {
    return -1;
  }
  if (rightIsNumeric) {
    return 1;
  }
  return left < right ? -1 : 1;
}

function compareVersions(left, right) {
  for (const field of ["major", "minor", "patch"]) {
    if (left[field] !== right[field]) {
      return left[field] < right[field] ? -1 : 1;
    }
  }
  if (left.prerelease.length === 0 || right.prerelease.length === 0) {
    return right.prerelease.length - left.prerelease.length;
  }
  const length = Math.max(left.prerelease.length, right.prerelease.length);
  for (let index = 0; index < length; index += 1) {
    const leftIdentifier = left.prerelease[index];
    const rightIdentifier = right.prerelease[index];
    if (leftIdentifier === undefined) {
      return -1;
    }
    if (rightIdentifier === undefined) {
      return 1;
    }
    const difference = compareIdentifiers(leftIdentifier, rightIdentifier);
    if (difference !== 0) {
      return difference;
    }
  }
  return 0;
}

function getPrereleaseChannel(prerelease) {
  return prerelease[0]?.match(/^[A-Za-z]+/)?.[0].toLowerCase();
}

export function validateReleaseInputs({ releaseTag, npmTag, currentDistTagVersion = "" }) {
  if (!releaseTag.startsWith("ts-sdk/")) {
    throw new Error("Release tag must have the form ts-sdk/<semver>");
  }
  const version = releaseTag.slice("ts-sdk/".length);
  const parsedVersion = parseVersion(version);

  if (!NPM_TAG_PATTERN.test(npmTag)) {
    throw new Error(`npm tag ${JSON.stringify(npmTag)} is invalid`);
  }
  if (parsedVersion.prerelease.length === 0 && npmTag !== "latest") {
    throw new Error("Stable releases must use the latest npm dist-tag");
  }
  if (parsedVersion.prerelease.length > 0) {
    const channel = getPrereleaseChannel(parsedVersion.prerelease);
    if (npmTag !== "next" && npmTag !== channel) {
      const allowedTags =
        channel === undefined || channel === "next" ? "next" : `${channel} or next`;
      throw new Error(`Prerelease ${version} must use the ${allowedTags} npm dist-tag`);
    }
  }

  if (currentDistTagVersion !== "") {
    const currentVersion = parseVersion(currentDistTagVersion);
    if (compareVersions(parsedVersion, currentVersion) <= 0) {
      throw new Error(
        `Release ${version} must be newer than ${npmTag}'s current version ${currentDistTagVersion}`,
      );
    }
  }

  return {
    version,
    packageName: PACKAGE_NAME,
    packageFile: `${PACKAGE_NAME}-${version}.tgz`,
  };
}

function run() {
  const result = validateReleaseInputs({
    releaseTag: process.env.RELEASE_TAG ?? "",
    npmTag: process.env.NPM_TAG ?? "",
    currentDistTagVersion: process.env.CURRENT_DIST_TAG_VERSION ?? "",
  });
  const outputFile = process.env.GITHUB_OUTPUT;
  if (outputFile === undefined) {
    throw new Error("GITHUB_OUTPUT is required");
  }
  appendFileSync(
    outputFile,
    `version=${result.version}\npackage_name=${result.packageName}\n` +
      `package_file=${result.packageFile}\n`,
  );
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    run();
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  }
}
