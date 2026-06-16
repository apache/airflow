#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Verify the Java SDK JDK version is consistent across all files that declare it.

The authoritative version lives in five places that must stay in sync:

- .github/workflows/ci-amd.yml
  -> env.JAVA_VERSION
- .github/workflows/ci-arm.yml
  -> env.JAVA_VERSION
- .github/workflows/codeql-analysis.yml
  -> steps[].with.java-version
- java-sdk/buildSrc/src/main/kotlin/airflow-jvm-conventions.gradle.kts
  -> java.toolchain.languageVersion.set(JavaLanguageVersion.of(<n>))
  -> java.sourceCompatibility = JavaVersion.VERSION_<n>
  -> kotlin.compilerOptions.jvmTarget = JvmTarget.JVM_<n>
- scripts/docker/install_os_dependencies.sh
  -> TEMURIN_VERSION=${TEMURIN_VERSION:-<n>}
- Dockerfile
  -> TEMURIN_VERSION=${TEMURIN_VERSION:-<n>}
- Dockerfile.ci
  -> TEMURIN_VERSION=${TEMURIN_VERSION:-<n>}
  -> ENV TEMURIN_VERSION="<n>"
"""

from __future__ import annotations

import dataclasses
import pathlib
import re
import sys


@dataclasses.dataclass
class VersionSite:
    """A single location that declares the Java version."""

    label: str
    path: pathlib.Path
    pattern: re.Pattern[str]

    def extract(self) -> str | None:
        """Find version string with regex."""
        if m := self.pattern.search(self.path.read_text()):
            return m.group(1)
        return None


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

WORKFLOWS = REPO_ROOT.joinpath(".github", "workflows")
SDK_CONVENTION = REPO_ROOT.joinpath("java-sdk/buildSrc/src/main/kotlin/airflow-jvm-conventions.gradle.kts")

SITES = [
    VersionSite(
        label=".github/workflows/ci-amd.yml  (env.JAVA_VERSION)",
        path=WORKFLOWS.joinpath("ci-amd.yml"),
        pattern=re.compile(r"^\s+JAVA_VERSION:\s+'(\d+)'", re.MULTILINE),
    ),
    VersionSite(
        label=".github/workflows/ci-arm.yml  (env.JAVA_VERSION)",
        path=WORKFLOWS.joinpath("ci-arm.yml"),
        pattern=re.compile(r"^\s+JAVA_VERSION:\s+'(\d+)'", re.MULTILINE),
    ),
    VersionSite(
        label=".github/workflows/codeql-analysis.yml  (steps[].with.java-version)",
        path=WORKFLOWS.joinpath("codeql-analysis.yml"),
        pattern=re.compile(r"^\s+java-version:\s+'(\d+)'", re.MULTILINE),
    ),
    VersionSite(
        label="java-sdk/buildSrc/.../airflow-jvm-conventions.gradle.kts  (JavaLanguageVersion.of(...))",
        path=SDK_CONVENTION,
        pattern=re.compile(r"JavaLanguageVersion\.of\((\d+)\)"),
    ),
    VersionSite(
        label="java-sdk/buildSrc/.../airflow-jvm-conventions.gradle.kts  (JavaVersion.VERSION_...)",
        path=SDK_CONVENTION,
        pattern=re.compile(r"JavaVersion\.VERSION_(\d+)"),
    ),
    VersionSite(
        label="java-sdk/buildSrc/.../airflow-jvm-conventions.gradle.kts  (JvmTarget.JVM_...)",
        path=SDK_CONVENTION,
        pattern=re.compile(r"JvmTarget\.JVM_(\d+)"),
    ),
    VersionSite(
        label="scripts/docker/install_os_dependencies.sh  (TEMURIN_VERSION default)",
        path=REPO_ROOT.joinpath("scripts", "docker", "install_os_dependencies.sh"),
        pattern=re.compile(r"^TEMURIN_VERSION=\$\{TEMURIN_VERSION:-(\d+)\}", re.MULTILINE),
    ),
    VersionSite(
        label="Dockerfile  (TEMURIN_VERSION heredoc default)",
        path=REPO_ROOT.joinpath("Dockerfile"),
        pattern=re.compile(r"^TEMURIN_VERSION=\$\{TEMURIN_VERSION:-(\d+)\}", re.MULTILINE),
    ),
    VersionSite(
        label="Dockerfile.ci  (TEMURIN_VERSION heredoc default)",
        path=REPO_ROOT.joinpath("Dockerfile.ci"),
        pattern=re.compile(r"^TEMURIN_VERSION=\$\{TEMURIN_VERSION:-(\d+)\}", re.MULTILINE),
    ),
    VersionSite(
        label="Dockerfile.ci  (ENV TEMURIN_VERSION)",
        path=REPO_ROOT.joinpath("Dockerfile.ci"),
        pattern=re.compile(r'^ENV TEMURIN_VERSION="(\d+)"', re.MULTILINE),
    ),
]


def main() -> int:
    results = [(site, site.extract()) for site in SITES]

    if not_found := [(site, v) for site, v in results if v is None]:
        for site, _ in not_found:
            print(f"ERROR: Java version pattern not found in {site.path.relative_to(REPO_ROOT)}")
            print(f"       Pattern: {site.pattern.pattern!r}")
        return len(not_found)

    versions = {v for _, v in results}
    if len(versions) == 1:
        (version,) = versions
        print(f"OK: Java SDK version is consistently {version} across all {len(SITES)} locations.")
        return 0

    print("ERROR: Java SDK version is inconsistent across files:")
    print()
    col = max(len(site.label) for site, _ in results)
    for site, version in results:
        print(f"  {site.label:<{col}}  {version}")
    print()
    print("Update all locations to the same version, or adjust the patterns in")
    print(f"  {pathlib.Path(__file__).relative_to(REPO_ROOT)}")
    print("if the pattern no longer matches.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
