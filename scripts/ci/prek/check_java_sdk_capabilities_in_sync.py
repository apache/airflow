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
"""Keep ``java-sdk/generated/lang-sdk/capabilities.json`` in sync with the Kotlin capability constant.

The Java SDK declares what it supports in ``conformance.Capabilities`` (Kotlin), the single source
of truth. ``java-sdk/generated/lang-sdk/capabilities.json`` is a generated artifact consumed by the compatibility-matrix
prek hooks. This check runs the ``:sdk:dumpCapabilities`` Gradle task, compares the emitted manifest
with the committed file *by content* (formatting is irrelevant), and rewrites the committed file in
canonical form if it is stale.

Run from the repo root::

    uv run --project scripts python scripts/ci/prek/check_java_sdk_capabilities_in_sync.py

Exits 0 if ``capabilities.json`` matches the constant, 1 otherwise.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lang_sdk_compat_matrix import AIRFLOW_ROOT_PATH, CapabilitiesDoc, validate_capabilities

JAVA_SDK = AIRFLOW_ROOT_PATH / "java-sdk"
GRADLEW = JAVA_SDK / "gradlew"
GRADLE_PROPERTIES = JAVA_SDK / "gradle.properties"
CAPABILITIES_JSON = JAVA_SDK / "generated" / "lang-sdk" / "capabilities.json"


def format_canonical_json(doc: CapabilitiesDoc) -> str:
    return json.dumps(doc, indent=2) + "\n"


def read_gradle_schema_version() -> str | None:
    """The ``airflowSupervisorSchemaVersion`` from gradle.properties (source of truth for the JAR)."""
    for line in GRADLE_PROPERTIES.read_text().splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() == "airflowSupervisorSchemaVersion":
            return value.strip()
    return None


def generate_manifest() -> CapabilitiesDoc:
    """Run the Gradle dump task and parse the capability manifest it prints."""
    completed = subprocess.run(
        [str(GRADLEW), "-q", "-p", "java-sdk", ":sdk:dumpCapabilities"],
        cwd=AIRFLOW_ROOT_PATH,
        capture_output=True,
        text=True,
        check=True,
    )
    out = completed.stdout
    # Gradle may prepend warnings, so decode the first complete JSON value rather than slicing to
    # the last "}" — a stray brace in surrounding output would make that span the wrong text.
    start = out.find("{")
    if start == -1:
        raise ValueError(f"no JSON object found in dumpCapabilities output:\n{out}")
    doc, _ = json.JSONDecoder().raw_decode(out[start:])
    return doc


def main() -> int:
    if not GRADLEW.exists() or shutil.which("java") is None:
        if "CI" in os.environ:
            print("ERROR: java/gradlew is required in CI but was not found.")
            return 1
        print(
            "SKIPPED: java/gradlew not available; the committed java-sdk/generated/lang-sdk/capabilities.json was left "
            "UNVERIFIED and is only gated in CI."
        )
        return 0
    try:
        generated = generate_manifest()
    except (subprocess.CalledProcessError, ValueError) as error:
        detail = getattr(error, "stderr", None) or str(error)
        print(f"ERROR: could not run `:sdk:dumpCapabilities`:\n{detail}")
        return 1
    validate_capabilities(generated, source=":sdk:dumpCapabilities", expected_sdk="java")
    gradle_version = read_gradle_schema_version()
    if gradle_version is not None and generated["supervisor_schema_version"] != gradle_version:
        print(
            f"ERROR: supervisor_schema_version mismatch — the Kotlin constant emits "
            f"{generated['supervisor_schema_version']!r} but gradle.properties "
            f"airflowSupervisorSchemaVersion is {gradle_version!r} (the JAR manifest uses the latter). "
            "Update SUPERVISOR_SCHEMA_VERSION in Capabilities.kt to match."
        )
        return 1
    current = CAPABILITIES_JSON.read_text() if CAPABILITIES_JSON.exists() else ""
    if current and json.loads(current) == generated:
        print("OK: java-sdk/generated/lang-sdk/capabilities.json is in sync with conformance.Capabilities.")
        return 0
    CAPABILITIES_JSON.parent.mkdir(parents=True, exist_ok=True)
    CAPABILITIES_JSON.write_text(format_canonical_json(generated))
    print(
        "ERROR: java-sdk/generated/lang-sdk/capabilities.json was stale and has been regenerated from "
        "conformance.Capabilities. Re-stage the file."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
