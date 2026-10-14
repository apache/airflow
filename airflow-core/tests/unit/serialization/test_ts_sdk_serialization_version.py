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
from __future__ import annotations

import re

import pytest

from airflow.serialization.serialized_objects import DagSerialization

from tests_common.pytest_plugin import AIRFLOW_ROOT_PATH

GENERATED_FIELDS_PATH = AIRFLOW_ROOT_PATH / "ts-sdk" / "src" / "generated" / "dag-schema-fields.ts"

SERIALIZATION_VERSION_PATTERN = re.compile(
    r"^export const SERIALIZATION_VERSION = (?P<version>\d+);$", re.MULTILINE
)


@pytest.mark.skipif(
    not GENERATED_FIELDS_PATH.is_file(),
    reason="TypeScript SDK sources are absent (source distribution build)",
)
def test_ts_sdk_pins_the_current_serialization_version():
    """
    The schema constrains ``__version`` only to a positive integer, so the TypeScript SDK
    pins it by hand. Without this check, bumping the serializer would leave the SDK emitting
    Dags stamped with a version core no longer writes.
    """
    match = SERIALIZATION_VERSION_PATTERN.search(GENERATED_FIELDS_PATH.read_text(encoding="utf-8"))
    assert match is not None, f"No SERIALIZATION_VERSION export found in {GENERATED_FIELDS_PATH}"

    assert int(match.group("version")) == DagSerialization.SERIALIZER_VERSION, (
        f"{GENERATED_FIELDS_PATH.relative_to(AIRFLOW_ROOT_PATH)} pins serialization version "
        f"{match.group('version')}, but DagSerialization.SERIALIZER_VERSION is "
        f"{DagSerialization.SERIALIZER_VERSION}. Update SERIALIZATION_VERSION in "
        "ts-sdk/scripts/generate-dag-schema.mjs, re-run `pnpm run generate:dag-schema`, and make "
        "sure the SDK still emits Dags core can read."
    )
