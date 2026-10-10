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
"""Unit tests for the YAML DAG version migrator (airflow.sdk.importers.yaml_importer.migrator)."""

from __future__ import annotations

import pytest
from cadwyn import (
    HeadVersion,
    Version,
    VersionBundle,
    VersionChange,
    convert_request_to_next_version_for,
    schema,
)

from airflow.sdk.importers.yaml_importer import migrator
from airflow.sdk.importers.yaml_importer.models import DagDocument, Task

HEAD_SCHEMA = migrator.schema_url("2026-10-30")


class _RenameOwnerAttr(VersionChange):
    "test-only: 2026-10-30 spelled the attribute `owner_old`; head renamed it to `owner`."

    description = __doc__
    instructions_to_migrate_to_previous_version = ()

    @convert_request_to_next_version_for(DagDocument)  # type: ignore[arg-type]
    def _move(request):
        if "owner_old" in request.body:
            request.body["owner"] = request.body.pop("owner_old")


_SYNTH_BUNDLE = VersionBundle(HeadVersion(), Version("2027-06-01", _RenameOwnerAttr), Version("2026-10-30"))


def _migrate(m, date, **extra):
    body = {"$schema": migrator.schema_url(date), "dag_id": "d", "tasks": [], **extra}
    return m.resolve_and_migrate(body)


def test_exact_version_pin_migrates_from_that_version():
    # Observed through the converter: it runs only when the pinned (exact) version is older than head.
    m = migrator.DagDocumentMigrator(_SYNTH_BUNDLE)
    assert _migrate(m, "2026-10-30", owner_old="t")["owner"] == "t"  # exact older version -> migrates
    # head-pinned: resolved source == head, so no converter runs
    at_head = _migrate(m, "2027-06-01", owner_old="t")
    assert at_head.get("owner_old") == "t"
    assert "owner" not in at_head


@pytest.mark.parametrize(
    "schema_value",
    [
        pytest.param(migrator.schema_url("2099-01-01"), id="newer-than-head"),
        pytest.param(migrator.schema_url("2020-01-01"), id="older-than-oldest"),
        pytest.param(migrator.schema_url("2027-03-01"), id="between-versions"),
        pytest.param("not-a-version", id="no-version-token"),
    ],
)
def test_unknown_version_is_rejected(schema_value):
    # Strict exact match, mirroring the supervisor migrator: newer, older, in-between, and
    # unreadable versions all raise -- there is no silent fallback to the head ruleset.
    m = migrator.DagDocumentMigrator(_SYNTH_BUNDLE)
    with pytest.raises(ValueError, match="not valid"):
        m.resolve_and_migrate({"$schema": schema_value, "dag_id": "d", "tasks": []})


def test_migrator_rejects_converter_for_non_dagdocument_model():
    class _ConvertTask(VersionChange):
        "test-only: a converter keyed on a nested model the migrator does not drive."

        description = __doc__
        instructions_to_migrate_to_previous_version = ()

        @convert_request_to_next_version_for(Task)  # type: ignore[arg-type]
        def _noop(request):
            pass

    bundle = VersionBundle(HeadVersion(), Version("2027-06-01", _ConvertTask), Version("2026-10-30"))
    with pytest.raises(RuntimeError, match="only whole-document DagDocument converters"):
        migrator.DagDocumentMigrator(bundle)


def test_migrator_rejects_non_request_instructions():
    class _RenameViaSchema(VersionChange):
        "test-only: a schema instruction the migrator does not apply."

        description = __doc__
        instructions_to_migrate_to_previous_version = (
            schema(DagDocument).field("schedule").had(name="schedule_interval"),
        )

    bundle = VersionBundle(HeadVersion(), Version("2027-06-01", _RenameViaSchema), Version("2026-10-30"))
    with pytest.raises(RuntimeError, match="unsupported cadwyn instructions"):
        migrator.DagDocumentMigrator(bundle)


def test_known_version_does_not_warn(recwarn):
    m = migrator.DagDocumentMigrator(_SYNTH_BUNDLE)
    _migrate(m, "2026-10-30")
    _migrate(m, "2027-06-01")
    assert not recwarn.list, "exact known versions must not warn"


def test_migrate_is_noop_at_head_for_the_real_bundle():
    body = {"$schema": HEAD_SCHEMA, "dag_id": "d", "tasks": []}
    assert migrator.get_migrator().resolve_and_migrate(body) is body  # one version -> no copy


class _AppendV2(VersionChange):
    "test-only: appends a marker so a run is observable and non-idempotent."

    description = __doc__
    instructions_to_migrate_to_previous_version = ()

    @convert_request_to_next_version_for(DagDocument)  # type: ignore[arg-type]
    def _move(request):
        request.body["trail"] = request.body.get("trail", "") + "+v2"


class _AppendV3(VersionChange):
    "test-only: appends a marker so a run is observable and non-idempotent."

    description = __doc__
    instructions_to_migrate_to_previous_version = ()

    @convert_request_to_next_version_for(DagDocument)  # type: ignore[arg-type]
    def _move(request):
        request.body["trail"] = request.body.get("trail", "") + "+v3"


_THREE_VERSIONS = VersionBundle(
    HeadVersion(),
    Version("2027-06-01", _AppendV3),
    Version("2027-01-01", _AppendV2),
    Version("2026-10-30"),
)


def test_migration_from_middle_version_skips_its_own_converter():
    # Pinning the middle version applies only the newer converter; the pinned
    # version's own converter is skipped.
    m = migrator.DagDocumentMigrator(_THREE_VERSIONS)
    body = {"$schema": migrator.schema_url("2027-01-01"), "dag_id": "d", "tasks": []}
    out = m.resolve_and_migrate(body)
    assert out["trail"] == "+v3"
    assert "trail" not in body  # the input body is not mutated


def test_migration_from_oldest_runs_every_newer_converter():
    m = migrator.DagDocumentMigrator(_THREE_VERSIONS)
    out = m.resolve_and_migrate({"$schema": migrator.schema_url("2026-10-30"), "dag_id": "d", "tasks": []})
    assert out["trail"] == "+v2+v3"  # oldest -> both converters, oldest-to-newest
