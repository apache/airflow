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
Unit tests for :mod:`airflow.sdk.execution_time.schema.migrator`.

These pin the in-process supervisor schema migration path -- both
directions: ``downgrade`` (supervisor head -> foreign-runtime client
version) and ``upgrade`` (foreign-runtime client version -> supervisor
head). The downgrade direction is what coordinators use to hand a
runtime a body shaped for its build; the upgrade direction is what the
supervisor will use to decode runtime-originated frames once the wire
schema diverges from head.
"""

from __future__ import annotations

import subprocess
import sys
from typing import Literal

import pytest
from cadwyn import (
    HeadVersion,
    Version,
    VersionBundle,
    VersionChange,
    convert_request_to_next_version_for,
    schema,
)
from pydantic import BaseModel
from task_sdk.execution_time.schema._mock_version_bundle import (
    MOCK_REGISTRY,
    _LangSdkRequest,
    _SupervisorResponse,
)

from airflow.sdk.execution_time.schema import (
    SchemaVersionMigrator,
    get_schema_version_migrator,
    resolve_body_class,
)


class _MockBody(BaseModel):
    """Mock body class used to drive bundle-level migration tests."""

    type: Literal["MockBody"] = "MockBody"
    ti_id: str
    queue_capacity: int | None = None
    sentry_trace_id: str | None = None


class _IntroduceQueueCapacity(VersionChange):
    """3026-04-17: introduce queue_capacity."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_MockBody).field("queue_capacity").didnt_exist,)

    # Upgrade direction: a client on the pre-04-17 wire shape sends no
    # ``queue_capacity``; once the body crosses into 04-17 we backfill
    # the field with a sentinel so the head Pydantic class can validate.
    @convert_request_to_next_version_for(_MockBody)  # type: ignore[arg-type]
    def _backfill_queue_capacity(request):
        request.body.setdefault("queue_capacity", 0)


class _IntroduceSentryTrace(VersionChange):
    """3026-06-16: introduce sentry_trace_id."""

    description = __doc__
    instructions_to_migrate_to_previous_version = (schema(_MockBody).field("sentry_trace_id").didnt_exist,)

    @convert_request_to_next_version_for(_MockBody)  # type: ignore[arg-type]
    def _backfill_sentry_trace(request):
        request.body.setdefault("sentry_trace_id", "")


_BUNDLE = VersionBundle(
    HeadVersion(),
    Version("3026-06-16", _IntroduceSentryTrace),
    Version("3026-04-17", _IntroduceQueueCapacity),
    Version("3025-01-01"),
)

# The latest *dated* entry (Cadwyn puts HeadVersion first but its value
# is not a date string; the first dated entry is at index 0 of the
# non-head versions).
_LATEST_VERSION = "3026-06-16"


class TestSchemaVersionMigratorDowngrade:
    """
    Drive the downgrade direction against a mock bundle so we can pin
    *field-level* migration behaviour independent of the real bundle's
    contents. The real bundle's ``stub_args`` migration is covered by
    :class:`TestRealBundleStubArgsDowngrade` below.
    """

    @pytest.fixture
    def migrator(self) -> SchemaVersionMigrator:
        # ``supervisor_version`` must be supplied explicitly; we pin it
        # to the latest dated entry in the test bundle, mirroring how
        # ``get_schema_version_migrator`` builds the real instance.
        return SchemaVersionMigrator(bundle=_BUNDLE, supervisor_version=_LATEST_VERSION)

    def _body(self) -> _MockBody:
        return _MockBody(
            ti_id="t1",
            queue_capacity=8,
            sentry_trace_id="00-trace-span-00",
        )

    def test_supervisor_version_is_latest_dated_entry(self, migrator):
        # ``_supervisor_version`` is the private attrs attribute; the
        # reimplementation does not expose a public ``supervisor_version``
        # property.
        assert migrator._supervisor_version == _LATEST_VERSION

    def test_head_version_returns_every_field(self, migrator):
        out = migrator.downgrade(self._body(), _LATEST_VERSION).model_dump()
        assert out["ti_id"] == "t1"
        assert out["queue_capacity"] == 8
        assert out["sentry_trace_id"] == "00-trace-span-00"

    def test_middle_version_strips_only_later_fields(self, migrator):
        # 3026-04-17 predates sentry_trace_id but knows about queue_capacity.
        out = migrator.downgrade(self._body(), "3026-04-17").model_dump()
        assert out["queue_capacity"] == 8
        assert "sentry_trace_id" not in out

    def test_baseline_strips_every_later_field(self, migrator):
        out = migrator.downgrade(self._body(), "3025-01-01").model_dump()
        assert out["ti_id"] == "t1"
        assert "queue_capacity" not in out
        assert "sentry_trace_id" not in out

    def test_downgrade_returns_pydantic_model_instance(self, migrator):
        # The reimplementation returns the Cadwyn-versioned Pydantic
        # model, not a plain dict.  Callers (e.g. _serialize_response)
        # call ``.model_dump()`` on the result themselves.
        result = migrator.downgrade(self._body(), "3025-01-01")
        assert isinstance(result, BaseModel)


class TestSchemaVersionMigratorUpgrade:
    """
    Mirror of the downgrade suite for the upgrade direction. The mock
    bundle's ``convert_request_to_next_version_for`` hooks backfill the
    new field at the version that introduces it, so a body off an
    older wire reaches the head with every field present.

    ``upgrade`` returns a plain dict (the result of
    ``versioned_class.model_validate(info.body).model_dump()``).
    """

    @pytest.fixture
    def migrator(self) -> SchemaVersionMigrator:
        return SchemaVersionMigrator(bundle=_BUNDLE, supervisor_version=_LATEST_VERSION)

    def test_baseline_client_payload_is_filled_up_to_head(self, migrator):
        # A client on the very first defined version sends only the
        # always-present field. Both 04-17 and 06-16 must run, each
        # backfilling its own newly-introduced field.
        out = migrator.upgrade({"ti_id": "t1"}, _MockBody, "3025-01-01")
        assert out["ti_id"] == "t1"
        assert out["queue_capacity"] == 0
        assert out["sentry_trace_id"] == ""

    def test_middle_client_payload_only_runs_later_versions(self, migrator):
        # Client built against 04-17 already provides queue_capacity;
        # only the 06-16 backfill should run on top.
        out = migrator.upgrade(
            {"ti_id": "t1", "queue_capacity": 8},
            _MockBody,
            "3026-04-17",
        )
        assert out["queue_capacity"] == 8  # the existing value is preserved
        assert out["sentry_trace_id"] == ""  # backfilled by 06-16

    def test_head_client_payload_is_returned_verbatim(self, migrator):
        # A client already on head needs no upgrade; the only diff from
        # *original* is the discriminator filled in by the final
        # ``model_validate`` round-trip (mirroring ``downgrade``).
        original = {"ti_id": "t1", "queue_capacity": 8, "sentry_trace_id": "00"}
        out = migrator.upgrade(dict(original), _MockBody, _LATEST_VERSION)
        assert out == {**original, "type": "MockBody"}

    def test_upgrade_returns_dict(self, migrator):
        # Unlike ``downgrade``, ``upgrade`` returns a plain dict (the
        # head shape ready for ``model_validate`` by the real decoder).
        out = migrator.upgrade({"ti_id": "t1"}, _MockBody, "3025-01-01")
        assert isinstance(out, dict)


class TestSchemaVersionMigratorVersionStringValidation:
    """``_resolve_version`` requires the version string to be present in the bundle."""

    @pytest.fixture
    def migrator(self) -> SchemaVersionMigrator:
        return SchemaVersionMigrator(bundle=_BUNDLE, supervisor_version=_LATEST_VERSION)

    @pytest.mark.parametrize(
        "bad_version",
        [
            pytest.param("not-a-date", id="freeform-text"),
            pytest.param("2026/04/17", id="slash-separator"),
            pytest.param("26-04-17", id="two-digit-year"),
            pytest.param("2026-4-17", id="single-digit-month"),
            pytest.param("", id="empty-string"),
        ],
    )
    def test_rejects_versions_not_in_bundle(self, migrator, bad_version):
        # The reimplementation validates only bundle membership; there is
        # no regex format check.  Any string absent from the bundle raises
        # ValueError with "not found in supervisor schema bundle".
        with pytest.raises(ValueError, match="not found in supervisor schema bundle"):
            migrator.downgrade(_MockBody(ti_id="t1"), bad_version)

    def test_rejects_well_formed_date_not_in_bundle(self, migrator):
        with pytest.raises(ValueError, match="not found in supervisor schema bundle"):
            migrator.downgrade(_MockBody(ti_id="t1"), "2999-01-01")

    def test_rejects_version_not_in_bundle_for_upgrade(self, migrator):
        with pytest.raises(ValueError, match="not found in supervisor schema bundle"):
            migrator.upgrade({"ti_id": "t1"}, _MockBody, "2999-01-01")


class TestSchemaVersionMigratorConstructorValidation:
    """The ``supervisor_version`` constructor arg must be present in the bundle."""

    def test_rejects_supervisor_version_not_in_bundle(self):
        with pytest.raises(ValueError, match="not found in supervisor schema bundle"):
            SchemaVersionMigrator(bundle=_BUNDLE, supervisor_version="2999-01-01")

    def test_accepts_any_dated_version_in_bundle(self):
        for version in ("3025-01-01", "3026-04-17", _LATEST_VERSION):
            migrator = SchemaVersionMigrator(bundle=_BUNDLE, supervisor_version=version)
            assert migrator._supervisor_version == version


class TestSchemaVersionMigratorRespectsExplicitSupervisorVersion:
    """
    A migrator pinned to an older ``supervisor_version`` must stop walking
    once the chain reaches that anchor. This is the knob a coordinator
    on a non-head build would use to clamp the upgrade walk so that
    transformers above its own version are not applied.

    Only the upgrade direction is asserted here: the downgrade walk
    delegates the final field-shape to ``generate_versioned_models``
    keyed by *target_schema_version*, which is independent of the
    supervisor anchor, so the anchor has no observable effect when the
    inbound body is already shaped for *supervisor_version*.
    """

    def test_upgrade_does_not_apply_changes_above_supervisor_anchor(self):
        migrator = SchemaVersionMigrator(bundle=_BUNDLE, supervisor_version="3026-04-17")
        out = migrator.upgrade({"ti_id": "t1"}, _MockBody, "3025-01-01")
        # The 04-17 backfill ran; the 06-16 backfill did not.
        assert out["queue_capacity"] == 0
        assert "sentry_trace_id" not in out


class TestGetSchemaVersionMigrator:
    def test_returns_singleton(self):
        # The cached factory must return the same instance across calls
        # so callers can share state-free migrator instances cheaply.
        assert get_schema_version_migrator() is get_schema_version_migrator()

    def test_is_bound_to_supervisor_bundle(self):
        # Sanity check: the singleton uses the real supervisor schema
        # bundle, not a mock one and not the execution-API HTTP bundle.
        # A regression here would silently detach the supervisor from
        # its versioning source of truth.
        from airflow.sdk.execution_time.schema.versions import bundle

        assert get_schema_version_migrator()._bundle is bundle

    def test_supervisor_version_defaults_to_real_bundle_head(self):
        # The supervisor anchor must be the latest dated entry in the
        # real bundle -- never the head sentinel, never silently older.
        from airflow.sdk.execution_time.schema.versions import bundle

        assert get_schema_version_migrator()._supervisor_version == bundle.versions[0].value


class TestResolveBodyClass:
    """All branches of :func:`resolve_body_class` with the mock registry."""

    @pytest.fixture
    def mock_registry(self, monkeypatch):
        """Swap the real ``registered_models_by_name`` for the mock registry."""
        monkeypatch.setattr(
            "airflow.sdk.execution_time.schema.registered_models_by_name",
            lambda: MOCK_REGISTRY,
        )

    def test_non_dict_returns_none(self, mock_registry):
        assert resolve_body_class("not a dict") is None
        assert resolve_body_class(42) is None
        assert resolve_body_class(None) is None
        assert resolve_body_class(["type", "Foo"]) is None

    def test_missing_type_key_returns_none(self, mock_registry):
        assert resolve_body_class({}) is None
        assert resolve_body_class({"ti_id": "x"}) is None

    def test_non_string_type_value_returns_none(self, mock_registry):
        assert resolve_body_class({"type": 123}) is None
        assert resolve_body_class({"type": None}) is None
        assert resolve_body_class({"type": ["_LangSdkRequest"]}) is None

    def test_unknown_discriminator_returns_none(self, mock_registry):
        assert resolve_body_class({"type": "NoSuchModel"}) is None

    def test_known_discriminator_returns_head_class(self, mock_registry):
        assert resolve_body_class({"type": "_LangSdkRequest"}) is _LangSdkRequest
        assert resolve_body_class({"type": "_SupervisorResponse"}) is _SupervisorResponse

    def test_extra_fields_in_body_do_not_affect_resolution(self, mock_registry):
        body = {"type": "_LangSdkRequest", "ti_id": "t1", "field_a": 7}
        assert resolve_body_class(body) is _LangSdkRequest


class TestLazyCadwynImport:
    """``cadwyn`` (which imports FastAPI/Starlette/Jinja2) must stay off the worker import path.

    The Task SDK supervisor imports the schema package on every Celery pool worker, but ``cadwyn`` is
    only needed on the foreign-language-SDK migration path, so its import is deferred. Each check runs
    in a fresh interpreter because ``sys.modules`` is process-global and other tests import cadwyn.
    """

    @pytest.mark.parametrize(
        "module",
        [
            "airflow.sdk.execution_time.schema",
            "airflow.sdk.execution_time.supervisor",
        ],
    )
    def test_importing_worker_path_does_not_load_cadwyn(self, module):
        code = (
            f"import sys; import {module}; "
            "assert 'cadwyn' not in sys.modules, sorted(m for m in sys.modules if 'cadwyn' in m); "
            "assert 'fastapi' not in sys.modules, 'fastapi was imported'"
        )
        subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)

    def test_accessing_bundle_loads_cadwyn(self):
        # The foreign-SDK migration path does need cadwyn; accessing the bundle is where it loads.
        code = (
            "import sys; from airflow.sdk.execution_time.schema import bundle; "
            "assert bundle.versions, 'bundle should be built'; "
            "assert 'cadwyn' in sys.modules, 'cadwyn should load when the bundle is accessed'"
        )
        subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)


class TestRealBundleStubArgsDowngrade:
    """
    Drive the *real* supervisor bundle through the ``stub_args`` migration.

    ``AddStubArgsToTIRunContext`` is the bundle's first ``schema(...)``
    instruction on a model *nested* inside a registered body
    (``StartupDetails.ti_context``); this pins that the downgrade
    re-validation strips the nested field on the wire for a runtime
    pinned to the previous version, and keeps it at head.
    """

    @pytest.fixture
    def startup_details(self):
        import datetime
        import uuid

        from airflow.sdk.api.datamodels._generated import (
            BundleInfo,
            DagRun,
            DagRunState,
            DagRunType,
            TaskInstance,
            TIRunContext,
        )
        from airflow.sdk.execution_time.comms import StartupDetails

        now = datetime.datetime.now(datetime.timezone.utc)
        return StartupDetails(
            ti=TaskInstance(
                id=uuid.uuid4(),
                task_id="transform",
                dag_id="d",
                run_id="r",
                try_number=1,
                dag_version_id=uuid.uuid4(),
            ),
            dag_rel_path="d.py",
            bundle_info=BundleInfo(name="b", version=None),
            start_date=now,
            ti_context=TIRunContext(
                dag_run=DagRun(
                    dag_id="d",
                    run_id="r",
                    logical_date=now,
                    start_date=now,
                    run_type=DagRunType.MANUAL,
                    state=DagRunState.RUNNING,
                    run_after=now,
                    consumed_asset_events=[],
                ),
                max_tries=1,
                stub_args=[
                    {"kind": "literal", "data_type": "string", "value": "uk"},
                    {"kind": "xcom", "data_type": "object", "task_id": "extract", "key": "return_value"},
                ],
            ),
            sentry_integration="",
        )

    @pytest.fixture
    def real_migrator(self) -> SchemaVersionMigrator:
        return get_schema_version_migrator()

    def test_downgrade_strips_stub_args_for_previous_version(self, real_migrator, startup_details):
        out = real_migrator.downgrade(startup_details, "2026-06-16").model_dump()
        assert "stub_args" not in out["ti_context"]

    def test_head_version_keeps_stub_args(self, real_migrator, startup_details):
        out = real_migrator.downgrade(startup_details, "2026-07-30")
        assert out.ti_context.stub_args is not None
        assert [a.kind for a in out.ti_context.stub_args] == ["literal", "xcom"]
