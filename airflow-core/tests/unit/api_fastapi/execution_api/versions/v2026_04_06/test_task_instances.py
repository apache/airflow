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

from dataclasses import dataclass

import pytest

from airflow._shared.timezones import timezone
from airflow.sdk import Asset, AssetAlias
from airflow.sdk.definitions.asset.access_control import AssetAccessControl
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.state import DagRunState, State

from tests_common.test_utils.db import clear_db_runs
from tests_common.test_utils.format_datetime import from_datetime_to_zulu_without_ms

pytestmark = pytest.mark.db_test

TIMESTAMP_STR = "2024-09-30T12:00:00Z"
TIMESTAMP = timezone.parse(TIMESTAMP_STR)

RUN_PATCH_BODY = {
    "state": "running",
    "hostname": "test-hostname",
    "unixname": "test-user",
    "pid": 12345,
    "start_date": TIMESTAMP_STR,
}


@dataclass
class _ResumePayload:
    value: int


@pytest.fixture
def old_ver_client(client):
    """Last released execution API before nullable DagRun.start_date (2026-04-06 bundle)."""
    client.headers["Airflow-API-Version"] = "2025-11-05"
    return client


class TestDagRunStartDateNullableBackwardCompat:
    """Test that older API versions get a non-null start_date fallback."""

    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_old_version_gets_run_after_when_start_date_is_null(
        self,
        old_ver_client,
        session,
        create_task_instance,
    ):
        ti = create_task_instance(
            task_id="test_start_date_nullable",
            state=State.QUEUED,
            dagrun_state=DagRunState.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.dag_run.start_date = None  # DagRun has not started yet
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        dag_run = response.json()["dag_run"]

        assert response.status_code == 200
        assert dag_run["start_date"] is not None
        assert dag_run["start_date"] == dag_run["run_after"]

    def test_head_version_allows_null_start_date(
        self,
        client,
        session,
        create_task_instance,
    ):
        ti = create_task_instance(
            task_id="test_start_date_null_head",
            state=State.QUEUED,
            dagrun_state=DagRunState.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.dag_run.start_date = None  # DagRun has not started yet
        session.commit()

        response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        dag_run = response.json()["dag_run"]

        assert response.status_code == 200
        assert dag_run["start_date"] is None

    def test_old_version_preserves_real_start_date(
        self,
        old_ver_client,
        session,
        create_task_instance,
    ):
        ti = create_task_instance(
            task_id="test_start_date_preserved",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        assert ti.dag_run.start_date == TIMESTAMP  # DagRun has already started
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        dag_run = response.json()["dag_run"]

        assert response.status_code == 200
        assert dag_run["start_date"] is not None, "start_date should not be None when DagRun has started"
        assert dag_run["start_date"] == from_datetime_to_zulu_without_ms(TIMESTAMP)


class TestNextKwargsBackwardCompat:
    """Old workers only know BaseSerialization.deserialize -- SDK serde plain dicts cause KeyError."""

    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_old_version_gets_base_serialization_format(self, old_ver_client, session, create_task_instance):
        """Old API version receives next_kwargs wrapped in __type/__var so BaseSerialization can parse it."""
        ti = create_task_instance(
            task_id="test_next_kwargs_compat",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        # Store SDK serde format (plain dict) in DB -- this is what trigger.py handle_event_submit produces
        ti.next_method = "execute_complete"
        ti.next_kwargs = {"cheesecake": True, "event": "payload"}
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        next_kwargs = response.json()["next_kwargs"]
        # Old workers call BaseSerialization.deserialize on this -- verify it works
        result = BaseSerialization.deserialize(next_kwargs)
        assert result == {"cheesecake": True, "event": "payload"}

    def test_old_version_deserializes_complex_types(self, old_ver_client, session, create_task_instance):
        """Non-primitive values (datetime) must round-trip through serde -> BaseSerialization correctly."""
        from airflow.sdk.serde import serialize as serde_serialize

        original = {"event": TIMESTAMP, "simple": True}
        # Store SDK serde format with a datetime -- this is what handle_event_submit produces
        # when the trigger payload contains a datetime (e.g. DateTimeSensorAsync)
        serde_encoded = serde_serialize(original)

        ti = create_task_instance(
            task_id="test_next_kwargs_datetime",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = serde_encoded
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        next_kwargs = response.json()["next_kwargs"]
        result = BaseSerialization.deserialize(next_kwargs)
        assert result["simple"] is True
        # datetime must come back as a datetime, not a {"__classname__": ...} dict
        assert result["event"] == TIMESTAMP

    def test_old_version_translates_assets_without_constructing_them(
        self, old_ver_client, session, create_task_instance, monkeypatch
    ):
        """Assets and asset aliases reach old workers in the legacy format without being rebuilt here."""
        from airflow.sdk.serde import serialize as serde_serialize

        original = {
            "assets": [Asset("resume-data", uri="s3://bucket/key", group="g", extra={"k": 1})],
            "alias": AssetAlias("resume-alias"),
            "event": TIMESTAMP,
        }
        expected = BaseSerialization.serialize(original)

        ti = create_task_instance(
            task_id="test_next_kwargs_assets",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = serde_serialize(original)
        session.commit()

        constructed = []
        for cls in (Asset, AssetAlias):
            original_init = cls.__init__

            def spy(self, *args, _original_init=original_init, **kwargs):
                constructed.append(type(self))
                _original_init(self, *args, **kwargs)

            monkeypatch.setattr(cls, "__init__", spy)

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert constructed == []
        assert response.json()["next_method"] == "execute_complete"
        next_kwargs = response.json()["next_kwargs"]
        assert next_kwargs == expected
        result = BaseSerialization.deserialize(next_kwargs)
        assert [(a.name, a.uri, a.group, a.extra) for a in result["assets"]] == [
            ("resume-data", "s3://bucket/key", "g", {"k": 1})
        ]
        assert (result["alias"].name, result["alias"].group) == ("resume-alias", "asset")
        assert result["event"] == TIMESTAMP

    @pytest.mark.parametrize(
        ("access_control", "expected_access_control"),
        [
            pytest.param(
                AssetAccessControl(producer_teams=["team_a"], allow_global=False),
                {"producer_teams": ["team_a"], "allow_global": False},
                id="producers-only",
            ),
            pytest.param(
                AssetAccessControl(consumer_teams=["team_b"]),
                {"producer_teams": [], "allow_global": True, "consumer_teams": ["team_b"]},
                id="consumers",
            ),
        ],
    )
    def test_old_version_keeps_asset_access_control(
        self,
        old_ver_client,
        session,
        create_task_instance,
        monkeypatch,
        access_control,
        expected_access_control,
    ):
        """Asset access restrictions survive the translation without the classes being rebuilt here."""
        from airflow.sdk.serde import serialize as serde_serialize

        original = {"asset": Asset("restricted", access_control=access_control)}
        ti = create_task_instance(
            task_id="test_next_kwargs_access_control",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = serde_serialize(original)
        session.commit()

        constructed = []
        for cls in (Asset, AssetAccessControl):
            original_init = cls.__init__

            def spy(self, *args, _original_init=original_init, **kwargs):
                constructed.append(type(self))
                _original_init(self, *args, **kwargs)

            monkeypatch.setattr(cls, "__init__", spy)

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert constructed == []
        assert response.json()["next_method"] == "execute_complete"
        next_kwargs = response.json()["next_kwargs"]
        assert next_kwargs == BaseSerialization.serialize(original)
        assert BaseSerialization.deserialize(next_kwargs)["asset"].access_control == expected_access_control

    @pytest.mark.parametrize(
        "assets",
        [
            pytest.param((Asset("first"), Asset("second")), id="tuple-of-assets"),
            pytest.param({Asset("first"), Asset("second")}, id="set-of-assets"),
            pytest.param({AssetAlias("resume-alias")}, id="set-with-alias"),
        ],
    )
    def test_old_version_translates_asset_collections_without_constructing_them(
        self, old_ver_client, session, create_task_instance, monkeypatch, assets
    ):
        """Tuples and sets of assets reach old workers in the legacy format without being rebuilt here."""
        from airflow.sdk.serde import serialize as serde_serialize

        original = {"assets": assets}
        expected = BaseSerialization.serialize(original)

        ti = create_task_instance(
            task_id="test_next_kwargs_asset_collections",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = serde_serialize(original)
        session.commit()

        constructed = []
        for cls in (Asset, AssetAlias):
            original_init = cls.__init__

            def spy(self, *args, _original_init=original_init, **kwargs):
                constructed.append(type(self))
                _original_init(self, *args, **kwargs)

            monkeypatch.setattr(cls, "__init__", spy)

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert constructed == []
        assert response.json()["next_method"] == "execute_complete"
        next_kwargs = response.json()["next_kwargs"]
        assert next_kwargs == expected
        # Decoded per item: core's decoder builds unhashable SerializedAssetAlias, so it cannot rebuild
        # a set of aliases; older workers decode into hashable Task SDK classes.
        decoded = [BaseSerialization.deserialize(item) for item in next_kwargs["__var"]["assets"]["__var"]]
        assert sorted((type(a).__name__, a.name) for a in decoded) == sorted(
            (f"Serialized{type(a).__name__}", a.name) for a in assets
        )

    @pytest.mark.parametrize(
        "original",
        [
            pytest.param(
                {"__type": "record", "__var": "resume", "asset": Asset("first")}, id="top-level-envelope-keys"
            ),
            pytest.param({"payload": {"__type": "record", "asset": Asset("first")}}, id="nested-type-key"),
            pytest.param(
                {"payload": {"__type": "dict", "__var": {"x": 1}, "asset": Asset("first")}},
                id="nested-envelope-keys",
            ),
        ],
    )
    def test_old_version_converts_dicts_with_legacy_keys(
        self, old_ver_client, session, create_task_instance, original
    ):
        """Dicts that merely contain ``__type``/``__var`` keys are user data, not legacy envelopes."""
        from airflow.sdk.serde import serialize as serde_serialize

        ti = create_task_instance(
            task_id="test_next_kwargs_legacy_keys",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = serde_serialize(original)
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert response.json()["next_method"] == "execute_complete"
        next_kwargs = response.json()["next_kwargs"]
        assert next_kwargs == BaseSerialization.serialize(original)
        assert BaseSerialization.deserialize(next_kwargs).keys() == original.keys()

    @pytest.mark.parametrize(
        "assets",
        [
            pytest.param(frozenset({Asset("first")}), id="frozenset"),
            pytest.param({frozenset({Asset("first")})}, id="frozenset-in-set"),
        ],
    )
    def test_old_version_fails_task_for_frozenset_of_assets(
        self, old_ver_client, session, create_task_instance, monkeypatch, assets
    ):
        """The legacy format has no frozenset, so a frozenset of assets resumes through a clear failure."""
        from airflow.sdk.serde import serialize as serde_serialize

        ti = create_task_instance(
            task_id="test_next_kwargs_frozenset_assets",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = serde_serialize({"assets": assets})
        session.commit()

        constructed = []
        original_init = Asset.__init__

        def spy(self, *args, **kwargs):
            constructed.append(True)
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(Asset, "__init__", spy)

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert constructed == []
        assert response.json()["next_method"] == "__fail__"
        error = BaseSerialization.deserialize(response.json()["next_kwargs"])["error"]
        assert "'builtins.frozenset'" in error

    @pytest.mark.parametrize("nested", [False, True], ids=["top-level", "nested"])
    def test_old_version_does_not_rebuild_classes_behind_legacy_envelopes(
        self, old_ver_client, session, create_task_instance, monkeypatch, nested
    ):
        """A class envelope inside a legacy dict envelope is never decoded in the API server."""
        payload = {
            "__type": "dict",
            "__var": {
                "__type": "dict",
                "__var": {},
                "__classname__": "airflow.sdk.definitions.asset.AssetAlias",
                "__version__": 0,
                "__data__": {"name": "hidden", "group": "asset"},
            },
        }
        ti = create_task_instance(
            task_id="test_next_kwargs_hidden_class",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = {"event": payload} if nested else payload
        session.commit()

        constructed = []
        for cls in (Asset, AssetAlias):
            original_init = cls.__init__

            def spy(self, *args, _original_init=original_init, **kwargs):
                constructed.append(type(self))
                _original_init(self, *args, **kwargs)

            monkeypatch.setattr(cls, "__init__", spy)

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert constructed == []
        assert response.json()["next_method"] == "__fail__"
        error = BaseSerialization.deserialize(response.json()["next_kwargs"])["error"]
        assert error.startswith("The deferred task cannot be resumed")

    def test_old_version_fails_task_for_unconvertible_values(
        self, old_ver_client, session, create_task_instance, monkeypatch
    ):
        """Values of other classes are not rebuilt; old workers get a trigger failure naming the type."""
        from airflow.sdk.serde import serialize as serde_serialize

        ti = create_task_instance(
            task_id="test_next_kwargs_unconvertible",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = serde_serialize({"payload": _ResumePayload(value=1), "event": TIMESTAMP})
        session.commit()

        constructed = []
        original_init = _ResumePayload.__init__

        def spy(self, *args, **kwargs):
            constructed.append(True)
            original_init(self, *args, **kwargs)

        monkeypatch.setattr(_ResumePayload, "__init__", spy)

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert constructed == []
        assert response.json()["next_method"] == "__fail__"
        error = BaseSerialization.deserialize(response.json()["next_kwargs"])["error"]
        assert f"{__name__}._ResumePayload" in error

    def test_old_version_passes_base_serialization_assets_through(
        self, old_ver_client, session, create_task_instance
    ):
        """Rolling upgrade: BaseSerialization-format assets already in the DB are returned as stored."""

        stored = BaseSerialization.serialize({"asset": Asset("resume-data")})
        ti = create_task_instance(
            task_id="test_next_kwargs_base_assets",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = stored
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert response.json()["next_method"] == "execute_complete"
        assert response.json()["next_kwargs"] == stored

    def test_old_version_handles_already_base_serialization_in_db(
        self, old_ver_client, session, create_task_instance
    ):
        """Rolling upgrade: DB still has BaseSerialization format from old handle_event_submit."""
        ti = create_task_instance(
            task_id="test_next_kwargs_already_base",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        # Pre-upgrade data: BaseSerialization format already in DB
        ti.next_kwargs = BaseSerialization.serialize({"cheesecake": True, "event": "payload"})
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        next_kwargs = response.json()["next_kwargs"]
        # Should still be parseable by old workers
        result = BaseSerialization.deserialize(next_kwargs)
        assert result == {"cheesecake": True, "event": "payload"}

    def test_old_version_handles_submit_failure_plain_dict(
        self, old_ver_client, session, create_task_instance
    ):
        """submit_failure and scheduler timeout write raw plain dicts -- converter must handle those too."""
        ti = create_task_instance(
            task_id="test_next_kwargs_failure",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "__fail__"
        # This is what submit_failure / scheduler timeout writes -- plain dict, no wrapping
        ti.next_kwargs = {"error": "Trigger timeout"}
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        next_kwargs = response.json()["next_kwargs"]
        result = BaseSerialization.deserialize(next_kwargs)
        assert result == {"error": "Trigger timeout"}

    def test_head_version_returns_raw_serde_format(self, client, session, create_task_instance):
        """Head API version returns next_kwargs as-is (SDK serde format)."""
        ti = create_task_instance(
            task_id="test_next_kwargs_head",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = {"cheesecake": True, "event": "payload"}
        session.commit()

        response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        # Head version gets the plain dict directly -- no BaseSerialization wrapping
        assert response.json()["next_kwargs"] == {"cheesecake": True, "event": "payload"}
