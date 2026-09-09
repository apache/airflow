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

import copy
import datetime
import json
from datetime import timedelta
from typing import Any
from unittest import mock

import pytest
from sqlalchemy import delete, func, select, update

from airflow._shared.timezones import timezone
from airflow.exceptions import DagVersionNotFound
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagcode import DagCode
from airflow.models.dagrun import DagRun
from airflow.models.deadline_alert import DeadlineAlert as DeadlineAlertModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference
from airflow.serialization.dag_version_diff import MAX_ALLOWED_CHANGES
from airflow.serialization.encoders import encode_deadline_alert
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


async def _handle_deadline(context, **kwargs):
    pass


class TestDagVersion:
    def setup_method(self):
        clear_db_dags()

    def teardown_method(self):
        # clear_db_dags() first: DagModel.bundle_name has an FK to dag_bundle.
        clear_db_dags()
        clear_db_dag_bundles()

    @pytest.mark.need_serialized_dag
    def test_writing_dag_version(self, dag_maker, session):
        with dag_maker("test_writing_dag_version") as dag:
            pass

        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version_number == 1
        assert latest_version.dag_id == dag.dag_id

    @pytest.mark.need_serialized_dag
    def test_get_version_treats_zero_as_a_real_filter(self, dag_maker, session):
        """version_number=0 must filter (and find nothing), not fall through to 'latest'."""
        with dag_maker("zero_guard_dag"):
            EmptyOperator(task_id="task1")

        assert DagVersion.get_version("zero_guard_dag", 0, session=session) is None
        # version_number=None still returns the latest version.
        assert DagVersion.get_version("zero_guard_dag", session=session).version_number == 1

    def test_writing_dag_version_with_changes(self, dag_maker, session):
        """This also tested the get_latest_version method"""
        with dag_maker("test1") as dag:
            EmptyOperator(task_id="task1")
        sync_dag_to_db(dag)
        dag_maker.create_dagrun()
        # Add extra task to change the dag
        with dag_maker("test1") as dag2:
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")
        sync_dag_to_db(dag2)
        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version_number == 2
        assert session.scalar(select(func.count()).where(DagVersion.dag_id == dag.dag_id)) == 2

    @staticmethod
    def _seed_two_versions_with_inverted_created_at(session, *, dag_id):
        """Create versions 1 and 2 where version 2 has an *earlier* created_at than version 1.

        This makes created_at ordering disagree with version_number ordering, modelling the
        timestamp tie / clock-skew case the ordering must be robust to. Returns the bundle name.
        """
        bundle_name = f"bundle-{dag_id}"
        session.add(DagBundleModel(name=bundle_name))
        session.flush()
        session.add(DagModel(dag_id=dag_id, bundle_name=bundle_name))
        session.flush()

        base = timezone.utcnow()
        for version_number, created_at in ((1, base), (2, base - timedelta(minutes=1))):
            session.add(
                DagVersion(
                    dag_id=dag_id,
                    version_number=version_number,
                    bundle_name=bundle_name,
                    created_at=created_at,
                    last_updated=created_at,
                )
            )
        session.commit()
        return bundle_name

    def test_latest_version_uses_version_number_not_created_at(self, session):
        """The latest version is the one with the highest version_number, not the latest created_at."""
        dag_id = "test_latest_ordering"
        self._seed_two_versions_with_inverted_created_at(session, dag_id=dag_id)

        assert DagVersion.get_latest_version(dag_id, session=session).version_number == 2
        assert DagVersion.get_version(dag_id, session=session).version_number == 2

    def test_write_dag_increments_from_max_version_number(self, session):
        """write_dag must increment from the max version_number, not the latest-created row.

        Otherwise, when created_at ordering disagrees with version_number ordering, it would
        recompute an already-used version_number and violate the (dag_id, version_number) unique
        constraint.
        """
        dag_id = "test_write_dag_increment"
        bundle_name = self._seed_two_versions_with_inverted_created_at(session, dag_id=dag_id)

        new_version = DagVersion.write_dag(dag_id=dag_id, bundle_name=bundle_name, session=session)
        session.commit()

        assert new_version.version_number == 3
        assert session.scalar(select(func.count()).where(DagVersion.dag_id == dag_id)) == 3

    @pytest.mark.need_serialized_dag
    def test_get_version(self, dag_maker, session):
        """The two dags have the same version name and number but different dag ids"""
        dag1_id = "test1"
        with dag_maker(dag1_id):
            EmptyOperator(task_id="task1")

        with dag_maker("test2"):
            EmptyOperator(task_id="task1")

        with dag_maker("test3"):
            EmptyOperator(task_id="task1")

        version = DagVersion.get_version(dag1_id)
        assert version.version_number == 1
        assert version.dag_id == dag1_id
        assert version.version == f"{dag1_id}-1"

    @pytest.mark.need_serialized_dag
    def test_version_property(self, dag_maker):
        with dag_maker("test1") as dag:
            pass

        latest_version = DagVersion.get_latest_version(dag.dag_id)
        assert latest_version.version == f"{dag.dag_id}-1"

    @pytest.mark.db_test
    def test_write_dag_with_version_data(self, dag_maker, session):
        """Test that version_data is stored and retrievable."""
        with dag_maker("test_version_data"):
            pass

        manifest = {"schema_version": 1, "files": {"dags/my_dag.py": "S3VersionId123"}}
        DagVersion.write_dag(
            dag_id="test_version_data",
            bundle_name="testing",
            bundle_version="sha256abc",
            version_data=manifest,
            session=session,
        )
        session.flush()

        retrieved = DagVersion.get_latest_version("test_version_data", session=session)
        assert retrieved.version_data == manifest
        assert retrieved.bundle_version == "sha256abc"

    @pytest.mark.db_test
    def test_write_dag_without_version_data(self, dag_maker, session):
        """Test that version_data defaults to None for bundles that don't use it."""
        with dag_maker("test_no_version_data"):
            pass

        DagVersion.write_dag(
            dag_id="test_no_version_data",
            bundle_name="testing",
            bundle_version="abc123",
            session=session,
        )
        session.flush()

        retrieved = DagVersion.get_latest_version("test_no_version_data", session=session)
        assert retrieved.version_data is None
        assert retrieved.bundle_version == "abc123"

    @pytest.mark.parametrize(
        ("view_url_kwargs", "expected"),
        [
            pytest.param(
                {"return_value": "https://example.com/tree/abc"},
                "https://example.com/tree/abc",
                id="bundle-still-configured",
            ),
            pytest.param(
                {"side_effect": ValueError("Bundle not configured")},
                None,
                id="bundle-no-longer-configured",
            ),
        ],
    )
    @mock.patch("airflow.models.dag_version.DagBundlesManager", autospec=True)
    def test_bundle_url_falls_back_to_manager_without_a_bundle_row(
        self, mock_manager, view_url_kwargs, expected
    ):
        """Without a dag_bundle row the deprecated manager lookup is the only path left."""
        mock_manager.return_value.view_url.configure_mock(**view_url_kwargs)
        # Never persisted, so ``bundle`` resolves to None -- the same state a Dag version
        # whose bundle row is missing ends up in.
        dag_version = DagVersion(
            dag_id="dag_without_bundle_row", bundle_name="removed-bundle", bundle_version="abc"
        )

        assert dag_version.bundle_url == expected
        mock_manager.return_value.view_url.assert_called_once_with("removed-bundle", "abc")


class TestResolveVersionData:
    """Unit tests for the _resolve_version_data pin-guard helper."""

    @pytest.mark.parametrize(
        ("dag_version", "bundle_version", "expected"),
        [
            pytest.param(
                mock.Mock(version_data={"schema_version": 1}),
                "abc123",
                {"schema_version": 1},
                id="pinned-with-data",
            ),
            pytest.param(
                mock.Mock(version_data={"schema_version": 1}),
                None,
                None,
                id="unpinned-suppresses-present-data",
            ),
            pytest.param(None, "abc123", None, id="missing-dag-version"),
            pytest.param(None, None, None, id="unpinned-and-missing"),
        ],
    )
    def test_resolve_version_data(self, dag_version, bundle_version, expected):
        from airflow.models.dag_version import _resolve_version_data

        assert _resolve_version_data(dag_version, bundle_version) == expected


class TestDagVersionGetDiff:
    @pytest.fixture
    def dag_id(self, dag_maker, session):
        clear_db_dags()
        clear_db_serialized_dags()
        dag_id = "version_diff_dag"
        for version_number in range(1, 3):
            with dag_maker(dag_id, session=session, bundle_version=f"commit{version_number}"):
                for task_number in range(version_number):
                    EmptyOperator(task_id=f"task{task_number + 1}")
            dag_maker.create_dagrun(
                run_id=f"run{version_number}",
                logical_date=datetime.datetime(2020, 1, version_number, tzinfo=datetime.timezone.utc),
                session=session,
            )
            session.commit()
        # Read versions back from the DB the way the API and CLI callers do, so
        # serialized payloads carry plain JSON keys rather than in-memory Encoding enums.
        session.expunge_all()
        return dag_id

    @pytest.fixture
    def create_deadline_versions(self, session):
        dag_id = "version_diff_deadlines"
        bundle_name = "version_diff_deadlines"

        def create_versions(*, target_interval=timedelta(minutes=5), target_callback_kwargs=None):
            definitions = []
            for version_number in (1, 2):
                deadline = DeadlineAlert(
                    name="completion",
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=timedelta(minutes=5) if version_number == 1 else target_interval,
                    callback=AsyncCallback(
                        _handle_deadline,
                        kwargs=(
                            target_callback_kwargs
                            if version_number == 2 and target_callback_kwargs is not None
                            else {"message": "original"}
                        ),
                    ),
                )
                definitions.append(encode_deadline_alert(deadline))
                with DAG(dag_id, deadline=deadline) as dag:
                    for task_number in range(version_number):
                        EmptyOperator(task_id=f"task{task_number + 1}")
                scheduler_dag = sync_dag_to_db(dag, bundle_name=bundle_name, session=session)
                logical_date = datetime.datetime(2020, 1, version_number, tzinfo=datetime.timezone.utc)
                scheduler_dag.create_dagrun(
                    run_id=f"run{version_number}",
                    run_after=logical_date,
                    state=DagRunState.QUEUED,
                    logical_date=logical_date,
                    data_interval=(logical_date, logical_date),
                    triggered_by=DagRunTriggeredByType.TEST,
                    run_type=DagRunType.MANUAL,
                    session=session,
                )
                session.commit()
            session.expunge_all()
            return dag_id, definitions

        yield create_versions

        session.rollback()
        session.execute(delete(DagRun).where(DagRun.dag_id == dag_id))
        session.execute(delete(DagModel).where(DagModel.dag_id == dag_id))
        session.execute(delete(DagBundleModel).where(DagBundleModel.name == bundle_name))
        session.commit()

    def test_reports_observed_changes_between_versions(self, dag_id, session):
        result = DagVersion.get_diff(dag_id, 1, 2, session=session)

        assert result["mode"] == "observed_state"
        paths = {change["path"]: change for change in result["changes"]}
        assert paths["/dag/tasks/*"]["operation"] == "added"
        assert "after_digest" not in paths["/dag/tasks/*"]
        assert paths["/provenance/bundle_version"]["category"] == "provenance"
        # A caller that states no authorization gets the structural diff and nothing else,
        # with both sections present so their status is always readable.
        assert result["source"] == {"status": "unavailable", "fidelity": "unavailable"}
        assert result["values"] == {"status": "unavailable"}

    def test_raises_for_missing_version(self, dag_id, session):
        with pytest.raises(DagVersionNotFound, match="version_number: `3`"):
            DagVersion.get_diff(dag_id, 1, 3, session=session)

    @pytest.mark.parametrize("version_numbers", [(0, 2), (1, 0)])
    def test_rejects_non_positive_version_numbers(self, dag_id, session, version_numbers):
        with pytest.raises(ValueError, match="Dag version numbers must be positive integers"):
            DagVersion.get_diff(dag_id, *version_numbers, session=session)

    @pytest.mark.parametrize(
        ("max_changes", "expected_message"),
        [
            (0, "max_changes must be a positive integer"),
            (MAX_ALLOWED_CHANGES + 1, f"max_changes must not exceed {MAX_ALLOWED_CHANGES}"),
        ],
    )
    def test_rejects_out_of_range_change_bound_before_querying(
        self, dag_id, session, max_changes, expected_message
    ):
        with assert_queries_count(0):
            with pytest.raises(ValueError, match=expected_message):
                DagVersion.get_diff(dag_id, 1, 2, max_changes=max_changes, session=session)

    def test_marks_values_available_when_allowed(self, dag_id, session):
        result = DagVersion.get_diff(dag_id, 1, 2, values_status="available", session=session)

        assert result["values"] == {"status": "available"}
        assert any("after_value" in change for change in result["changes"])
        assert any(change["path"] == "/dag/tasks/task2" for change in result["changes"])

    def test_marks_values_unavailable_when_status_denied(self, dag_id, session):
        result = DagVersion.get_diff(dag_id, 1, 2, values_status="unavailable", session=session)

        assert result["mode"] == "observed_state"
        assert any(change["path"] == "/dag/tasks/*" for change in result["changes"])
        assert all(
            "before_digest" not in change
            and "after_digest" not in change
            and "before_value" not in change
            and "after_value" not in change
            for change in result["changes"]
        )
        assert result["values"] == {"status": "unavailable"}

    @pytest.mark.parametrize("version_number", [1, 2])
    def test_compares_json_string_serialized_data(self, dag_id, session, version_number):
        expected = DagVersion.get_diff(dag_id, 1, 2, values_status="available", session=session)
        serialized_dag = DagVersion.get_version(dag_id, version_number, session=session).serialized_dag
        session.execute(
            update(SerializedDagModel)
            .where(SerializedDagModel.id == serialized_dag.id)
            .values(_data=json.dumps(serialized_dag.data), _data_compressed=None)
        )
        session.commit()
        session.expunge_all()

        stored_data = DagVersion.get_version(dag_id, version_number, session=session).serialized_dag.data
        assert isinstance(stored_data, str)
        assert DagVersion.get_diff(dag_id, 1, 2, values_status="available", session=session) == expected

    def test_marks_diff_unavailable_for_invalid_serialized_json(self, dag_id, session):
        base = DagVersion.get_version(dag_id, 1, session=session).serialized_dag
        session.execute(
            update(SerializedDagModel)
            .where(SerializedDagModel.id == base.id)
            .values(_data="invalid JSON", _data_compressed=None)
        )
        session.commit()
        session.expunge_all()

        result = DagVersion.get_diff(dag_id, 1, 2, values_status="available", session=session)

        assert result["mode"] == "unavailable"
        assert result["unavailable_reason"] == "serialized_dag_canonicalization_failed"
        assert result["changes"] == []
        assert result["values"] == {"status": "unavailable"}

    @pytest.mark.parametrize("values_status", ["unavailable", "available"])
    @pytest.mark.parametrize("changed_field", [None, "interval", "callback"])
    def test_compares_stored_deadline_definitions(
        self, create_deadline_versions, session, values_status, changed_field
    ):
        dag_id, definitions = create_deadline_versions(
            target_interval=timedelta(minutes=10 if changed_field == "interval" else 5),
            target_callback_kwargs={"message": "changed"} if changed_field == "callback" else None,
        )
        versions = [DagVersion.get_version(dag_id, number, session=session) for number in (1, 2)]
        stored_payloads = [copy.deepcopy(version.serialized_dag.data) for version in versions]
        assert stored_payloads[0]["dag"]["deadline"] != stored_payloads[1]["dag"]["deadline"]
        assert all(isinstance(payload["dag"]["deadline"][0], str) for payload in stored_payloads)

        with assert_queries_count(2):
            result = DagVersion.get_diff(dag_id, 1, 2, values_status=values_status, session=session)

        assert result["mode"] == "observed_state"
        deadline_changes = [change for change in result["changes"] if change["path"] == "/dag/deadline"]
        if changed_field is None:
            assert deadline_changes == []
        else:
            assert len(deadline_changes) == 1
            change = deadline_changes[0]
            assert change["category"] == "deadline"
            assert change["impact"] == "execution"
            if values_status == "available":
                assert change["before_value"] == [definitions[0]]
                assert change["after_value"] == [definitions[1]]
        assert [version.serialized_dag.data for version in versions] == stored_payloads

    def test_compares_inline_and_referenced_deadline_definitions(self, create_deadline_versions, session):
        dag_id, definitions = create_deadline_versions()
        base = DagVersion.get_version(dag_id, 1, session=session).serialized_dag
        inline_data = copy.deepcopy(base.data)
        inline_data["dag"]["deadline"] = [definitions[0]]
        session.execute(
            update(SerializedDagModel)
            .where(SerializedDagModel.id == base.id)
            .values(_data=inline_data, _data_compressed=None)
        )
        session.commit()
        session.expunge_all()

        result = DagVersion.get_diff(dag_id, 1, 2, values_status="available", session=session)

        assert result["mode"] == "observed_state"
        assert not any(change["path"] == "/dag/deadline" for change in result["changes"])

    @pytest.mark.parametrize("missing_version", [1, 2])
    @pytest.mark.parametrize("same_version", [False, True])
    def test_marks_diff_unavailable_when_deadline_missing(
        self, create_deadline_versions, session, missing_version, same_version
    ):
        dag_id, _ = create_deadline_versions()
        serialized_dag = DagVersion.get_version(dag_id, missing_version, session=session).serialized_dag
        session.execute(
            delete(DeadlineAlertModel).where(DeadlineAlertModel.serialized_dag_id == serialized_dag.id)
        )
        session.commit()
        session.expunge_all()
        version_numbers = (missing_version, missing_version) if same_version else (1, 2)

        result = DagVersion.get_diff(dag_id, *version_numbers, values_status="available", session=session)

        assert result["mode"] == "unavailable"
        assert result["unavailable_reason"] == "deadline_alert_missing"
        assert result["serialized_dag_schema_versions"] == {"base": 3, "target": 3}
        assert result["changes"] == []
        assert result["truncated"] is False
        assert result["values"] == {"status": "unavailable"}

    @pytest.mark.parametrize("target_source", ["base source", "changed source"])
    def test_includes_current_stored_source(self, dag_id, session, target_source):
        DagVersion.get_version(dag_id, 1, session=session).dag_code.source_code = "base source"
        DagVersion.get_version(dag_id, 2, session=session).dag_code.source_code = target_source
        session.flush()
        result = DagVersion.get_diff(dag_id, 1, 2, source_status="current_stored_code", session=session)

        source = result["source"]
        assert source["status"] == "current_stored_code"
        assert source["changed"] is (target_source != "base source")
        assert source["base"]["digest"].startswith("sha256:")
        assert source["base"]["content"] == "base source"
        assert source["target"]["content"] == target_source
        assert (source["base"]["digest"] == source["target"]["digest"]) is (target_source == "base source")

    @pytest.mark.parametrize("source_status", ["redacted", "unavailable"])
    def test_hides_source_when_status_denied(self, dag_id, session, source_status):
        result = DagVersion.get_diff(dag_id, 1, 2, source_status=source_status, session=session)

        assert result["source"] == {"status": source_status, "fidelity": source_status}

    @pytest.mark.parametrize(
        ("status_name", "invalid_status"),
        [("source_status", ""), ("values_status", "redacted")],
    )
    def test_rejects_invalid_authorization_status(self, dag_id, session, status_name, invalid_status):
        statuses: dict[str, Any] = {
            "source_status": "current_stored_code",
            "values_status": "available",
            status_name: invalid_status,
        }

        with pytest.raises(ValueError, match=rf"{status_name} must be one of"):
            DagVersion.get_diff(dag_id, 1, 2, session=session, **statuses)

    def test_marks_source_unavailable_when_code_missing(self, dag_id, session):
        session.execute(delete(DagCode).where(DagCode.dag_id == dag_id))
        session.commit()
        session.expunge_all()

        result = DagVersion.get_diff(dag_id, 1, 2, source_status="current_stored_code", session=session)

        assert result["source"] == {"status": "unavailable", "fidelity": "unavailable"}
