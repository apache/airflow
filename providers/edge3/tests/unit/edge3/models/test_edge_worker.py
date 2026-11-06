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

from typing import TYPE_CHECKING
from unittest import mock

import pytest
from sqlalchemy import delete, select

from airflow.providers.common.compat.sdk import Stats
from airflow.providers.edge3.models.edge_worker import (
    EdgeWorkerModel,
    EdgeWorkerState,
    _glob_to_like_pattern,
    add_worker_queues,
    change_maintenance_comment,
    exit_maintenance,
    get_registered_edge_hosts,
    remove_worker,
    remove_worker_queues,
    request_maintenance,
    request_shutdown,
    set_metrics,
    set_worker_concurrency,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

stats_reference = f"{Stats.__module__}.Stats"


def test_set_metrics():
    worker_name = "test_worker1"
    if AIRFLOW_V_3_3_PLUS:
        with mock.patch("airflow.sdk._shared.observability.metrics.stats._get_backend") as mock_get_backend:
            mock_backend = mock.MagicMock()
            mock_get_backend.return_value = mock_backend

            set_metrics(
                worker_name=worker_name,
                state=EdgeWorkerState.IDLE,
                jobs_active=0,
                concurrency=1,
                free_concurrency=1,
                queues=None,
                sysinfo={"status": 1},
            )

            metric_names = [call.args[0] for call in mock_backend.gauge.call_args_list]
    else:
        with mock.patch(f"{stats_reference}.gauge") as mock_gauge:
            set_metrics(
                worker_name=worker_name,
                state=EdgeWorkerState.IDLE,
                jobs_active=0,
                concurrency=1,
                free_concurrency=1,
                queues=None,
                sysinfo={"status": 1},
            )

            metric_names = [call.args[0] for call in mock_gauge.call_args_list]

    assert "edge_worker.status" in metric_names

    legacy_metric_name = f"edge_worker.status.{worker_name}"
    assert legacy_metric_name in metric_names


@pytest.mark.parametrize(
    ("glob", "expected"),
    [
        ("prod-*", "prod-%"),
        ("worker-?", "worker-_"),
        ("*gpu*", "%gpu%"),
        # Literal LIKE metacharacters are escaped so they are not treated as wildcards.
        ("50%_worker", "50\\%\\_worker"),
        ("back\\slash", "back\\\\slash"),
    ],
)
def test_glob_to_like_pattern(glob, expected):
    assert _glob_to_like_pattern(glob) == expected


@pytest.mark.db_test
class TestGetRegisteredEdgeHosts:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, session: Session):
        session.execute(delete(EdgeWorkerModel))
        queues_by_name = {
            "prod-worker-1": ["default", "gpu"],
            "prod-worker-2": ["default"],
            "dev-worker-1": ["gpu"],
        }
        for name, queues in queues_by_name.items():
            session.add(EdgeWorkerModel(worker_name=name, queues=queues, state=EdgeWorkerState.RUNNING))
        session.commit()

    def test_no_pattern_returns_all(self, session: Session):
        hosts = get_registered_edge_hosts(session=session)
        assert {h.worker_name for h in hosts} == {
            "prod-worker-1",
            "prod-worker-2",
            "dev-worker-1",
        }

    def test_star_glob_filters_by_prefix(self, session: Session):
        hosts = get_registered_edge_hosts(worker_name_pattern="prod-*", session=session)
        assert {h.worker_name for h in hosts} == {"prod-worker-1", "prod-worker-2"}

    def test_question_mark_glob_matches_single_char(self, session: Session):
        hosts = get_registered_edge_hosts(worker_name_pattern="prod-worker-?", session=session)
        assert {h.worker_name for h in hosts} == {"prod-worker-1", "prod-worker-2"}

    def test_no_match_returns_empty(self, session: Session):
        hosts = get_registered_edge_hosts(worker_name_pattern="nonexistent-*", session=session)
        assert list(hosts) == []

    def test_queues_filters_by_exact_membership(self, session: Session):
        hosts = get_registered_edge_hosts(queues=["gpu"], session=session)
        assert {h.worker_name for h in hosts} == {"prod-worker-1", "dev-worker-1"}

    def test_queues_matches_any_of_multiple(self, session: Session):
        hosts = get_registered_edge_hosts(queues=["gpu", "default"], session=session)
        assert {h.worker_name for h in hosts} == {"prod-worker-1", "prod-worker-2", "dev-worker-1"}

    def test_queues_no_match_returns_empty(self, session: Session):
        hosts = get_registered_edge_hosts(queues=["nonexistent"], session=session)
        assert list(hosts) == []

    def test_queues_combined_with_name_pattern(self, session: Session):
        hosts = get_registered_edge_hosts(worker_name_pattern="prod-*", queues=["gpu"], session=session)
        assert {h.worker_name for h in hosts} == {"prod-worker-1"}


class TestEdgeWorkerModelQueues:
    def test_queues_default_to_none(self):
        worker = EdgeWorkerModel(worker_name="worker-1", state=EdgeWorkerState.IDLE, queues=None)

        assert worker.queues is None

    def test_queues_round_trip_through_string_column(self):
        worker = EdgeWorkerModel(
            worker_name="worker-1", state=EdgeWorkerState.IDLE, queues=["default", "gpu"]
        )

        assert worker.queues == ["default", "gpu"]

    def test_add_queues_deduplicates(self):
        worker = EdgeWorkerModel(worker_name="worker-1", state=EdgeWorkerState.IDLE, queues=["default"])

        worker.add_queues(["gpu", "default"])

        assert worker.queues is not None
        assert sorted(worker.queues) == ["default", "gpu"]

    def test_remove_queues_ignores_absent_queue(self):
        worker = EdgeWorkerModel(
            worker_name="worker-1", state=EdgeWorkerState.IDLE, queues=["default", "gpu"]
        )

        worker.remove_queues(["gpu", "nonexistent"])

        assert worker.queues == ["default"]

    def test_update_state_coerces_string_to_enum(self):
        worker = EdgeWorkerModel(worker_name="worker-1", state=EdgeWorkerState.IDLE, queues=None)

        worker.update_state("maintenance mode")

        assert worker.state == EdgeWorkerState.MAINTENANCE_MODE


@pytest.mark.db_test
class TestWorkerLifecycleOperations:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, session: Session):
        session.execute(delete(EdgeWorkerModel))
        session.add(
            EdgeWorkerModel(worker_name="running-worker", state=EdgeWorkerState.RUNNING, queues=["default"])
        )
        session.add(EdgeWorkerModel(worker_name="offline-worker", state=EdgeWorkerState.OFFLINE, queues=None))
        session.commit()

    @staticmethod
    def _find_worker(session: Session, worker_name: str) -> EdgeWorkerModel | None:
        return session.scalar(select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name))

    @classmethod
    def _get_worker(cls, session: Session, worker_name: str) -> EdgeWorkerModel:
        worker = cls._find_worker(session, worker_name)
        assert worker is not None
        return worker

    def test_request_maintenance_sets_state_and_comment(self, session: Session):
        request_maintenance("running-worker", "planned upgrade", session=session)

        worker = self._get_worker(session, "running-worker")
        assert worker.state == EdgeWorkerState.MAINTENANCE_REQUEST
        assert worker.maintenance_comment == "planned upgrade"

    def test_exit_maintenance_sets_state_and_clears_comment(self, session: Session):
        request_maintenance("running-worker", "planned upgrade", session=session)

        exit_maintenance("running-worker", session=session)

        worker = self._get_worker(session, "running-worker")
        assert worker.state == EdgeWorkerState.MAINTENANCE_EXIT
        assert worker.maintenance_comment is None

    def test_change_maintenance_comment_in_maintenance_state(self, session: Session):
        request_maintenance("running-worker", "planned upgrade", session=session)

        change_maintenance_comment("running-worker", "upgrade extended", session=session)

        worker = self._get_worker(session, "running-worker")
        assert worker.maintenance_comment == "upgrade extended"

    def test_change_maintenance_comment_rejected_outside_maintenance(self, session: Session):
        with pytest.raises(TypeError, match="not in maintenance"):
            change_maintenance_comment("running-worker", "some comment", session=session)

    def test_request_shutdown_sets_state(self, session: Session):
        request_shutdown("running-worker", session=session)

        worker = self._get_worker(session, "running-worker")
        assert worker.state == EdgeWorkerState.SHUTDOWN_REQUEST

    def test_request_shutdown_keeps_offline_worker_untouched(self, session: Session):
        request_shutdown("offline-worker", session=session)

        worker = self._get_worker(session, "offline-worker")
        assert worker.state == EdgeWorkerState.OFFLINE

    def test_remove_worker_deletes_offline_worker(self, session: Session):
        remove_worker("offline-worker", session=session)

        assert self._find_worker(session, "offline-worker") is None

    def test_remove_worker_rejects_active_worker(self, session: Session):
        with pytest.raises(TypeError, match="Cannot remove edge worker"):
            remove_worker("running-worker", session=session)

    def test_add_worker_queues_extends_queues(self, session: Session):
        add_worker_queues("running-worker", ["gpu"], session=session)

        worker = self._get_worker(session, "running-worker")
        assert worker.queues is not None
        assert sorted(worker.queues) == ["default", "gpu"]

    def test_remove_worker_queues_removes_queue(self, session: Session):
        remove_worker_queues("running-worker", ["default"], session=session)

        worker = self._get_worker(session, "running-worker")
        assert worker.queues is None

    def test_set_worker_concurrency_updates_value(self, session: Session):
        set_worker_concurrency("running-worker", 8, session=session)

        worker = self._get_worker(session, "running-worker")
        assert worker.concurrency == 8

    @pytest.mark.parametrize(
        "operation",
        [
            lambda session: add_worker_queues("offline-worker", ["gpu"], session=session),
            lambda session: remove_worker_queues("offline-worker", ["gpu"], session=session),
            lambda session: set_worker_concurrency("offline-worker", 8, session=session),
        ],
    )
    def test_queue_and_concurrency_changes_rejected_for_offline_worker(self, operation, session: Session):
        with pytest.raises(TypeError):
            operation(session)

    @pytest.mark.parametrize(
        "operation",
        [
            lambda session: request_maintenance("ghost", "comment", session=session),
            lambda session: exit_maintenance("ghost", session=session),
            lambda session: change_maintenance_comment("ghost", "comment", session=session),
            lambda session: request_shutdown("ghost", session=session),
            lambda session: remove_worker("ghost", session=session),
            lambda session: add_worker_queues("ghost", ["gpu"], session=session),
            lambda session: remove_worker_queues("ghost", ["gpu"], session=session),
            lambda session: set_worker_concurrency("ghost", 8, session=session),
        ],
    )
    def test_unknown_worker_raises_value_error(self, operation, session: Session):
        with pytest.raises(ValueError, match="not found in list of registered workers"):
            operation(session)
