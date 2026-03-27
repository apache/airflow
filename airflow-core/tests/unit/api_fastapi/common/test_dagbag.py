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

from collections import OrderedDict
from unittest import mock
from uuid import uuid4

import pytest

from airflow.api_fastapi.app import purge_cached_app
from airflow.models.dagbag import DBDagBag
from airflow.sdk import BaseOperator

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


class TestDagBagSingleton:
    """Tests to ensure that DagBag is instantiated only once per app lifecycle."""

    dagbag_call_counter = {"count": 0}

    def setup(self):
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    def teardown(self):
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @pytest.fixture(autouse=True)
    def patch_dagbag_once_before_app(self):
        """Patch DagBag once before app is created, and reset counter."""
        self.dagbag_call_counter["count"] = 0

        from airflow.models.dagbag import DBDagBag as RealDagBag

        def factory(*args, **kwargs):
            self.dagbag_call_counter["count"] += 1
            return RealDagBag(*args, **kwargs)

        with mock.patch("airflow.api_fastapi.common.dagbag.DBDagBag", side_effect=factory):
            purge_cached_app()
            yield

    def test_dagbag_used_as_singleton_in_dependency(self, session, dag_maker, test_client):
        """
        Ensure DagBag is created only once and reused across multiple API requests.

        This test sets up a single DAG, patches the DagBag constructor to track instantiation count,
        and verifies that two calls to the `/api/v2/dags/{dag_id}` endpoint both return 200 OK,
        while only one DagBag instance is created.

        This validates that the FastAPI DagBag dependency correctly resolves to app.state.dag_bag,
        maintaining singleton behavior instead of creating a new DagBag per request.
        """
        dag_id = "dagbag_singleton_test"

        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            BaseOperator(task_id="test_task")
        session.commit()

        resp1 = test_client.get(f"/api/v2/dags/{dag_id}")
        assert resp1.status_code == 200

        resp2 = test_client.get(f"/api/v2/dags/{dag_id}")
        assert resp2.status_code == 200

        assert self.dagbag_call_counter["count"] == 1


class TestDBDagBagLRUCache:
    """Tests for the bounded LRU eviction behaviour of DBDagBag._dags."""

    def _make_bag(self, max_size: int) -> DBDagBag:
        bag = DBDagBag.__new__(DBDagBag)
        bag.load_op_links = True
        bag._max_dag_version_cache_size = max_size
        bag._dags = OrderedDict()
        return bag

    def _make_model(self, version_id):
        m = mock.MagicMock()
        m.dag_version_id = version_id
        m.dag = mock.MagicMock()  # truthy — deserialization succeeds
        return m

    def test_cache_bounded_by_max_size(self):
        """Inserting beyond max_size evicts the least-recently-used entry."""
        bag = self._make_bag(max_size=3)
        ids = [uuid4() for _ in range(4)]
        for uid in ids:
            bag._read_dag(self._make_model(uid))

        assert len(bag._dags) == 3
        assert ids[0] not in bag._dags  # first inserted → LRU → evicted
        assert ids[3] in bag._dags

    def test_cache_hit_promotes_to_mru(self):
        """A cache hit via get_serialized_dag_model promotes the entry to MRU."""
        bag = self._make_bag(max_size=3)
        ids = [uuid4() for _ in range(3)]
        models = {uid: self._make_model(uid) for uid in ids}
        for uid in ids:
            bag._read_dag(models[uid])

        # Re-access ids[0] through get_serialized_dag_model to promote it
        session = mock.MagicMock()
        bag.get_serialized_dag_model(ids[0], session=session)
        session.get.assert_not_called()  # should be a pure cache hit

        # Insert a 4th entry — ids[1] (now LRU) should be evicted, not ids[0]
        bag._read_dag(self._make_model(uuid4()))

        assert ids[0] in bag._dags  # promoted to MRU, survives
        assert ids[1] not in bag._dags  # was LRU after ids[0] promoted, evicted

    def test_failed_deserialization_not_cached(self):
        """Entries whose .dag property is falsy are not inserted into the cache."""
        bag = self._make_bag(max_size=10)
        m = mock.MagicMock()
        m.dag_version_id = uuid4()
        m.dag = None  # deserialization failure

        bag._read_dag(m)

        assert len(bag._dags) == 0
