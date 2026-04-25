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

import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest
import time_machine
from cachetools import LRUCache, TTLCache

from airflow.models.dagbag import DBDagBag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.serialization.serialized_objects import SerializedDAG

pytestmark = pytest.mark.db_test

# This file previously contained tests for DagBag functionality, but those tests
# have been moved to airflow-core/tests/unit/dag_processing/test_dagbag.py to match
# the source code reorganization where DagBag moved from models to dag_processing.
#
# Tests for models-specific functionality (DBDagBag, DagPriorityParsingRequest, etc.)
# remain in this file.


class TestDBDagBag:
    def setup_method(self):
        self.db_dag_bag = DBDagBag()
        self.session = MagicMock()

    def test__read_dag_stores_and_returns_dag(self):
        """It should store the (SerializedDAG, dag_hash) tuple in _dags and return the dag."""
        mock_dag = MagicMock(spec=SerializedDAG)
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_serdag.dag = mock_dag
        mock_serdag.dag_version_id = "v1"
        mock_serdag.dag_hash = "h1"

        result = self.db_dag_bag._read_dag(mock_serdag)

        assert result == mock_dag
        assert self.db_dag_bag._dags["v1"] == (mock_dag, "h1")
        assert mock_serdag.load_op_links is True

    def test__read_dag_returns_none_when_no_dag(self):
        """It should return None and not modify _dags when no DAG is present."""
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_serdag.dag = None
        mock_serdag.dag_version_id = "v1"

        result = self.db_dag_bag._read_dag(mock_serdag)

        assert result is None
        assert "v1" not in self.db_dag_bag._dags

    def test_get_dag_fetches_from_db_on_miss(self):
        """It should query the DB and cache the result when not in cache."""
        mock_dag = MagicMock(spec=SerializedDAG)
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_serdag.dag = mock_dag
        mock_serdag.dag_version_id = "v1"
        mock_dag_version = MagicMock()
        mock_dag_version.serialized_dag = mock_serdag
        self.session.get.return_value = mock_dag_version

        result = self.db_dag_bag.get_dag("v1", session=self.session)

        self.session.get.assert_called_once()
        assert result == mock_dag

    def test_get_dag_returns_cached_on_hit(self):
        """It should return cached DAG without re-loading the full row from DB.

        A cheap ``dag_hash`` lookup is still issued to detect in-place updates
        (issue #65696), but the heavy ``session.get(DagVersion, ...)`` is skipped.
        """
        mock_dag = MagicMock(spec=SerializedDAG)
        self.db_dag_bag._dags["v1"] = (mock_dag, "h1")
        self.session.scalar.return_value = "h1"

        result = self.db_dag_bag.get_dag("v1", session=self.session)

        assert result == mock_dag
        self.session.get.assert_not_called()

    def test_get_dag_returns_none_when_not_found(self):
        """It should return None if version_id not found in DB."""
        self.session.get.return_value = None

        result = self.db_dag_bag.get_dag("v1", session=self.session)

        assert result is None

    def test_get_dag_invalidates_cache_when_dag_hash_changes_in_place(self):
        """Regression test for issue #65696.

        ``SerializedDagModel.write_dag`` updates the serialized DAG in-place under the
        same ``dag_version_id`` when the version has no associated task instances. Long-lived
        ``DBDagBag`` instances (e.g. the scheduler's ``self.scheduler_dag_bag``) cache by
        ``dag_version_id`` and previously had no staleness check, so the scheduler kept
        returning the stale cached ``SerializedDAG`` until restart.

        The fix: cache the ``dag_hash`` alongside the deserialized DAG and compare against
        the current DB ``dag_hash`` on each cache lookup. A mismatch triggers re-deserialization.
        """
        from airflow.models.serialized_dag import SerializedDagModel

        # First read: cache the original DAG with its hash
        original_dag = MagicMock(spec=SerializedDAG)
        original_serdag = MagicMock(spec=SerializedDagModel)
        original_serdag.dag = original_dag
        original_serdag.dag_version_id = "v1"
        original_serdag.dag_hash = "hash_original"
        original_dag_version = MagicMock()
        original_dag_version.serialized_dag = original_serdag
        self.session.get.return_value = original_dag_version

        first = self.db_dag_bag.get_dag("v1", session=self.session)
        assert first is original_dag

        # Simulate write_dag in-place update: same dag_version_id, new content + hash
        updated_dag = MagicMock(spec=SerializedDAG)
        updated_serdag = MagicMock(spec=SerializedDagModel)
        updated_serdag.dag = updated_dag
        updated_serdag.dag_version_id = "v1"
        updated_serdag.dag_hash = "hash_updated"
        updated_dag_version = MagicMock()
        updated_dag_version.serialized_dag = updated_serdag

        # Configure session: scalar() returns the new hash; get() returns the new full row.
        self.session.scalar.return_value = "hash_updated"
        self.session.get.return_value = updated_dag_version

        second = self.db_dag_bag.get_dag("v1", session=self.session)

        # Should NOT be the stale cached original
        assert second is updated_dag, (
            "DBDagBag returned stale cached SerializedDAG after in-place update of dag_version "
            "(see issue #65696)"
        )


class TestDBDagBagCache:
    """Tests for DBDagBag optional caching behavior."""

    def test_no_caching_by_default(self):
        """Test that DBDagBag uses a simple dict without caching by default."""
        dag_bag = DBDagBag()
        assert dag_bag._use_cache is False
        assert isinstance(dag_bag._dags, dict)

    def test_lru_cache_enabled_with_cache_size(self):
        """Test that LRU cache is enabled when cache_size is provided."""
        dag_bag = DBDagBag(cache_size=10)
        assert dag_bag._use_cache is True
        assert isinstance(dag_bag._dags, LRUCache)

    def test_ttl_cache_enabled_with_cache_size_and_ttl(self):
        """Test that TTL cache is enabled when both cache_size and cache_ttl are provided."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)
        assert dag_bag._use_cache is True
        assert isinstance(dag_bag._dags, TTLCache)

    def test_zero_cache_size_uses_unbounded_dict(self):
        """Test that cache_size=0 uses unbounded dict (same as no caching)."""
        dag_bag = DBDagBag(cache_size=0, cache_ttl=60)
        assert dag_bag._use_cache is False
        assert isinstance(dag_bag._dags, dict)

    def test_clear_cache_with_caching(self):
        """Test clear_cache() with caching enabled."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)

        mock_dag = MagicMock()
        dag_bag._dags["version_1"] = mock_dag
        dag_bag._dags["version_2"] = mock_dag
        assert len(dag_bag._dags) == 2

        count = dag_bag.clear_cache()
        assert count == 2
        assert len(dag_bag._dags) == 0

    def test_clear_cache_without_caching(self):
        """Test clear_cache() without caching enabled."""
        dag_bag = DBDagBag()

        mock_dag = MagicMock()
        dag_bag._dags["version_1"] = mock_dag
        assert len(dag_bag._dags) == 1

        count = dag_bag.clear_cache()
        assert count == 1
        assert len(dag_bag._dags) == 0

    def test_ttl_cache_expiry(self):
        """Test that cached DAGs expire after TTL."""
        # TTLCache defaults to time.monotonic which time_machine cannot control.
        # Use time.time as the timer so time_machine can advance it.
        dag_bag = DBDagBag(cache_size=10, cache_ttl=1)
        dag_bag._dags = TTLCache(maxsize=10, ttl=1, timer=time.time)

        with time_machine.travel("2025-01-01 00:00:00", tick=False):
            dag_bag._dags["test_version_id"] = MagicMock()
            assert "test_version_id" in dag_bag._dags

        # Jump ahead beyond TTL
        with time_machine.travel("2025-01-01 00:00:02", tick=False):
            assert dag_bag._dags.get("test_version_id") is None

    def test_lru_eviction(self):
        """Test that LRU eviction works when cache is full."""
        dag_bag = DBDagBag(cache_size=2)

        dag_bag._dags["version_1"] = MagicMock()
        dag_bag._dags["version_2"] = MagicMock()
        dag_bag._dags["version_3"] = MagicMock()

        # version_1 should be evicted (LRU)
        assert dag_bag._dags.get("version_1") is None
        assert dag_bag._dags.get("version_2") is not None
        assert dag_bag._dags.get("version_3") is not None

    def test_thread_safety_with_caching(self):
        """Test concurrent access doesn't cause race conditions with caching enabled."""
        dag_bag = DBDagBag(cache_size=100, cache_ttl=60)
        errors = []
        mock_session = MagicMock()

        def make_dag_version(version_id):
            serdag = MagicMock()
            serdag.dag = MagicMock()
            serdag.dag_version_id = version_id
            return MagicMock(serialized_dag=serdag)

        def get_dag_version(model, version_id, options=None):
            return make_dag_version(version_id)

        mock_session.get.side_effect = get_dag_version

        def access_cache(i):
            try:
                dag_bag._get_dag(f"version_{i % 5}", mock_session)
            except Exception as e:
                errors.append(e)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(access_cache, i) for i in range(100)]
            for f in futures:
                f.result()

        assert not errors

    def test_read_dag_stores_in_bounded_cache(self):
        """Test that _read_dag stores DAG in bounded cache when cache_size > 0."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)

        mock_sdm = MagicMock()
        mock_sdm.dag = MagicMock()
        mock_sdm.dag_version_id = "test_version"

        result = dag_bag._read_dag(mock_sdm)

        assert result == mock_sdm.dag
        assert "test_version" in dag_bag._dags

    def test_read_dag_stores_in_unbounded_dict(self):
        """Test that _read_dag stores DAG in unbounded dict when no cache_size."""
        dag_bag = DBDagBag()

        mock_sdm = MagicMock()
        mock_sdm.dag = MagicMock()
        mock_sdm.dag_version_id = "test_version"

        result = dag_bag._read_dag(mock_sdm)

        assert result == mock_sdm.dag
        assert "test_version" in dag_bag._dags

    def test_iter_all_latest_version_dags_does_not_cache(self):
        """Test that iter_all_latest_version_dags does not cache to prevent thrashing."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)

        mock_session = MagicMock()
        mock_sdm = MagicMock()
        mock_sdm.dag = MagicMock()
        mock_sdm.dag_version_id = "test_version"
        mock_session.scalars.return_value = [mock_sdm]

        list(dag_bag.iter_all_latest_version_dags(session=mock_session))

        # Cache should be empty -- iter doesn't cache to prevent thrashing
        assert len(dag_bag._dags) == 0

    @patch("airflow.models.dagbag.Stats")
    def test_cache_hit_metric_emitted(self, mock_stats):
        """Test that cache hit metric is emitted when caching is enabled."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)
        mock_session = MagicMock()
        mock_dag = MagicMock()
        dag_bag._dags["test_version"] = (mock_dag, "h")
        # Hash matches so the cache entry is considered fresh.
        mock_session.scalar.return_value = "h"

        dag_bag._get_dag("test_version", mock_session)

        mock_stats.incr.assert_called_with("api_server.dag_bag.cache_hit")

    @patch("airflow.models.dagbag.Stats")
    def test_cache_miss_metric_emitted(self, mock_stats):
        """Test that cache miss metric is emitted when DAG is found in DB but not in cache."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)
        mock_session = MagicMock()

        # Set up a DB result so _get_dag reaches the miss metric path
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_serdag.dag = MagicMock(spec=SerializedDAG)
        mock_serdag.dag_version_id = "uncached_version"
        mock_dag_version = MagicMock()
        mock_dag_version.serialized_dag = mock_serdag
        mock_session.get.return_value = mock_dag_version

        dag_bag._get_dag("uncached_version", mock_session)

        mock_stats.incr.assert_any_call("api_server.dag_bag.cache_miss")

    @patch("airflow.models.dagbag.Stats")
    def test_cache_clear_metric_emitted(self, mock_stats):
        """Test that cache clear metric is emitted when caching is enabled."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)
        dag_bag._dags["test_version"] = MagicMock()

        dag_bag.clear_cache()

        mock_stats.incr.assert_called_with("api_server.dag_bag.cache_clear")

    @patch("airflow.models.dagbag.Stats")
    def test_cache_size_gauge_emitted(self, mock_stats):
        """Test that cache size gauge is emitted when a DAG is cached."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)
        mock_serdag = MagicMock()
        mock_serdag.dag_version_id = "test_version_1"
        mock_serdag.dag = MagicMock()
        mock_serdag.load_op_links = True

        dag_bag._read_dag(mock_serdag)

        mock_stats.gauge.assert_called_with("api_server.dag_bag.cache_size", 1, rate=0.1)
