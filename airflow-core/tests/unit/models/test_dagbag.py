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

from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DBDagBag, _CacheEntry
from airflow.models.dagbundle import DagBundleModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedDAG
from airflow.utils.session import create_session

from tests_common.test_utils import db

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
        """It should store the SerializedDAG with its hash, and return it."""
        mock_dag = MagicMock(spec=SerializedDAG)
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_serdag.dag = mock_dag
        mock_serdag.dag_version_id = "v1"
        mock_serdag.dag_hash = "hash1"

        result = self.db_dag_bag._read_dag(mock_serdag)

        assert result == mock_dag
        entry = self.db_dag_bag._dags["v1"]
        assert (entry.dag, entry.dag_hash) == (mock_dag, "hash1")
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
        """It should query the DB and cache the result (with its hash) when not in cache."""
        mock_dag = MagicMock(spec=SerializedDAG)
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_serdag.dag = mock_dag
        mock_serdag.dag_version_id = "v1"
        mock_serdag.dag_hash = "hash1"
        mock_dag_version = MagicMock()
        mock_dag_version.serialized_dag = mock_serdag
        self.session.get.return_value = mock_dag_version

        result = self.db_dag_bag.get_dag("v1", session=self.session)

        self.session.get.assert_called_once()
        assert result == mock_dag
        entry = self.db_dag_bag._dags["v1"]
        assert (entry.dag, entry.dag_hash) == (mock_dag, "hash1")

    def test_get_dag_serves_within_revalidation_window_without_query(self):
        """A recently validated entry is served straight from cache with no DB query at all."""
        mock_dag = MagicMock(spec=SerializedDAG)
        # Just-validated entry, well within the (default 30s) revalidation window.
        self.db_dag_bag._dags["v1"] = _CacheEntry(mock_dag, "hash1", time.monotonic())

        result = self.db_dag_bag.get_dag("v1", session=self.session)

        assert result == mock_dag
        self.session.scalar.assert_not_called()  # no revalidation query inside the window
        self.session.get.assert_not_called()

    def test_get_dag_serves_stale_within_window_even_if_db_changed(self):
        """Inside the window the cached copy is served even if the DB has since changed.

        This is the intended throttle tradeoff: staleness is bounded by the window, not zero.
        """
        stale_dag = MagicMock(spec=SerializedDAG)
        self.db_dag_bag._dags["v1"] = _CacheEntry(stale_dag, "old_hash", time.monotonic())
        # The DB has a different hash now, but we are inside the window so it is never consulted.
        self.session.scalar.return_value = "new_hash"

        result = self.db_dag_bag.get_dag("v1", session=self.session)

        assert result == stale_dag
        self.session.scalar.assert_not_called()
        self.session.get.assert_not_called()

    def test_get_dag_revalidates_after_window_and_serves_when_hash_matches(self):
        """Past the window, a hit is revalidated by hash and served (window restarted)."""
        mock_dag = MagicMock(spec=SerializedDAG)
        # last_validated=0.0 is far in the past, so the entry is revalidated.
        self.db_dag_bag._dags["v1"] = _CacheEntry(mock_dag, "hash1", 0.0)
        self.session.scalar.return_value = "hash1"

        result = self.db_dag_bag.get_dag("v1", session=self.session)

        assert result == mock_dag
        # Validated via a cheap scalar() lookup, not a full DagVersion load.
        self.session.scalar.assert_called_once()
        self.session.get.assert_not_called()
        # The window is restarted so the next hit can skip the query.
        assert self.db_dag_bag._dags["v1"].last_validated > 0.0

    def test_get_dag_reloads_when_version_updated_in_place(self):
        """A version updated in place (same id, new hash) must be reloaded, not served stale."""
        stale_dag = MagicMock(spec=SerializedDAG)
        fresh_dag = MagicMock(spec=SerializedDAG)
        # last_validated=0.0 forces revalidation regardless of the window.
        self.db_dag_bag._dags["v1"] = _CacheEntry(stale_dag, "old_hash", 0.0)
        mock_serdag = MagicMock(spec=SerializedDagModel)
        mock_serdag.dag = fresh_dag
        mock_serdag.dag_version_id = "v1"
        mock_serdag.dag_hash = "new_hash"
        mock_dag_version = MagicMock()
        mock_dag_version.serialized_dag = mock_serdag
        self.session.get.return_value = mock_dag_version
        # The dag_hash validation lookup returns the new hash (mismatch -> reload).
        self.session.scalar.return_value = "new_hash"

        result = self.db_dag_bag.get_dag("v1", session=self.session)

        assert result == fresh_dag
        self.session.get.assert_called_once()
        entry = self.db_dag_bag._dags["v1"]
        assert (entry.dag, entry.dag_hash) == (fresh_dag, "new_hash")

    def test_get_dag_reloads_when_cached_version_deleted(self):
        """A cached entry whose serialized row no longer exists must not be served."""
        stale_dag = MagicMock(spec=SerializedDAG)
        self.db_dag_bag._dags["v1"] = _CacheEntry(stale_dag, "old_hash", 0.0)
        self.session.scalar.return_value = None  # validation finds no row
        self.session.get.return_value = None  # version is gone

        result = self.db_dag_bag.get_dag("v1", session=self.session)

        assert result is None
        assert "v1" not in self.db_dag_bag._dags

    def test_get_dag_returns_none_when_not_found(self):
        """It should return None if version_id not found in DB."""
        self.session.get.return_value = None

        result = self.db_dag_bag.get_dag("v1", session=self.session)

        assert result is None

    def test_get_dag_reflects_in_place_version_update_end_to_end(self):
        """End-to-end regression: an in-place version update must be re-read, not served stale.

        When a DagVersion has no task instances, ``SerializedDagModel.write_dag`` updates the
        serialized DAG in place (same ``dag_version_id``, new content). A long-lived DagBag (e.g.
        the scheduler's) must reflect the new content instead of serving the cached old code.

        Each step uses its own session, matching the real deployment where the dag processor
        writes and the scheduler reads in separate processes/sessions.
        """
        dag_id = "stale_cache_dag"
        bundle_name = "testing"
        db.clear_db_dags()
        db.clear_db_serialized_dags()
        db.clear_db_dag_bundles()

        def make_lazy(task_ids):
            with DAG(dag_id, schedule=None) as dag:
                for task_id in task_ids:
                    EmptyOperator(task_id=task_id)
            return LazyDeserializedDAG.from_dag(dag)

        # Long-lived bag, like the scheduler's process-lived scheduler_dag_bag. A 0s revalidation
        # interval makes every hit revalidate, exercising the post-window reload path
        # deterministically without manipulating the clock.
        dag_bag = DBDagBag()
        dag_bag._revalidation_interval = 0

        with create_session() as session:
            session.add(DagBundleModel(name=bundle_name))
            session.flush()
            session.add(DagModel(dag_id=dag_id, bundle_name=bundle_name))
            session.flush()
            # Version 1: a single task, no task instances yet.
            SerializedDagModel.write_dag(make_lazy(["a"]), bundle_name=bundle_name, session=session)
            session.commit()
            version_id = DagVersion.get_latest_version(dag_id, session=session).id

        # The scheduler loads and caches the DAG.
        with create_session() as session:
            assert set(dag_bag.get_dag(version_id, session=session).task_ids) == {"a"}

        # The dag processor adds a task and re-writes. With no task instances on the version,
        # write_dag updates it in place (same dag_version_id, new content + hash).
        with create_session() as session:
            did_write = SerializedDagModel.write_dag(
                make_lazy(["a", "b"]), bundle_name=bundle_name, session=session
            )
            session.commit()
            assert did_write is True
            assert DagVersion.get_latest_version(dag_id, session=session).id == version_id

        # The scheduler reads again: it must serve the updated DAG, not the stale cached one.
        with create_session() as session:
            assert set(dag_bag.get_dag(version_id, session=session).task_ids) == {"a", "b"}

        db.clear_db_dags()
        db.clear_db_serialized_dags()
        db.clear_db_dag_bundles()


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

    @patch("airflow.models.dagbag.stats")
    def test_cache_hit_metric_emitted(self, mock_stats):
        """Test that cache hit metric is emitted when caching is enabled."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)
        mock_session = MagicMock()
        # last_validated=0.0 forces revalidation; the hash matches, so it counts as a hit.
        dag_bag._dags["test_version"] = _CacheEntry(MagicMock(), "hash1", 0.0)
        mock_session.scalar.return_value = "hash1"

        dag_bag._get_dag("test_version", mock_session)

        mock_stats.incr.assert_called_with("api_server.dag_bag.cache_hit")

    @patch("airflow.models.dagbag.stats")
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

    @patch("airflow.models.dagbag.stats")
    def test_cache_clear_metric_emitted(self, mock_stats):
        """Test that cache clear metric is emitted when caching is enabled."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)
        dag_bag._dags["test_version"] = MagicMock()

        dag_bag.clear_cache()

        mock_stats.incr.assert_called_with("api_server.dag_bag.cache_clear")

    @patch("airflow.models.dagbag.stats")
    def test_cache_size_gauge_emitted(self, mock_stats):
        """Test that cache size gauge is emitted when a DAG is cached."""
        dag_bag = DBDagBag(cache_size=10, cache_ttl=60)
        mock_serdag = MagicMock()
        mock_serdag.dag_version_id = "test_version_1"
        mock_serdag.dag = MagicMock()
        mock_serdag.load_op_links = True

        dag_bag._read_dag(mock_serdag)

        mock_stats.gauge.assert_called_with("api_server.dag_bag.cache_size", 1, rate=0.1)
