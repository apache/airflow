#
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

import hashlib
import time
from collections.abc import MutableMapping
from contextlib import nullcontext
from threading import RLock
from typing import TYPE_CHECKING, Any, NamedTuple
from uuid import UUID

from cachetools import LRUCache, TTLCache
from sqlalchemy import String, select
from sqlalchemy.orm import Mapped, joinedload, mapped_column

from airflow._shared.observability.metrics import stats
from airflow.configuration import conf
from airflow.models.base import Base, StringID
from airflow.models.dag_version import DagVersion

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy.orm import Session

    from airflow.models import DagRun
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.definitions.dag import SerializedDAG


class _CacheEntry(NamedTuple):
    """A cached deserialized DAG plus the metadata needed to detect staleness on lookup."""

    dag: SerializedDAG
    dag_hash: str
    # Monotonic timestamp of the last time this entry's dag_hash was confirmed current against the
    # DB. Used to throttle revalidation: a serialized DAG cannot be rewritten more often than
    # [core] min_serialized_dag_update_interval, so within that window the cached copy is served
    # without a DB round-trip. Because the window restarts on each confirmed hit and is on a
    # different clock than the dag processor's write throttle, worst-case staleness is bounded to
    # roughly one-to-two update intervals -- still bounded, vs. the previous unbounded-until-restart.
    last_validated: float


class DBDagBag:
    """
    Internal class for retrieving dags from the database.

    Optionally supports LRU+TTL caching when cache_size is provided.
    The scheduler uses this without caching, while the API server can
    enable caching via configuration.

    :meta private:
    """

    def __init__(
        self,
        load_op_links: bool = True,
        cache_size: int | None = None,
        cache_ttl: int | None = None,
    ) -> None:
        """
        Initialize DBDagBag.

        :param load_op_links: Should the extra operator link be loaded when de-serializing the DAG?
        :param cache_size: Size of LRU cache. If None or 0, uses unbounded dict (no eviction).
        :param cache_ttl: Time-to-live for cache entries in seconds. If None or 0, no TTL (LRU only).
        """
        self.load_op_links = load_op_links
        self._dags: MutableMapping[UUID | str, _CacheEntry] = {}
        self._use_cache = False

        self._revalidation_interval = conf.getint("core", "min_serialized_dag_update_interval")

        # Initialize bounded cache if cache_size is provided and > 0
        if cache_size and cache_size > 0:
            if cache_ttl and cache_ttl > 0:
                self._dags = TTLCache(maxsize=cache_size, ttl=cache_ttl)
            else:
                self._dags = LRUCache(maxsize=cache_size)
            self._use_cache = True

        # Lock required for bounded caches: cachetools caches are NOT thread-safe
        # (LRU reordering and TTL cleanup mutate internal linked lists).
        # nullcontext for unbounded dict avoids lock overhead in the scheduler path.
        self._lock: RLock | nullcontext = RLock() if self._use_cache else nullcontext()

    def _read_dag(self, serdag: SerializedDagModel) -> SerializedDAG | None:
        """Read and cache a SerializedDAG (with its ``dag_hash`` for staleness detection)."""
        serdag.load_op_links = self.load_op_links
        dag = serdag.dag
        if not dag:
            return None
        with self._lock:
            self._dags[serdag.dag_version_id] = _CacheEntry(dag, serdag.dag_hash, time.monotonic())
            cache_size = len(self._dags)
        if self._use_cache:
            stats.gauge("api_server.dag_bag.cache_size", cache_size, rate=0.1)
        return dag

    @staticmethod
    def _current_dag_hash(version_id: UUID | str, session: Session) -> str | None:
        """Return the current ``dag_hash`` of the serialized DAG for ``version_id``, or None."""
        from airflow.models.serialized_dag import SerializedDagModel

        return session.scalar(
            select(SerializedDagModel.dag_hash).where(SerializedDagModel.dag_version_id == version_id)
        )

    def _get_dag(self, version_id: UUID | str, session: Session) -> SerializedDAG | None:
        with self._lock:
            cached = self._dags.get(version_id)

        if cached is not None:
            now = time.monotonic()
            # A serialized DAG cannot be rewritten more often than
            # [core] min_serialized_dag_update_interval, so an entry validated within that window
            # cannot have gone stale yet -- serve it without touching the DB.
            if now - cached.last_validated < self._revalidation_interval:
                if self._use_cache:
                    stats.incr("api_server.dag_bag.cache_hit")
                return cached.dag
            # Past the window: a version may have been updated in place (same dag_version_id, new
            # content + new dag_hash) by SerializedDagModel.write_dag, so confirm the cached copy
            # against the current dag_hash. That validation is a single-row lookup on the
            # uniquely-indexed serialized_dag.dag_version_id column.
            if self._current_dag_hash(version_id, session) == cached.dag_hash:
                # Still current: restart the revalidation window so the next hits skip the query.
                # (For a TTLCache this write-back also refreshes the entry's TTL/LRU recency, which
                # is fine -- the entry was just re-confirmed against the DB.)
                with self._lock:
                    current = self._dags.get(version_id)
                    if current is not None and current.dag_hash == cached.dag_hash:
                        self._dags[version_id] = current._replace(last_validated=now)
                if self._use_cache:
                    stats.incr("api_server.dag_bag.cache_hit")
                return cached.dag
            # Stale (updated in place) or the version no longer exists: drop and reload below.
            with self._lock:
                self._dags.pop(version_id, None)

        dag_version = session.get(DagVersion, version_id, options=[joinedload(DagVersion.serialized_dag)])
        if not dag_version:
            return None
        if not (serdag := dag_version.serialized_dag):
            return None

        # Double-checked locking: another thread may have cached it while we queried DB. Such an
        # entry was just loaded from the DB, so it is well within its revalidation window and is
        # served without an extra hash check, consistent with the policy above. Only emit the miss
        # metric after confirming no other thread cached it, to avoid counting a single lookup as
        # both a miss and a hit.
        if self._use_cache:
            with self._lock:
                if (cached := self._dags.get(version_id)) is not None:
                    stats.incr("api_server.dag_bag.cache_hit")
                    return cached.dag
            stats.incr("api_server.dag_bag.cache_miss")
        return self._read_dag(serdag)

    def get_dag(self, version_id: UUID | str, session: Session) -> SerializedDAG | None:
        """Get a dag by its version id, using cache if enabled."""
        return self._get_dag(version_id=version_id, session=session)

    def get_serialized_dag_model(self, version_id: UUID | str, session: Session) -> SerializedDagModel | None:
        """
        Return the SerializedDagModel for a given dag version id.

        Always queries the database. The triggerer needs the full model
        for ``serialized_dag_model.data``, which cannot be stored in the
        LRU/TTL cache (it stores deserialized SerializedDAG objects).
        """
        dag_version = session.get(DagVersion, version_id, options=[joinedload(DagVersion.serialized_dag)])
        if not dag_version or not (serdag := dag_version.serialized_dag):
            return None
        serdag.load_op_links = self.load_op_links
        return serdag

    def clear_cache(self) -> int:
        """
        Clear all cached DAGs and serialized DAG models.

        :return: Number of entries cleared from the DAG cache.
        """
        with self._lock:
            count = len(self._dags)
            self._dags.clear()

        if self._use_cache:
            stats.incr("api_server.dag_bag.cache_clear")
            stats.gauge("api_server.dag_bag.cache_size", 0)
        return count

    @staticmethod
    def _version_from_dag_run(dag_run: DagRun, *, session: Session) -> UUID | None:
        if not dag_run.bundle_version:
            if dag_version := DagVersion.get_latest_version(dag_id=dag_run.dag_id, session=session):
                return dag_version.id

        return dag_run.created_dag_version_id

    def get_dag_for_run(self, dag_run: DagRun, session: Session) -> SerializedDAG | None:
        if version_id := self._version_from_dag_run(dag_run=dag_run, session=session):
            return self._get_dag(version_id=version_id, session=session)
        return None

    def iter_all_latest_version_dags(self, *, session: Session) -> Generator[SerializedDAG, None, None]:
        """
        Walk through all latest version dags available in the database.

        Note: This method does NOT cache the DAGs to avoid cache thrashing when
        iterating over many DAGs. Each DAG is deserialized fresh from the database.
        """
        from airflow.models.serialized_dag import SerializedDagModel

        for sdm in session.scalars(select(SerializedDagModel)):
            sdm.load_op_links = self.load_op_links
            if dag := sdm.dag:
                yield dag

    def get_latest_version_of_dag(self, dag_id: str, *, session: Session) -> SerializedDAG | None:
        """Get the latest version of a dag by its id."""
        from airflow.models.serialized_dag import SerializedDagModel

        if not (serdag := SerializedDagModel.get(dag_id, session=session)):
            return None
        return self._read_dag(serdag)


def generate_md5_hash(context):
    bundle_name = context.get_current_parameters()["bundle_name"]
    relative_fileloc = context.get_current_parameters()["relative_fileloc"]
    return hashlib.md5(f"{bundle_name}:{relative_fileloc}".encode()).hexdigest()


class DagPriorityParsingRequest(Base):
    """Model to store the dag parsing requests that will be prioritized when parsing files."""

    __tablename__ = "dag_priority_parsing_request"

    # Adding a unique constraint to fileloc results in the creation of an index and we have a limitation
    # on the size of the string we can use in the index for MySQL DB. We also have to keep the fileloc
    # size consistent with other tables. This is a workaround to enforce the unique constraint.
    id: Mapped[str] = mapped_column(
        String(32), primary_key=True, default=generate_md5_hash, onupdate=generate_md5_hash
    )

    bundle_name: Mapped[str] = mapped_column(StringID(), nullable=False)
    # The location of the file containing the DAG object
    # Note: Do not depend on fileloc pointing to a file; in the case of a
    # packaged DAG, it will point to the subpath of the DAG within the
    # associated zip.
    relative_fileloc: Mapped[str] = mapped_column(String(2000), nullable=False)

    def __init__(self, bundle_name: str, relative_fileloc: str) -> None:
        super().__init__()
        self.bundle_name = bundle_name
        self.relative_fileloc = relative_fileloc

    def __repr__(self) -> str:
        return f"<DagPriorityParsingRequest: bundle_name={self.bundle_name} relative_fileloc={self.relative_fileloc}>"


def __getattr__(name: str) -> Any:
    """
    Backwards-compat shim: importing DagBag from airflow.models.dagbag is deprecated.

    Emits DeprecationWarning and re-exports DagBag from airflow.dag_processing.dagbag
    to preserve compatibility for external callers.
    """
    if name in {"DagBag", "FileLoadStat", "timeout"}:
        import warnings

        from airflow.utils.deprecation_tools import DeprecatedImportWarning

        warnings.warn(
            f"Importing {name} from airflow.models.dagbag is deprecated and will be removed in a future "
            "release. Please import from airflow.dag_processing.dagbag instead.",
            DeprecatedImportWarning,
            stacklevel=2,
        )
        # Import on demand to avoid import-time side effects
        from airflow.dag_processing import dagbag as _dagbag

        return getattr(_dagbag, name)
    raise AttributeError(name)
