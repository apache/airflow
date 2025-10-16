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

import logging
import threading
import weakref
from collections import defaultdict
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from time import monotonic
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy import select

from airflow.configuration import conf
from airflow.models import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.providers.keycloak.auth_manager.constants import (
    CONF_DAG_INVENTORY_CACHE_TTL_KEY,
    CONF_DAG_PERMISSIONS_CACHE_TTL_KEY,
    CONF_SECTION_NAME,
)
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_fastapi.auth.managers.base_auth_manager import ExtendedResourceMethod
    from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

ContextAttributes = dict[str, str | None]
OptionalContextAttributes = ContextAttributes | None
CacheKey = tuple[str, str, "ExtendedResourceMethod", str | None]
DagPermissionResolver = Callable[
    ["ExtendedResourceMethod", "KeycloakAuthManagerUser", str, OptionalContextAttributes],
    bool,
]


class _DagPermissionCacheEntry(NamedTuple):
    expires_at: float
    allowed: bool


log = logging.getLogger(__name__)


class KeycloakDagPermissionCache:
    """
    Keycloak DAG permission cache.

    Caches DAG authorization decisions and inventory state for the auth manager.
    """

    def __init__(
        self,
        *,
        permission_resolver: DagPermissionResolver,
        permissions_cache_ttl_seconds: int | None = None,
        inventory_cache_ttl_seconds: int | None = None,
    ) -> None:
        if permissions_cache_ttl_seconds is None:
            permissions_cache_ttl_seconds = conf.getint(
                CONF_SECTION_NAME,
                CONF_DAG_PERMISSIONS_CACHE_TTL_KEY,
                fallback=30,
            )
        if inventory_cache_ttl_seconds is None:
            inventory_cache_ttl_seconds = conf.getint(
                CONF_SECTION_NAME,
                CONF_DAG_INVENTORY_CACHE_TTL_KEY,
                fallback=300,
            )

        self._permissions_cache_ttl_seconds = permissions_cache_ttl_seconds
        self._inventory_cache_ttl_seconds = inventory_cache_ttl_seconds
        self._permission_resolver = permission_resolver
        self._dag_permissions_cache: dict[CacheKey, _DagPermissionCacheEntry] = {}
        self._dag_permissions_cache_lock = threading.RLock()
        self._dag_inventory: dict[str | None, set[str]] | None = None
        self._dag_inventory_last_refreshed: float | None = None
        self._dag_inventory_lock = threading.RLock()
        self._dag_warmup_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="keycloak-dag-warm")
        self._warmup_executor_finalizer = weakref.finalize(
            self, self._dag_warmup_executor.shutdown, wait=False
        )
        self._dag_warmup_lock = threading.RLock()
        self._dag_warmup_inflight: set[str] = set()

    @property
    def permissions_cache_enabled(self) -> bool:
        return self._permissions_cache_ttl_seconds > 0

    def init(self) -> None:
        if not self.permissions_cache_enabled:
            return
        self._dag_warmup_executor.submit(self._prime_dag_inventory)

    def filter_authorized_dag_ids(
        self,
        *,
        dag_ids: set[str],
        user: KeycloakAuthManagerUser,
        method: ExtendedResourceMethod,
        team_name: str | None,
    ) -> set[str]:
        if not dag_ids:
            return set()

        attributes: OptionalContextAttributes = {"team_name": team_name} if team_name else None
        user_id = str(user.get_id())

        if not self.permissions_cache_enabled:
            results = self._check_dag_authorizations(
                dag_ids,
                user=user,
                method=method,
                attributes=attributes,
                log_context={"user_id": user_id},
            )
            return {dag_id for dag_id, allowed in results.items() if allowed}

        now = monotonic()
        authorized_dags: set[str] = set()
        dag_ids_to_check: list[tuple[str, CacheKey]] = []
        for dag_id in dag_ids:
            cache_key = (user_id, dag_id, method, team_name)
            cached_permission = self._get_cached_dag_permission(cache_key, now=now)
            if cached_permission is None:
                dag_ids_to_check.append((dag_id, cache_key))
                continue
            if cached_permission:
                authorized_dags.add(dag_id)

        if dag_ids_to_check:
            dag_ids_list = [dag_id for dag_id, _ in dag_ids_to_check]
            results = self._check_dag_authorizations(
                dag_ids_list,
                user=user,
                method=method,
                attributes=attributes,
                log_context={"user_id": user_id},
            )
            cache_time = monotonic()
            for dag_id, cache_key in dag_ids_to_check:
                is_authorized = results.get(dag_id)
                if is_authorized is None:
                    continue
                if is_authorized:
                    authorized_dags.add(dag_id)
                self._set_cached_dag_permission(cache_key, is_authorized, now=cache_time)

        return authorized_dags

    def schedule_dag_permission_warmup(
        self,
        user_snapshot: KeycloakAuthManagerUser,
        *,
        method: ExtendedResourceMethod,
    ) -> None:
        if not self.permissions_cache_enabled:
            return

        user_id = str(user_snapshot.get_id())
        with self._dag_warmup_lock:
            if user_id in self._dag_warmup_inflight:
                return
            self._dag_warmup_inflight.add(user_id)
        self._dag_warmup_executor.submit(self._warmup_user_dag_permissions, user_snapshot, method)

    def _get_cached_dag_permission(
        self,
        cache_key: CacheKey,
        *,
        now: float,
    ) -> bool | None:
        with self._dag_permissions_cache_lock:
            cached = self._dag_permissions_cache.get(cache_key)
            if not cached:
                return None
            if cached.expires_at <= now:
                self._dag_permissions_cache.pop(cache_key, None)
                return None
            return cached.allowed

    def _set_cached_dag_permission(
        self,
        cache_key: CacheKey,
        allowed: bool,
        *,
        now: float,
    ) -> None:
        expires_at = now + self._permissions_cache_ttl_seconds
        with self._dag_permissions_cache_lock:
            self._dag_permissions_cache[cache_key] = _DagPermissionCacheEntry(expires_at, allowed)

    def _prime_dag_inventory(self) -> None:
        try:
            inventory = self._fetch_dag_inventory()
            with self._dag_inventory_lock:
                self._dag_inventory = inventory
                self._dag_inventory_last_refreshed = monotonic()
        except Exception:
            log.exception("Failed to prime DAG inventory for permissions warmup")

    def _get_dag_inventory(self) -> dict[str | None, set[str]]:
        if self._inventory_cache_ttl_seconds <= 0:
            return self._fetch_dag_inventory()

        with self._dag_inventory_lock:
            inventory = self._dag_inventory
            last_refreshed = self._dag_inventory_last_refreshed

        current_time = monotonic()
        if (
            inventory is not None
            and last_refreshed is not None
            and current_time - last_refreshed < self._inventory_cache_ttl_seconds
        ):
            return {team: set(dag_ids) for team, dag_ids in inventory.items()}

        inventory = self._fetch_dag_inventory()
        with self._dag_inventory_lock:
            self._dag_inventory = inventory
            self._dag_inventory_last_refreshed = monotonic()
        return inventory

    @staticmethod
    @provide_session
    def _fetch_dag_inventory(*, session: Session = NEW_SESSION) -> dict[str | None, set[str]]:
        team_model_cls: type[Any] | None
        team_assoc_table: Any | None
        try:
            # Lazy import: Team models exist only in newer Airflow releases
            from airflow.models.team import (
                Team as team_model_cls_runtime,
                dag_bundle_team_association_table as team_assoc_table_runtime,
            )
        except ModuleNotFoundError:
            team_model_cls = None
            team_assoc_table = None
        else:
            team_model_cls = team_model_cls_runtime
            team_assoc_table = team_assoc_table_runtime

        if team_model_cls is None or team_assoc_table is None:
            stmt = select(DagModel.dag_id)
            dag_ids = session.execute(stmt).scalars().all()
            return {None: set(dag_ids)}

        stmt = (
            select(DagModel.dag_id, team_model_cls.name)
            .join(DagBundleModel, DagModel.bundle_name == DagBundleModel.name)
            .join(
                team_assoc_table,
                DagBundleModel.name == team_assoc_table.c.dag_bundle_name,
                isouter=True,
            )
            .join(
                team_model_cls,
                team_model_cls.id == team_assoc_table.c.team_id,
                isouter=True,
            )
        )
        rows = session.execute(stmt).all()
        dags_by_team: dict[str | None, set[str]] = defaultdict(set)
        for dag_id, team_name in rows:
            dags_by_team[team_name].add(dag_id)
        return {team: set(dag_ids) for team, dag_ids in dags_by_team.items()}

    def _warmup_user_dag_permissions(
        self,
        user: KeycloakAuthManagerUser,
        method: ExtendedResourceMethod,
    ) -> None:
        user_id = str(user.get_id())
        try:
            dag_inventory = self._get_dag_inventory()
            if not dag_inventory:
                return

            for team_name, dag_ids in dag_inventory.items():
                if not dag_ids:
                    continue
                attributes: OptionalContextAttributes = {"team_name": team_name} if team_name else None
                results = self._check_dag_authorizations(
                    dag_ids,
                    user=user,
                    method=method,
                    attributes=attributes,
                    log_context={"team": team_name},
                )
                cache_time = monotonic()
                for dag_id, allowed in results.items():
                    cache_key = (user_id, dag_id, method, team_name)
                    self._set_cached_dag_permission(cache_key, allowed, now=cache_time)
        except Exception:
            log.exception("Failed to warm DAG permissions for user %s", user_id)
        finally:
            with self._dag_warmup_lock:
                self._dag_warmup_inflight.discard(user_id)

    def _check_dag_authorizations(
        self,
        dag_ids: Iterable[str],
        *,
        user: KeycloakAuthManagerUser,
        method: ExtendedResourceMethod,
        attributes: OptionalContextAttributes = None,
        log_context: OptionalContextAttributes = None,
    ) -> dict[str, bool]:
        dag_list = list(dag_ids)
        if not dag_list:
            return {}

        user_id = str(user.get_id())
        context_suffix = ""
        if log_context:
            decorated = [f"{key}={value}" for key, value in log_context.items() if value is not None]
            if decorated:
                context_suffix = f" ({', '.join(decorated)})"

        results: dict[str, bool] = {}
        for dag_id in dag_list:
            try:
                results[dag_id] = self._permission_resolver(method, user, dag_id, attributes)
            except Exception:
                log.exception("Failed to authorize dag %s for user %s%s", dag_id, user_id, context_suffix)

        return results
