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

import functools
import importlib
import json
import logging
import os
import time
import warnings
from collections import defaultdict
from configparser import ConfigParser
from copy import deepcopy
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from itsdangerous import URLSafeSerializer
from pydantic import BaseModel, ValidationError
from sqlalchemy import and_, delete, exists, or_, select, update
from sqlalchemy.orm import selectinload

from airflow._shared.module_loading import import_string
from airflow.configuration import conf, run_command
from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: TC001
from airflow.exceptions import AirflowConfigException
from airflow.models.dagbundle import DagBundleModel
from airflow.models.team import Team
from airflow.providers_manager import ProvidersManager
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, create_session, provide_session

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.engine import CursorResult
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

_example_dag_bundle_name = "example_dags"

# Chunk size for the one-time startup repair of unconfigured bundles.
_REASSIGN_BATCH_SIZE = 1000


def _guess_best_bundle_for_fileloc(
    fileloc: str, descending_bundle_paths: dict[str, Path]
) -> tuple[str, str] | None:
    """
    Return ``(bundle_name, relative_fileloc)`` for the first bundle whose path contains ``fileloc``.

    ``descending_bundle_paths`` must be sorted by path length descending so
    the deepest bundle wins when paths overlap.

    Returns ``None`` when ``fileloc`` is not under any bundle's path.

    Uses the same plain ``Path.relative_to`` check as
    ``BaseDagImporter.get_relative_path``, so the ``relative_fileloc`` written
    here matches what the next parse computes for the same file. Filelocs are
    produced by the Dag processor parsing admin-controlled bundle files, so they
    are trusted and need no path-traversal normalization.
    """
    file_path = Path(fileloc)
    for name, path in descending_bundle_paths.items():
        try:
            relative = file_path.relative_to(path)
        except ValueError:
            continue
        return name, str(relative)
    return None


class _ExternalBundleConfig(BaseModel):
    """Schema defining the user-specified configuration for a DAG bundle."""

    name: str
    classpath: str
    kwargs: dict
    team_name: str | None = None
    importers: Any = None


class _InternalBundleConfig(BaseModel):
    """
    Schema used internally (in this file) to define the configuration for a DAG bundle.

    Configuration defined by users when read must match ``_ExternalBundleConfig``.
    This configuration is then parsed and converted to ``_InternalBundleConfig`` to be used across this file.
    """

    bundle_class: type[BaseDagBundle]
    kwargs: dict
    team_name: str | None = None


def _bundle_item_exc(msg):
    return AirflowConfigException(
        "Invalid config for section `dag_processor` key `dag_bundle_config_list`. " + msg
    )


@dataclass
class _BundleConfigSnapshot:
    """The last accepted bundle configuration in this process."""

    configs: dict[str, _ExternalBundleConfig]
    names: frozenset[str]
    team_names: dict[str, str | None]
    last_checked: float = field(default_factory=lambda: time.monotonic())


def _get_bundle_config_reload_option() -> tuple[str, str] | None:
    section, key = "dag_processor", "dag_bundle_config_list"
    env_key = "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"
    if env_key in os.environ:
        return None
    for suffix in ("cmd", "secret"):
        env_option = f"{env_key}_{suffix.upper()}"
        if env_option in os.environ:
            return suffix, os.environ[env_option]
    if ConfigParser.has_option(conf, section, key):
        return None
    for suffix in ("cmd", "secret"):
        option = f"{key}_{suffix}"
        if ConfigParser.has_option(conf, section, option):
            return suffix, ConfigParser.get(conf, section, option)
    return None


def _create_bundle_config_snapshot(bundle_config_list: list[_ExternalBundleConfig]) -> _BundleConfigSnapshot:
    team_names = {cfg.name: cfg.team_name for cfg in bundle_config_list}
    if bundle_config_list and conf.getboolean("core", "LOAD_EXAMPLES"):
        _add_example_dag_bundle(bundle_config_list)
        _add_provider_example_dags_to_bundle(bundle_config_list)
    return _BundleConfigSnapshot(
        configs={cfg.name: cfg for cfg in bundle_config_list},
        names=frozenset(cfg.name for cfg in bundle_config_list),
        team_names=team_names,
    )


def is_bundle_config_reload_enabled() -> bool:
    """Return whether bundle configuration comes from a command or secrets backend."""
    return _get_bundle_config_reload_option() is not None


@functools.cache
def _get_cached_bundle_config_snapshot(*, metadata_only: bool = False) -> _BundleConfigSnapshot:
    """Return the cached bundle snapshot, reading configuration on first use."""
    if metadata_only:
        return replace(_get_cached_bundle_config_snapshot())
    return _create_bundle_config_snapshot(_read_bundle_config_list())


def _refresh_bundle_config_snapshot(
    *,
    force: bool = False,
    metadata_only: bool = False,
    snapshot: _BundleConfigSnapshot | None = None,
) -> _BundleConfigSnapshot:
    cached = (
        _get_cached_bundle_config_snapshot(metadata_only=True)
        if metadata_only
        else _get_cached_bundle_config_snapshot()
    )
    if snapshot is None:
        snapshot = cached
    if not is_bundle_config_reload_enabled():
        return snapshot
    now = time.monotonic()
    try:
        if not force and now - cached.last_checked < conf.getint(
            "dag_processor", "bundle_refresh_check_interval"
        ):
            if snapshot.configs is cached.configs:
                return snapshot
            config_list = [cached.configs[name] for name in cached.team_names]
        else:
            cached.last_checked = now
            config_list = _read_bundle_config_list()
        if {config.name: config for config in config_list} == {
            name: snapshot.configs[name] for name in snapshot.team_names
        }:
            current = snapshot
        else:
            for config in config_list:
                existing = snapshot.configs.get(config.name)
                if existing is not None:
                    if existing != config:
                        raise _bundle_item_exc(
                            f"Use a new bundle name when changing configuration for '{config.name}'"
                        )
                    continue
                if not metadata_only:
                    if config.team_name and not conf.getboolean("core", "multi_team"):
                        raise _bundle_item_exc("Bundle teams require multi-team mode")
                    _InternalBundleConfig(
                        bundle_class=import_string(config.classpath),
                        kwargs=config.kwargs,
                        team_name=config.team_name,
                    )
            current = _create_bundle_config_snapshot(config_list)
    except Exception:
        log.exception("Could not reload Dag bundle configuration; keeping the last accepted list")
        return snapshot
    cached.configs = current.configs
    cached.names = current.names
    cached.team_names = current.team_names
    return replace(current, last_checked=cached.last_checked)


def _parse_bundle_config(config_list) -> list[_ExternalBundleConfig]:
    bundles = {}
    for item in config_list:
        if not isinstance(item, dict):
            raise _bundle_item_exc(f"Expected dict but got {item.__class__}")

        try:
            cfg = _ExternalBundleConfig(**item)
        except ValidationError as e:
            raise _bundle_item_exc(f"Item {item} failed validation: {e}")

        if cfg.name == _example_dag_bundle_name:
            raise AirflowConfigException(
                f"Bundle name '{_example_dag_bundle_name}' is a reserved name. Please choose another name for your bundle."
                " Example DAGs can be enabled with the '[core] load_examples' config."
            )

        bundles[cfg.name] = cfg
    if len(bundles.keys()) != len(config_list):
        raise _bundle_item_exc("One or more bundle names appeared multiple times")
    return list(bundles.values())


def _read_bundle_config_list() -> list[_ExternalBundleConfig]:
    if source := _get_bundle_config_reload_option():
        # Normal config lookup can silently fall back to defaults when a secret is missing.
        kind, value = source
        raw = run_command(value) if kind == "cmd" else conf._get_config_value_from_secret_backend(value)
        if not raw or not raw.strip():
            raise _bundle_item_exc("The configured command or secret returned no value")
        try:
            config_list = json.loads(raw)
        except json.JSONDecodeError as e:
            raise _bundle_item_exc("The configured command or secret returned invalid JSON") from e
    else:
        config_list = conf.getjson("dag_processor", "dag_bundle_config_list")
    if source is None and not config_list:
        return []
    if not isinstance(config_list, list):
        raise AirflowConfigException(
            "Section `dag_processor` key `dag_bundle_config_list` "
            f"must be list but got {config_list.__class__}"
        )
    return _parse_bundle_config(config_list)


def get_configured_bundle_names() -> frozenset[str]:
    """Get configured Dag bundle names without importing their classes."""
    return _refresh_bundle_config_snapshot(metadata_only=True).names


def get_configured_bundle_team_names() -> dict[str, str | None]:
    """
    Get the team owning each explicitly configured Dag bundle.

    This reads the config rather than going through ``DagBundlesManager`` so that callers who only
    need the declared bundle partition neither import every bundle class nor see the example-Dag
    bundles that ``DagBundlesManager.parse_config`` injects when ``[core] load_examples`` is set --
    those are added by Airflow, not declared by the deployment.

    :return: mapping of bundle name to team name, ``None`` for bundles that are not team scoped.
    """
    if is_bundle_config_reload_enabled():
        return dict(_refresh_bundle_config_snapshot(metadata_only=True).team_names)
    return {cfg.name: cfg.team_name for cfg in _read_bundle_config_list()}


def _add_example_dag_bundle(bundle_config_list: list[_ExternalBundleConfig]):
    from airflow import example_dags

    example_dag_folder = next(iter(example_dags.__path__))
    bundle_config_list.append(
        _ExternalBundleConfig(
            name=_example_dag_bundle_name,
            classpath="airflow.dag_processing.bundles.local.LocalDagBundle",
            kwargs={
                "path": example_dag_folder,
            },
        )
    )


def _add_provider_example_dags_to_bundle(bundle_config_list: list[_ExternalBundleConfig]):
    """
    Add an ``example_dags`` folder of every installed provider as a bundle.

    Provider locations are resolved through ``ProvidersManager`` instead of
    walking ``airflow.providers.__path__`` so that:

    - nested providers (e.g. ``apache-airflow-providers-common-sql`` whose
      module path is ``airflow.providers.common.sql``) are discovered;
    - providers installed outside the ``airflow.providers`` namespace package
      are discovered via their entry point.
    """
    # Dedup on the resolved on-disk folder rather than the bundle name: distributions
    # under ``airflow.providers.common.*`` use ``pkgutil.extend_path``, so when several
    # ``common-*`` packages are installed ``airflow.providers.common.__path__`` has
    # multiple entries and the inner loop iterates more than once. Path-based dedup
    # only skips when the same folder is seen twice; distinct folders are preserved.
    seen: set[str] = set()

    for package_name in ProvidersManager().providers:
        # Heuristic: derive the import path from the canonical
        # ``apache-airflow-providers-*`` distribution name. Tracked as a follow-up
        # to record the provider module path on ``ProviderInfo`` (see
        # https://github.com/apache/airflow/issues/66305).
        if package_name.startswith("apache-airflow-providers-"):
            suffix = package_name[len("apache-airflow-providers-") :]
            module_name = "airflow.providers." + suffix.replace("-", ".")
        else:
            module_name = package_name.replace("-", "_")
        try:
            module = importlib.import_module(module_name)
            module_paths = list(getattr(module, "__path__", []))
        except Exception:
            log.exception("Could not load provider module %s for example DAG discovery", module_name)
            continue

        for module_path in module_paths:
            example_dag_folder = os.path.join(module_path, "example_dags")
            if not os.path.isdir(example_dag_folder):
                continue
            if example_dag_folder in seen:
                continue
            seen.add(example_dag_folder)
            bundle_name = f"{package_name}-example-dags"
            bundle_config_list.append(
                _ExternalBundleConfig(
                    name=bundle_name,
                    classpath="airflow.dag_processing.bundles.local.LocalDagBundle",
                    kwargs={
                        "path": example_dag_folder,
                    },
                )
            )


def _is_safe_bundle_url(url: str) -> bool:
    """
    Check if a bundle URL is safe to use.

    This function validates that the URL:
    - Uses HTTP or HTTPS schemes (no JavaScript, data, or other schemes)
    - Is properly formatted
    - Doesn't contain malicious content
    """
    import logging
    from urllib.parse import urlparse

    logger = logging.getLogger(__name__)

    if not url:
        return False

    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            logger.error(
                "Bundle URL uses unsafe scheme '%s'. Only 'http' and 'https' are allowed", parsed.scheme
            )
            return False

        if not parsed.netloc:
            logger.error("Bundle URL '%s' has no network location", url)
            return False

        if any(ord(c) < 32 for c in url):
            logger.error("Bundle URL '%s' contains control characters (ASCII < 32)", url)
            return False

        return True
    except Exception as e:
        logger.error("Failed to parse bundle URL '%s': %s", url, str(e))
        return False


def _sign_bundle_url(url: str, bundle_name: str) -> str:
    """
    Sign a bundle URL for integrity verification.

    :param url: The URL to sign
    :param bundle_name: The name of the bundle (used in the payload)
    :return: The signed URL token
    """
    serializer = URLSafeSerializer(conf.get_mandatory_value("core", "fernet_key"))
    payload = {
        "url": url,
        "bundle_name": bundle_name,
    }
    return serializer.dumps(payload)


class DagBundlesManager(LoggingMixin):
    """Manager for DAG bundles."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._bundle_config: dict[str, _InternalBundleConfig] = {}
        # Keep misses local so new task managers can discover bundles immediately.
        self._failed_bundle_lookups: dict[str, float] = {}
        self._pending_template_updates: dict[str, BaseDagBundle | None] = {}
        self._snapshot = replace(_get_cached_bundle_config_snapshot())
        self.parse_config()

    def parse_config(self) -> None:
        """
        Load the accepted bundle configuration into this manager.

        Reuse the parsed configuration while the configured bundle names remain unchanged.

        :meta private:
        """
        snapshot = self._snapshot
        if self._bundle_config.keys() == snapshot.names:
            return

        self._bundle_config = {}
        bundle_config_list = snapshot.configs.values()
        if not bundle_config_list:
            return

        for bundle_config in bundle_config_list:
            if bundle_config.team_name and not conf.getboolean("core", "multi_team"):
                raise AirflowConfigException(
                    "Section `dag_processor` key `dag_bundle_config_list` "
                    "cannot have a team name when multi-team mode is disabled."
                    "To enable multi-team, you need to update section `core` key `multi_team` in your config."
                )

            class_ = import_string(bundle_config.classpath)
            self._bundle_config[bundle_config.name] = _InternalBundleConfig(
                bundle_class=class_,
                kwargs=bundle_config.kwargs,
                team_name=bundle_config.team_name,
            )
        self.log.info("DAG bundles loaded: %s", ", ".join(self._bundle_config.keys()))

    @provide_session
    def sync_bundles_to_db(
        self,
        *,
        deactivate_missing: bool = True,
        bundle_instances: dict[str, BaseDagBundle] | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Persist the configured DAG bundles into ``DagBundleModel`` rows.

        This only reconciles bundle metadata, not the DAGs contained in them.
        Parsing each bundle's DAG files and writing the resulting
        ``DagModel`` / ``SerializedDagModel`` rows is the responsibility of
        ``DagBag`` plus ``sync_bag_to_db`` (or, in production, the DAG
        processor); calling this method does not trigger that work.

        :param deactivate_missing: When ``True`` (the default), any bundle stored in
            the database that is not present in this manager's config is marked
            inactive. This is only correct when the calling process sees the
            *complete* bundle configuration. When multiple dag-processors each run
            with a partial config (e.g. one bundle per processor), pass ``False`` so a
            processor does not disable bundles owned by other processors -- otherwise
            the processors repeatedly deactivate each other and never converge.
        """
        self.log.debug("Syncing DAG bundles to the database")

        def _extract_and_sign_template(
            bundle_name: str, bundle_instance: BaseDagBundle
        ) -> tuple[str | None, dict]:
            new_template_ = bundle_instance.view_url_template()
            new_params_ = self._extract_template_params(bundle_instance)
            if new_template_:
                if not _is_safe_bundle_url(new_template_):
                    self.log.warning(
                        "Bundle %s has unsafe URL template '%s', skipping URL update",
                        bundle_name,
                        new_template_,
                    )
                    new_template_ = None
                else:
                    # Sign the URL for integrity verification
                    new_template_ = _sign_bundle_url(new_template_, bundle_name)
                    self.log.debug("Signed URL template for bundle %s", bundle_name)
            return new_template_, new_params_

        stored = {
            b.name: b
            for b in session.scalars(select(DagBundleModel).options(selectinload(DagBundleModel.teams))).all()
        }
        bundle_to_team = {
            bundle.name: bundle.teams[0].name if len(bundle.teams) == 1 else None
            for bundle in stored.values()
        }

        requested_teams = {config.team_name for config in self._bundle_config.values() if config.team_name}
        teams = {
            team.name: team for team in session.scalars(select(Team).where(Team.name.in_(requested_teams)))
        }
        for team_name in requested_teams:
            if team_name not in teams:
                raise _bundle_item_exc(f"Team '{team_name}' does not exist")

        template_instances = self._pending_template_updates | (bundle_instances or {})
        pending_templates: dict[str, BaseDagBundle | None] = {}
        for name, config in self._bundle_config.items():
            team = teams.get(config.team_name) if config.team_name else None

            bundle = stored.pop(name, None)
            update_template = bundle_instances is None or name in template_instances
            if update_template:
                bundle_instance = template_instances.get(name)
                try:
                    if bundle_instance is None:
                        bundle_instance = self.get_bundle(name)
                    new_template, new_params = _extract_and_sign_template(name, bundle_instance)
                except Exception as e:
                    self.log.exception("Error creating bundle '%s': %s", name, e)
                    pending_templates[name] = bundle_instance
                    if bundle is None and bundle_instances is None:
                        continue
                    update_template = False
            if bundle is None:
                bundle = DagBundleModel(name=name)
                session.add(bundle)
                self.log.info("Added new Dag bundle %s to the database", name)
            bundle.active = True
            if update_template:
                if new_template != bundle.signed_url_template:
                    bundle.signed_url_template = new_template
                    self.log.debug("Updated URL template for bundle %s", name)
                if new_params != bundle.template_params:
                    bundle.template_params = new_params
                    self.log.debug("Updated template parameters for bundle %s", name)

            if team and bundle_to_team.get(name) != config.team_name:
                # Change of team. It can be associating a team to a dag bundle that did not have one or
                # swapping a team for another
                bundle.teams = [team]
                if bundle_to_team.get(name):
                    self.log.warning(
                        "Changing ownership of team '%s' from Dag bundle '%s' to '%s'",
                        bundle_to_team[name],
                        name,
                        team.name,
                    )
            elif not team and bundle_to_team.get(name) is not None:
                # Only remove ownership if a team was previously associated; stored bundles with
                # no team already map to None in bundle_to_team.
                # Remove team association
                self.log.warning(
                    "Removing ownership of team '%s' from Dag bundle '%s'", bundle_to_team[name], name
                )
                bundle.teams = []

        self._pending_template_updates = pending_templates
        if not deactivate_missing:
            # This process only owns a subset of the bundles, so the remaining stored
            # bundles may well be owned by another dag-processor. Leave them alone.
            return

        # Import here to avoid circular import
        from airflow.models.errors import ParseImportError

        for name, bundle in stored.items():
            if bundle.active is False:
                continue
            bundle.active = False
            bundle.teams = []
            self.log.warning("DAG bundle %s is no longer found in config and has been disabled", name)
            session.execute(delete(ParseImportError).where(ParseImportError.bundle_name == name))
            self.log.info("Deleted import errors for bundle %s which is no longer configured", name)

    def reassign_dags_with_unconfigured_bundles(self) -> int:
        """
        Reassign Dags pointing at unconfigured bundles to a configured one.

        Side effect of the ``0082_3_1_0_make_bundle_name_not_nullable``
        migration (#63323): legacy rows get ``bundle_name='dags-folder'``
        and NULL ``relative_fileloc``, which raises ``Requested bundle
        '{name}' is not configured.`` at trigger time when the deployment
        uses a custom bundle.

        Each legacy-candidate row (NULL ``relative_fileloc`` and no
        ``DagVersion``) is routed to the most-specific configured bundle
        whose path contains its ``fileloc``, writing ``relative_fileloc``
        atomically. Rows whose ``fileloc`` is under no configured bundle
        are left untouched -- writing ``bundle_name`` without a verified
        ``relative_fileloc`` would produce a row task workers cannot
        execute -- and instead self-heal via the normal staleness
        lifecycle: ``sync_bundles_to_db`` deactivates the old bundle, the
        stale-scan marks the row stale, and the next successful parse
        from any configured bundle resets everything via
        ``update_dag_parsing_results_in_db``. No manual ``airflow dags
        reserialize`` is required.

        Multi-team-safe because a bundle path belongs to at most one team.
        Each chunk runs in its own internally-owned transaction so the
        row-lock window stays bounded and no caller-provided session is
        committed.

        :return: Number of Dags reassigned.
        """
        # Import here to avoid circular import
        # (manager -> dag -> dagrun -> taskinstance -> dag_version -> manager)
        from airflow.models.dag import DagModel
        from airflow.models.dag_version import DagVersion

        # Fast-skip once any 3.x parse cycle has run. DagVersion is written
        # only by the parse path, and that path overwrites both bundle_name
        # and relative_fileloc on every parse (see
        # ``DagModelOperation.update_dags``), so any legacy row whose file
        # is under a configured bundle self-heals at the next parse and any
        # row whose file is under no configured bundle self-heals via the
        # staleness lifecycle -- reassign has no work the parse path will
        # not do itself. The probe is an index hit on dag_version's PK
        # vs. a sequential scan of dag (no index on relative_fileloc).
        with create_session() as session:
            if session.scalar(select(DagVersion.id).limit(1)) is not None:
                return 0

            if not (active_bundle_paths := self._resolve_active_bundle_paths(session=session)):
                self.log.info(
                    "No active Dag bundles with resolvable paths; skipping reassignment of Dags "
                    "with unconfigured bundles."
                )
                return 0

        # Chunked UPDATEs ordered by (bundle_name, dag_id), one transaction per
        # chunk; repaired rows drop out of the predicate because writing
        # relative_fileloc makes the IS NULL clause false. Scanning in
        # bundle_name order groups the movements log by source bundle without
        # a Python-side re-sort.
        #
        # Legacy-candidate predicate (rows never parsed in 3.x): NULL
        # relative_fileloc (the 0082 migration leaves it NULL) AND NOT EXISTS
        # DagVersion (the parse path writes DagModel.bundle_name before the
        # DagVersion). Equivalent under that invariant; both stated as
        # defense in depth and repeated on the UPDATE itself as a CAS guard
        # so a concurrent parser write wins the race.
        movements: dict[tuple[str | None, str], int] = defaultdict(int)
        total_reassigned = 0
        total_backfilled = 0
        total_skipped = 0
        last_seen: tuple[str, str] | None = None

        while True:
            with create_session() as session:
                query = (
                    select(DagModel.dag_id, DagModel.bundle_name, DagModel.fileloc)
                    .where(
                        DagModel.relative_fileloc.is_(None),
                        ~exists().where(DagVersion.dag_id == DagModel.dag_id),
                    )
                    .order_by(DagModel.bundle_name, DagModel.dag_id)
                    .limit(_REASSIGN_BATCH_SIZE)
                )
                if last_seen is not None:
                    last_bundle, last_dag_id = last_seen
                    query = query.where(
                        or_(
                            DagModel.bundle_name > last_bundle,
                            and_(DagModel.bundle_name == last_bundle, DagModel.dag_id > last_dag_id),
                        )
                    )

                if not (chunk := session.execute(query).all()):
                    break
                last_seen = (chunk[-1].bundle_name, chunk[-1].dag_id)

            # Route every legacy row by fileloc, not just those on
            # unconfigured bundles, so a migration-assigned dags-folder
            # row whose file lives under a different configured bundle
            # gets relocated instead of stranded. Classify as skip
            # (no match), backfill (match == current bundle), or
            # reassign (match != current bundle).
            chunk_updates: list[tuple[str, str | None, str, str]] = []
            for row in chunk:
                match = (
                    _guess_best_bundle_for_fileloc(row.fileloc, active_bundle_paths) if row.fileloc else None
                )
                if match is None:
                    total_skipped += 1
                    continue
                target, relative = match
                chunk_updates.append((row.dag_id, row.bundle_name, target, relative))

            if not chunk_updates:
                continue

            with create_session() as session:
                # create_session commits on context exit, bounding the
                # row-lock window to one chunk.
                for dag_id, prev_bundle, target, relative in chunk_updates:
                    result = cast(
                        "CursorResult",
                        session.execute(
                            update(DagModel)
                            .where(
                                DagModel.dag_id == dag_id,
                                DagModel.relative_fileloc.is_(None),
                                ~exists().where(DagVersion.dag_id == DagModel.dag_id),
                            )
                            .values(relative_fileloc=relative, bundle_name=target)
                            .execution_options(synchronize_session=False)
                        ),
                    )

                    if result.rowcount:
                        # Rowcount is the source of truth for whether the CAS actually fired
                        if target == prev_bundle:
                            total_backfilled += 1
                        else:
                            movements[(prev_bundle, target)] += 1
                            total_reassigned += 1
                    else:
                        self.log.debug("Skipping repair for Dag '%s': lost race to parser.", dag_id)

        for (prev, target), n in movements.items():
            self.log.info(
                "Reassigning %d Dag(s) from bundle '%s' to '%s'",
                n,
                prev,
                target,
            )

        if total_backfilled:
            self.log.info("Backfilled relative_fileloc for %d legacy Dag(s).", total_backfilled)

        if total_skipped:
            self.log.warning(
                "Skipped %d legacy Dag(s) whose fileloc is not under any configured bundle; "
                "triggering them will keep raising \"Requested bundle '{name}' is not configured.\" "
                "until a bundle whose path contains the fileloc is added to "
                "dag_bundle_config_list. The next parse will then restore them automatically, "
                "or run `airflow dags reserialize` to force the parse path to rewrite "
                "bundle_name and relative_fileloc immediately.",
                total_skipped,
            )

        return total_reassigned

    def _resolve_active_bundle_paths(self, *, session: Session) -> dict[str, Path]:
        """
        Return paths for configured-and-active bundles.

        A bundle is "configured-and-active" when it is both in the manager's
        config and persisted as ``active=True`` in ``dag_bundle`` -- bundles
        missing from ``dag_bundle`` are excluded so they can't trigger an FK
        violation if used as a reassignment target.

        The returned dict is ``{name: path}`` ordered by path length descending
        so the most specific bundle wins in ``_guess_best_bundle_for_fileloc``.
        """
        active_db_names = set(
            session.scalars(select(DagBundleModel.name).where(DagBundleModel.active.is_(True)))
        )

        active_bundle_paths: dict[str, Path] = {}
        for bundle in self.get_all_dag_bundles():
            if bundle.name not in active_db_names:
                continue
            active_bundle_paths[bundle.name] = bundle.path

        return dict(sorted(active_bundle_paths.items(), key=lambda item: len(str(item[1])), reverse=True))

    @staticmethod
    def _extract_template_params(bundle_instance: BaseDagBundle) -> dict:
        """
        Extract template parameters from a bundle instance's view_url_template method.

        :param bundle_instance: The bundle instance to extract parameters from
        :return: Dictionary of template parameters
        """
        import re

        params: dict[str, str] = {}
        template = bundle_instance.view_url_template()

        if not template:
            return params

        # Extract template placeholders using regex
        # This matches {placeholder} patterns in the template
        PLACEHOLDER_PATTERN = re.compile(r"\{([^}]+)\}")
        placeholders = PLACEHOLDER_PATTERN.findall(template)

        # Extract values for each placeholder found in the template
        for placeholder in placeholders:
            field_value = getattr(bundle_instance, placeholder, None)
            if field_value:
                params[placeholder] = field_value

        return params

    def get_bundle_importers_config(self, name: str) -> Any:
        """Return accepted importer settings without refreshing bundle configuration."""
        try:
            return deepcopy(self._snapshot.configs[name].importers)
        except KeyError:
            raise ValueError(f"Requested bundle '{name}' is not configured.") from None

    def _refresh_config(self) -> None:
        """Refresh bundle configuration and update this manager."""
        self._snapshot = _refresh_bundle_config_snapshot(snapshot=self._snapshot)
        self.parse_config()

    def get_bundle(
        self, name: str, version: str | None = None, version_data: dict[str, Any] | None = None
    ) -> BaseDagBundle:
        """
        Get a DAG bundle by name.

        :param name: The name of the DAG bundle.
        :param version: The version of the DAG bundle you need (optional). If not provided, ``tracking_ref`` will be used instead.
        :param version_data: Optional structured data associated with this version (e.g., S3 manifest).

        :return: The DAG bundle.
        """
        bundle_config = self._bundle_config
        if name not in bundle_config:
            snapshot = _get_cached_bundle_config_snapshot()
            if name not in snapshot.names and is_bundle_config_reload_enabled():
                now = time.monotonic()
                interval = conf.getint("dag_processor", "bundle_refresh_check_interval")
                self._failed_bundle_lookups = {
                    bundle_name: checked_at
                    for bundle_name, checked_at in self._failed_bundle_lookups.items()
                    if now - checked_at < interval
                }
                if name not in self._failed_bundle_lookups:
                    snapshot = _refresh_bundle_config_snapshot(force=True)
                    if name not in snapshot.names:
                        self._failed_bundle_lookups[name] = now
            if name in snapshot.names:
                bundle_config = DagBundlesManager()._bundle_config
        cfg_bundle = bundle_config.get(name)
        if not cfg_bundle:
            raise ValueError(f"Requested bundle '{name}' is not configured.")
        self._failed_bundle_lookups.pop(name, None)
        return cfg_bundle.bundle_class(
            name=name, version=version, version_data=version_data, **cfg_bundle.kwargs
        )

    @classmethod
    def is_bundle_configured(cls, name: str) -> bool:
        """
        Return whether *name* is a configured Dag bundle.

        Only configuration metadata is read; bundle classes need not be installed
        in the process performing this check.
        """
        return name in get_configured_bundle_names()

    def get_all_dag_bundles(self) -> Iterable[BaseDagBundle]:
        """
        Get all DAG bundles.

        :return: list of DAG bundles.
        """
        self._refresh_config()
        for name, cfg in self._bundle_config.items():
            try:
                yield cfg.bundle_class(name=name, version=None, **cfg.kwargs)
            except Exception as e:
                self.log.exception("Error creating bundle '%s': %s", name, e)
                # Skip this bundle and continue with others
                continue

    def get_all_bundle_names(self) -> Iterable[str]:
        """
        Get all bundle names, refreshing dynamic configuration when due.

        :return: sorted list of bundle names.
        """
        self._refresh_config()
        return sorted(self._bundle_config.keys())

    def view_url(self, name: str, version: str | None = None) -> str | None:
        warnings.warn(
            "The 'view_url' method is deprecated and will be removed when providers "
            "have Airflow 3.1 as the minimum supported version. "
            "Use DagBundleModel.render_url() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        bundle = self.get_bundle(name, version)
        return bundle.view_url(version=version)
