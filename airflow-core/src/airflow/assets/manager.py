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

from collections.abc import Collection
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import exc, or_, select
from sqlalchemy.orm import joinedload

from airflow.configuration import conf
from airflow.listeners.listener import get_listener_manager
from airflow.models.asset import (
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetAliasReference,
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
)
from airflow.stats import Stats
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models.dag import DagModel
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetUniqueKey

log = structlog.get_logger(__name__)


class AssetManager(LoggingMixin):
    """
    A pluggable class that manages operations for assets.

    The intent is to have one place to handle all Asset-related operations, so different
    Airflow deployments can use plugins that broadcast Asset events to each other.
    """

    @classmethod
    def create_assets(cls, assets: list[Asset], *, session: Session) -> list[AssetModel]:
        """Create new assets."""

        def _add_one(asset: Asset) -> AssetModel:
            model = AssetModel.from_public(asset)
            session.add(model)
            cls.notify_asset_created(asset=asset)
            return model

        return [_add_one(a) for a in assets]

    @classmethod
    def create_asset_aliases(
        cls,
        asset_aliases: list[AssetAlias],
        *,
        session: Session,
    ) -> list[AssetAliasModel]:
        """Create new asset aliases."""

        def _add_one(asset_alias: AssetAlias) -> AssetAliasModel:
            model = AssetAliasModel.from_public(asset_alias)
            session.add(model)
            cls.notify_asset_alias_created(asset_assets=asset_alias)
            return model

        return [_add_one(a) for a in asset_aliases]

    @classmethod
    def _add_asset_alias_association(
        cls,
        alias_names: Collection[str],
        asset_model: AssetModel,
        *,
        session: Session,
    ) -> None:
        already_related = {m.name for m in asset_model.aliases}
        existing_aliases = {
            m.name: m
            for m in session.scalars(select(AssetAliasModel).where(AssetAliasModel.name.in_(alias_names)))
        }
        asset_model.aliases.extend(
            existing_aliases.get(name, AssetAliasModel(name=name))
            for name in alias_names
            if name not in already_related
        )

    @classmethod
    def register_asset_change(
        cls,
        *,
        task_instance: TaskInstance | None = None,
        asset: Asset | AssetModel | AssetUniqueKey,
        extra=None,
        source_alias_names: Collection[str] = (),
        session: Session,
        **kwargs,
    ) -> AssetEvent | None:
        """
        Register asset related changes.

        For local assets, look them up, record the asset event, queue dagruns, and broadcast
        the asset event
        """
        from airflow.models.dag import DagModel

        asset_model: AssetModel | None = session.scalar(
            select(AssetModel)
            .where(AssetModel.name == asset.name, AssetModel.uri == asset.uri)
            .options(
                joinedload(AssetModel.active),
                joinedload(AssetModel.aliases),
                joinedload(AssetModel.scheduled_dags).joinedload(DagScheduleAssetReference.dag),
            )
        )
        if not asset_model:
            cls.logger().warning("AssetModel %s not found", asset)
            return None

        if not asset_model.active:
            cls.logger().warning("Emitting event for inactive AssetModel %s", asset)

        cls._add_asset_alias_association(
            alias_names=source_alias_names, asset_model=asset_model, session=session
        )

        event_kwargs = {
            "asset_id": asset_model.id,
            "extra": extra,
        }
        if task_instance:
            event_kwargs.update(
                source_task_id=task_instance.task_id,
                source_dag_id=task_instance.dag_id,
                source_run_id=task_instance.run_id,
                source_map_index=task_instance.map_index,
            )

        asset_event = AssetEvent(**event_kwargs)
        session.add(asset_event)
        session.flush()  # Ensure the event is written earlier than DDRQ entries below.

        dags_to_queue_from_asset = {
            ref.dag for ref in asset_model.scheduled_dags if not ref.dag.is_stale and not ref.dag.is_paused
        }

        dags_to_queue_from_asset_alias = set()
        if source_alias_names:
            asset_alias_models = session.scalars(
                select(AssetAliasModel)
                .where(AssetAliasModel.name.in_(source_alias_names))
                .options(
                    joinedload(AssetAliasModel.scheduled_dags).joinedload(DagScheduleAssetAliasReference.dag)
                )
            ).unique()

            for asset_alias_model in asset_alias_models:
                asset_alias_model.asset_events.append(asset_event)
                session.add(asset_alias_model)

                dags_to_queue_from_asset_alias |= {
                    alias_ref.dag
                    for alias_ref in asset_alias_model.scheduled_dags
                    if not alias_ref.dag.is_stale and not alias_ref.dag.is_paused
                }

        dags_to_queue_from_asset_ref = set(
            session.scalars(
                select(DagModel)
                .join(DagModel.schedule_asset_name_references, isouter=True)
                .join(DagModel.schedule_asset_uri_references, isouter=True)
                .where(
                    or_(
                        DagScheduleAssetNameReference.name == asset.name,
                        DagScheduleAssetUriReference.uri == asset.uri,
                    )
                )
            )
        )

        cls.notify_asset_changed(asset=asset_model.to_public())

        Stats.incr("asset.updates")

        dags_to_queue = (
            dags_to_queue_from_asset | dags_to_queue_from_asset_alias | dags_to_queue_from_asset_ref
        )
        cls._queue_dagruns(asset_id=asset_model.id, dags_to_queue=dags_to_queue, session=session)
        return asset_event

    @staticmethod
    def notify_asset_created(asset: Asset):
        """Run applicable notification actions when an asset is created."""
        try:
            get_listener_manager().hook.on_asset_created(asset=asset)
        except Exception:
            log.exception("error calling listener")

    @staticmethod
    def notify_asset_alias_created(asset_assets: AssetAlias):
        """Run applicable notification actions when an asset alias is created."""
        try:
            get_listener_manager().hook.on_asset_alias_created(asset_alias=asset_assets)
        except Exception:
            log.exception("error calling listener")

    @staticmethod
    def notify_asset_changed(asset: Asset):
        """Run applicable notification actions when an asset is changed."""
        try:
            get_listener_manager().hook.on_asset_changed(asset=asset)
        except Exception:
            log.exception("error calling listener")

    @classmethod
    def _queue_dagruns(cls, asset_id: int, dags_to_queue: set[DagModel], session: Session) -> None:
        # Possible race condition: if multiple dags or multiple (usually
        # mapped) tasks update the same asset, this can fail with a unique
        # constraint violation.
        #
        # If we support it, use ON CONFLICT to do nothing, otherwise
        # "fallback" to running this in a nested transaction. This is needed
        # so that the adding of these rows happens in the same transaction
        # where `ti.state` is changed.
        if not dags_to_queue:
            return

        if session.bind.dialect.name == "postgresql":
            return cls._postgres_queue_dagruns(asset_id, dags_to_queue, session)
        return cls._slow_path_queue_dagruns(asset_id, dags_to_queue, session)

    @classmethod
    def _slow_path_queue_dagruns(cls, asset_id: int, dags_to_queue: set[DagModel], session: Session) -> None:
        def _queue_dagrun_if_needed(dag: DagModel) -> str | None:
            item = AssetDagRunQueue(target_dag_id=dag.dag_id, asset_id=asset_id)
            # Don't error whole transaction when a single RunQueue item conflicts.
            # https://docs.sqlalchemy.org/en/14/orm/session_transaction.html#using-savepoint
            try:
                with session.begin_nested():
                    session.merge(item)
            except exc.IntegrityError:
                cls.logger().debug("Skipping record %s", item, exc_info=True)
            return dag.dag_id

        queued_results = (_queue_dagrun_if_needed(dag) for dag in dags_to_queue)
        if queued_dag_ids := [r for r in queued_results if r is not None]:
            cls.logger().debug("consuming dag ids %s", queued_dag_ids)

    @classmethod
    def _postgres_queue_dagruns(cls, asset_id: int, dags_to_queue: set[DagModel], session: Session) -> None:
        from sqlalchemy.dialects.postgresql import insert

        values = [{"target_dag_id": dag.dag_id} for dag in dags_to_queue]
        stmt = insert(AssetDagRunQueue).values(asset_id=asset_id).on_conflict_do_nothing()
        session.execute(stmt, values)


def resolve_asset_manager() -> AssetManager:
    """Retrieve the asset manager."""
    _asset_manager_class = conf.getimport(
        section="core",
        key="asset_manager_class",
        fallback="airflow.assets.manager.AssetManager",
    )
    _asset_manager_kwargs = conf.getjson(
        section="core",
        key="asset_manager_kwargs",
        fallback={},
    )
    return _asset_manager_class(**_asset_manager_kwargs)


asset_manager = resolve_asset_manager()
