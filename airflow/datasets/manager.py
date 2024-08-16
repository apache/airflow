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

from collections.abc import Iterable
from typing import TYPE_CHECKING

from sqlalchemy import exc, select
from sqlalchemy.orm import joinedload

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.configuration import conf
from airflow.datasets import Dataset
from airflow.listeners.listener import get_listener_manager
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.models.dataset import (
    DagScheduleDatasetAliasReference,
    DagScheduleDatasetReference,
    DatasetAliasModel,
    DatasetDagRunQueue,
    DatasetEvent,
    DatasetModel,
)
from airflow.stats import Stats
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models.dag import DagModel
    from airflow.models.taskinstance import TaskInstance


class DatasetManager(LoggingMixin):
    """
    A pluggable class that manages operations for datasets.

    The intent is to have one place to handle all Dataset-related operations, so different
    Airflow deployments can use plugins that broadcast dataset events to each other.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def create_datasets(self, dataset_models: list[DatasetModel], session: Session) -> None:
        """Create new datasets."""
        for dataset_model in dataset_models:
            session.add(dataset_model)
        session.flush()

        for dataset_model in dataset_models:
            self.notify_dataset_created(dataset=Dataset(uri=dataset_model.uri, extra=dataset_model.extra))

    @classmethod
    @internal_api_call
    @provide_session
    def register_dataset_change(
        cls,
        *,
        task_instance: TaskInstance | None = None,
        dataset: Dataset,
        extra=None,
        session: Session = NEW_SESSION,
        source_alias_names: Iterable[str] | None = None,
        **kwargs,
    ) -> DatasetEvent | None:
        """
        Register dataset related changes.

        For local datasets, look them up, record the dataset event, queue dagruns, and broadcast
        the dataset event
        """
        # todo: add test so that all usages of internal_api_call are added to rpc endpoint
        dataset_model = session.scalar(
            select(DatasetModel)
            .where(DatasetModel.uri == dataset.uri)
            .options(joinedload(DatasetModel.consuming_dags).joinedload(DagScheduleDatasetReference.dag))
        )
        if not dataset_model:
            cls.logger().warning("DatasetModel %s not found", dataset)
            return None

        event_kwargs = {
            "dataset_id": dataset_model.id,
            "extra": extra,
        }
        if task_instance:
            event_kwargs.update(
                {
                    "source_task_id": task_instance.task_id,
                    "source_dag_id": task_instance.dag_id,
                    "source_run_id": task_instance.run_id,
                    "source_map_index": task_instance.map_index,
                }
            )

        dataset_event = DatasetEvent(**event_kwargs)
        session.add(dataset_event)

        dags_to_queue_from_dataset = {
            ref.dag for ref in dataset_model.consuming_dags if ref.dag.is_active and not ref.dag.is_paused
        }
        dags_to_queue_from_dataset_alias = set()
        if source_alias_names:
            dataset_alias_models = session.scalars(
                select(DatasetAliasModel)
                .where(DatasetAliasModel.name.in_(source_alias_names))
                .options(
                    joinedload(DatasetAliasModel.consuming_dags).joinedload(
                        DagScheduleDatasetAliasReference.dag
                    )
                )
            ).unique()

            for dsa in dataset_alias_models:
                dsa.dataset_events.append(dataset_event)
                session.add(dsa)

                dags_to_queue_from_dataset_alias |= {
                    alias_ref.dag
                    for alias_ref in dsa.consuming_dags
                    if alias_ref.dag.is_active and not alias_ref.dag.is_paused
                }

        dags_to_reparse = dags_to_queue_from_dataset_alias - dags_to_queue_from_dataset
        if dags_to_reparse:
            file_locs = {dag.fileloc for dag in dags_to_reparse}
            cls._send_dag_priority_parsing_request(file_locs, session)
        session.flush()

        cls.notify_dataset_changed(dataset=dataset)

        Stats.incr("dataset.updates")

        dags_to_queue = dags_to_queue_from_dataset | dags_to_queue_from_dataset_alias
        cls._queue_dagruns(dataset_id=dataset_model.id, dags_to_queue=dags_to_queue, session=session)
        session.flush()
        return dataset_event

    def notify_dataset_created(self, dataset: Dataset):
        """Run applicable notification actions when a dataset is created."""
        get_listener_manager().hook.on_dataset_created(dataset=dataset)

    @classmethod
    def notify_dataset_changed(cls, dataset: Dataset):
        """Run applicable notification actions when a dataset is changed."""
        get_listener_manager().hook.on_dataset_changed(dataset=dataset)

    @classmethod
    def _queue_dagruns(cls, dataset_id: int, dags_to_queue: set[DagModel], session: Session) -> None:
        # Possible race condition: if multiple dags or multiple (usually
        # mapped) tasks update the same dataset, this can fail with a unique
        # constraint violation.
        #
        # If we support it, use ON CONFLICT to do nothing, otherwise
        # "fallback" to running this in a nested transaction. This is needed
        # so that the adding of these rows happens in the same transaction
        # where `ti.state` is changed.
        if not dags_to_queue:
            return

        if session.bind.dialect.name == "postgresql":
            return cls._postgres_queue_dagruns(dataset_id, dags_to_queue, session)
        return cls._slow_path_queue_dagruns(dataset_id, dags_to_queue, session)

    @classmethod
    def _slow_path_queue_dagruns(
        cls, dataset_id: int, dags_to_queue: set[DagModel], session: Session
    ) -> None:
        def _queue_dagrun_if_needed(dag: DagModel) -> str | None:
            item = DatasetDagRunQueue(target_dag_id=dag.dag_id, dataset_id=dataset_id)
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
    def _postgres_queue_dagruns(cls, dataset_id: int, dags_to_queue: set[DagModel], session: Session) -> None:
        from sqlalchemy.dialects.postgresql import insert

        values = [{"target_dag_id": dag.dag_id} for dag in dags_to_queue]
        stmt = insert(DatasetDagRunQueue).values(dataset_id=dataset_id).on_conflict_do_nothing()
        session.execute(stmt, values)

    @classmethod
    def _send_dag_priority_parsing_request(cls, file_locs: Iterable[str], session: Session) -> None:
        if session.bind.dialect.name == "postgresql":
            return cls._postgres_send_dag_priority_parsing_request(file_locs, session)
        return cls._slow_path_send_dag_priority_parsing_request(file_locs, session)

    @classmethod
    def _slow_path_send_dag_priority_parsing_request(cls, file_locs: Iterable[str], session: Session) -> None:
        def _send_dag_priority_parsing_request_if_needed(fileloc: str) -> str | None:
            # Don't error whole transaction when a single DagPriorityParsingRequest item conflicts.
            # https://docs.sqlalchemy.org/en/14/orm/session_transaction.html#using-savepoint
            req = DagPriorityParsingRequest(fileloc=fileloc)
            try:
                with session.begin_nested():
                    session.merge(req)
            except exc.IntegrityError:
                cls.logger().debug("Skipping request %s, already present", req, exc_info=True)
                return None
            return req.fileloc

        (_send_dag_priority_parsing_request_if_needed(fileloc) for fileloc in file_locs)

    @classmethod
    def _postgres_send_dag_priority_parsing_request(cls, file_locs: Iterable[str], session: Session) -> None:
        from sqlalchemy.dialects.postgresql import insert

        stmt = insert(DagPriorityParsingRequest).on_conflict_do_nothing()
        session.execute(stmt, {"fileloc": fileloc for fileloc in file_locs})


def resolve_dataset_manager() -> DatasetManager:
    """Retrieve the dataset manager."""
    _dataset_manager_class = conf.getimport(
        section="core",
        key="dataset_manager_class",
        fallback="airflow.datasets.manager.DatasetManager",
    )
    _dataset_manager_kwargs = conf.getjson(
        section="core",
        key="dataset_manager_kwargs",
        fallback={},
    )
    return _dataset_manager_class(**_dataset_manager_kwargs)


dataset_manager = resolve_dataset_manager()
