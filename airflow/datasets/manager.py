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

from typing import TYPE_CHECKING

from sqlalchemy import exc, select
from sqlalchemy.orm import joinedload

from airflow.configuration import conf
from airflow.datasets import Dataset
from airflow.listeners.listener import get_listener_manager
from airflow.models.dataset import DatasetDagRunQueue, DatasetEvent, DatasetModel
from airflow.stats import Stats
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

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

    def register_dataset_change(
        self,
        *,
        task_instance: TaskInstance | None = None,
        dataset: Dataset,
        extra=None,
        session: Session,
        **kwargs,
    ) -> DatasetEvent | None:
        """
        Register dataset related changes.

        For local datasets, look them up, record the dataset event, queue dagruns, and broadcast
        the dataset event
        """
        dataset_model = session.scalar(
            select(DatasetModel)
            .where(DatasetModel.uri == dataset.uri)
            .options(joinedload(DatasetModel.consuming_dags))
        )
        if not dataset_model:
            self.log.warning("DatasetModel %s not found", dataset)
            return

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
        session.flush()

        self.notify_dataset_changed(dataset=dataset)

        Stats.incr("dataset.updates")
        if dataset_model.consuming_dags:
            self._queue_dagruns(dataset_model, session)
        session.flush()
        return dataset_event

    def notify_dataset_created(self, dataset: Dataset):
        """Run applicable notification actions when a dataset is created."""
        get_listener_manager().hook.on_dataset_created(dataset=dataset)

    def notify_dataset_changed(self, dataset: Dataset):
        """Run applicable notification actions when a dataset is changed."""
        get_listener_manager().hook.on_dataset_changed(dataset=dataset)

    def _queue_dagruns(self, dataset: DatasetModel, session: Session) -> None:
        # Possible race condition: if multiple dags or multiple (usually
        # mapped) tasks update the same dataset, this can fail with a unique
        # constraint violation.
        #
        # If we support it, use ON CONFLICT to do nothing, otherwise
        # "fallback" to running this in a nested transaction. This is needed
        # so that the adding of these rows happens in the same transaction
        # where `ti.state` is changed.

        if session.bind.dialect.name == "postgresql":
            return self._postgres_queue_dagruns(dataset, session)
        return self._slow_path_queue_dagruns(dataset, session)

    def _slow_path_queue_dagruns(self, dataset: DatasetModel, session: Session) -> None:
        consuming_dag_ids = [x.dag_id for x in dataset.consuming_dags]
        self.log.debug("consuming dag ids %s", consuming_dag_ids)

        # Don't error whole transaction when a single RunQueue item conflicts.
        # https://docs.sqlalchemy.org/en/14/orm/session_transaction.html#using-savepoint
        for dag_id in consuming_dag_ids:
            item = DatasetDagRunQueue(target_dag_id=dag_id, dataset_id=dataset.id)
            try:
                with session.begin_nested():
                    session.merge(item)
            except exc.IntegrityError:
                self.log.debug("Skipping record %s", item, exc_info=True)

    def _postgres_queue_dagruns(self, dataset: DatasetModel, session: Session) -> None:
        from sqlalchemy.dialects.postgresql import insert

        stmt = insert(DatasetDagRunQueue).values(dataset_id=dataset.id).on_conflict_do_nothing()
        session.execute(
            stmt,
            [{"target_dag_id": target_dag.dag_id} for target_dag in dataset.consuming_dags],
        )


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
