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
from typing import TYPE_CHECKING

from sqlalchemy.orm.session import Session

from airflow.configuration import conf
from airflow.datasets import Dataset
from airflow.models.dataset import DatasetDagRunQueue, DatasetEvent, DatasetModel
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


class DatasetEventManager(LoggingMixin):
    """
    A pluggable class that manages operations for dataset events.

    The intent is to have one place to handle all DatasetEvent-related operations, so different
    Airflow deployments can use plugins that broadcast dataset events to each other.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def register_dataset_change(
        self, *, task_instance: "TaskInstance", dataset: Dataset, extra=None, session: Session, **kwargs
    ) -> None:
        """
        For local datasets, look them up, record the dataset event, queue dagruns, and broadcast
        the dataset event
        """
        dataset_model = session.query(DatasetModel).filter(DatasetModel.uri == dataset.uri).one_or_none()
        if not dataset_model:
            self.log.warning("DatasetModel %s not found", dataset_model)
            return
        session.add(
            DatasetEvent(
                dataset_id=dataset_model.id,
                source_task_id=task_instance.task_id,
                source_dag_id=task_instance.dag_id,
                source_run_id=task_instance.run_id,
                source_map_index=task_instance.map_index,
                extra=extra,
            )
        )
        self._queue_dagruns(dataset_model, session)

    def _queue_dagruns(self, dataset: DatasetModel, session: Session) -> None:
        consuming_dag_ids = [x.dag_id for x in dataset.consuming_dags]
        self.log.debug("consuming dag ids %s", consuming_dag_ids)
        for dag_id in consuming_dag_ids:
            session.merge(DatasetDagRunQueue(dataset_id=dataset.id, target_dag_id=dag_id))


def resolve_dataset_event_manager():
    _dataset_event_manager_class = conf.getimport(
        section='core',
        key='dataset_event_manager_class',
        fallback='airflow.datasets.manager.DatasetEventManager',
    )
    _dataset_event_manager_kwargs = conf.getjson(
        section='core',
        key='dataset_event_manager_kwargs',
        fallback={},
    )
    return _dataset_event_manager_class(**_dataset_event_manager_kwargs)


dataset_event_manager = resolve_dataset_event_manager()
