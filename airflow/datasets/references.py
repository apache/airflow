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

"""
Dataset reference objects.

These are intermediate representations of DAG- and task-level references to
Dataset and DatasetAlias. These are meant only for Airflow internals so the DAG
processor can collect information from DAGs without the "full picture", which is
only available when it updates the database.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from airflow.datasets import Dataset, DatasetAlias
    from airflow.models.dataset import (
        DagScheduleDatasetAliasReference,
        DagScheduleDatasetReference,
        DatasetAliasModel,
        DatasetModel,
        TaskOutletDatasetReference,
    )

DatasetReference = Union["DatasetNameReference", "DatasetURIReference"]

DatasetOrAliasReference = Union[DatasetReference, "DatasetAliasReference"]


def create_dag_dataset_reference(source: Dataset) -> DatasetReference:
    """Create reference to a dataset."""
    if source.name:
        return DatasetNameReference(source.name)
    return DatasetURIReference(source.uri)


def create_dag_dataset_alias_reference(source: DatasetAlias) -> DatasetAliasReference:
    """Create reference to a dataset or dataset alias."""
    return DatasetAliasReference(source.name)


@dataclasses.dataclass
class DatasetNameReference:
    """Reference to a dataset by name."""

    name: str

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.name))


@dataclasses.dataclass
class DatasetURIReference:
    """Reference to a dataset by URI."""

    uri: str

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.uri))


@dataclasses.dataclass
class DatasetAliasReference:
    """Reference to a dataset alias."""

    name: str

    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.name))


def resolve_dag_schedule_reference(
    ref: DatasetOrAliasReference,
    *,
    dag_id: str,
    dataset_names: dict[str, DatasetModel],
    dataset_uris: dict[str, DatasetModel],
    alias_names: dict[str, DatasetAliasModel],
) -> DagScheduleDatasetReference | DagScheduleDatasetAliasReference:
    """Create database representation from DAG-level references."""
    from airflow.models.dataset import DagScheduleDatasetAliasReference, DagScheduleDatasetReference

    if isinstance(ref, DatasetNameReference):
        return DagScheduleDatasetReference(dataset_id=dataset_names[ref.name].id, dag_id=dag_id)
    elif isinstance(ref, DatasetURIReference):
        return DagScheduleDatasetReference(dataset_id=dataset_uris[ref.uri].id, dag_id=dag_id)
    return DagScheduleDatasetAliasReference(alias_id=alias_names[ref.name], dag_id=dag_id)


def resolve_task_outlet_reference(
    ref: DatasetReference,
    *,
    dag_id: str,
    task_id: str,
    dataset_names: dict[str, DatasetModel],
    dataset_uris: dict[str, DatasetModel],
) -> TaskOutletDatasetReference:
    """Create database representation from task-level references."""
    from airflow.models.dataset import TaskOutletDatasetReference

    if isinstance(ref, DatasetURIReference):
        dataset = dataset_uris[ref.uri]
    else:
        dataset = dataset_names[ref.name]
    return TaskOutletDatasetReference(dataset_id=dataset.id, dag_id=dag_id, task_id=task_id)
