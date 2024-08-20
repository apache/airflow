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
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel as BaseModelPydantic, ConfigDict


class DagScheduleAssetReferencePydantic(BaseModelPydantic):
    """Serializable version of the DagScheduleAssetReference ORM SqlAlchemyModel used by internal API."""

    dataset_id: int
    dag_id: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class TaskOutletAssetReferencePydantic(BaseModelPydantic):
    """Serializable version of the TaskOutletAssetReference ORM SqlAlchemyModel used by internal API."""

    dataset_id: int
    dag_id: str
    task_id: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class DatasetPydantic(BaseModelPydantic):
    """Serializable representation of the Dataset ORM SqlAlchemyModel used by internal API."""

    id: int
    uri: str
    extra: Optional[dict]
    created_at: datetime
    updated_at: datetime
    is_orphaned: bool

    consuming_dags: List[DagScheduleAssetReferencePydantic]
    producing_tasks: List[TaskOutletAssetReferencePydantic]

    model_config = ConfigDict(from_attributes=True)


class DatasetEventPydantic(BaseModelPydantic):
    """Serializable representation of the DatasetEvent ORM SqlAlchemyModel used by internal API."""

    id: int
    dataset_id: Optional[int]
    extra: dict
    source_task_id: Optional[str]
    source_dag_id: Optional[str]
    source_run_id: Optional[str]
    source_map_index: Optional[int]
    timestamp: datetime
    dataset: Optional[DatasetPydantic]

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)
