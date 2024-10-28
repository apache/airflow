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

import pathlib
from datetime import datetime
from typing import Any, List, Optional

from pydantic import (
    BaseModel as BaseModelPydantic,
    ConfigDict,
    PlainSerializer,
    PlainValidator,
    ValidationInfo,
)
from typing_extensions import Annotated

from airflow import DAG, settings
from airflow.configuration import conf as airflow_conf


def serialize_operator(x: DAG) -> dict:
    from airflow.serialization.serialized_objects import SerializedDAG

    return SerializedDAG.serialize_dag(x)


def validate_operator(x: DAG | dict[str, Any], _info: ValidationInfo) -> Any:
    from airflow.serialization.serialized_objects import SerializedDAG

    if isinstance(x, DAG):
        return x
    return SerializedDAG.deserialize_dag(x)


PydanticDag = Annotated[
    DAG,
    PlainValidator(validate_operator),
    PlainSerializer(serialize_operator, return_type=dict),
]


class DagOwnerAttributesPydantic(BaseModelPydantic):
    """Serializable representation of the DagOwnerAttributes ORM SqlAlchemyModel used by internal API."""

    owner: str
    link: str

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)


class DagTagPydantic(BaseModelPydantic):
    """Serializable representation of the DagTag ORM SqlAlchemyModel used by internal API."""

    name: str
    dag_id: str

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)


class DagModelPydantic(BaseModelPydantic):
    """Serializable representation of the DagModel ORM SqlAlchemyModel used by internal API."""

    dag_id: str
    is_paused_at_creation: bool = airflow_conf.getboolean(
        "core", "dags_are_paused_at_creation"
    )
    is_paused: bool = is_paused_at_creation
    is_active: Optional[bool] = False
    last_parsed_time: Optional[datetime]
    last_pickled: Optional[datetime]
    last_expired: Optional[datetime]
    pickle_id: Optional[int]
    fileloc: str
    processor_subdir: Optional[str]
    owners: Optional[str]
    description: Optional[str]
    default_view: Optional[str]
    timetable_summary: Optional[str]
    timetable_description: Optional[str]
    tags: List[DagTagPydantic]  # noqa: UP006
    dag_owner_links: List[DagOwnerAttributesPydantic]  # noqa: UP006

    max_active_tasks: int
    max_active_runs: Optional[int]
    max_consecutive_failed_dag_runs: Optional[int]

    has_task_concurrency_limits: bool
    has_import_errors: Optional[bool] = False

    _processor_dags_folder: Optional[str] = None

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    @property
    def relative_fileloc(self) -> pathlib.Path:
        """File location of the importable dag 'file' relative to the configured DAGs folder."""
        path = pathlib.Path(self.fileloc)
        try:
            rel_path = path.relative_to(
                self._processor_dags_folder or settings.DAGS_FOLDER
            )
            if rel_path == pathlib.Path("."):
                return path
            else:
                return rel_path
        except ValueError:
            # Not relative to DAGS_FOLDER.
            return path
