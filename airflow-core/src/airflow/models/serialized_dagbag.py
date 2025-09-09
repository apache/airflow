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

from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.serialization.serialized_objects import SerializedDAG


class SerializedDagBag:
    """
    A lightweight in-memory cache for Serialized DAGs, similar to DagBag, but loading DAGs from SerializedDagModel instead of files.
    """

    def __init__(self) -> None:
        super().__init__()
        self._serialized_dags: dict[str, SerializedDagModel] = {}

    @provide_session
    def get_dag_model(
        self, dag_id: str, dag_version_id: str, session: Session = NEW_SESSION
    ) -> SerializedDagModel | None:
        """
        Return the serialized DagModel with the given dag_id and dag_version_id.

        First checks the in-memory cache, then loads the serialized DagModel from the metadata DB if not cached.
        """
        dag_key = self._dag_key(dag_id=dag_id, dag_version_id=dag_version_id)

        if dag_key in self._serialized_dags:
            return self._serialized_dags[dag_key]

        serialized_dag = SerializedDagModel.get_serialized_dag_by_version(
            dag_id=dag_id, dag_version_id=dag_version_id, session=session
        )

        if serialized_dag:
            self._serialized_dags[dag_key] = serialized_dag
            return serialized_dag
        return None

    @provide_session
    def get_dag(
        self, dag_id: str, dag_version_id: str, session: Session = NEW_SESSION
    ) -> SerializedDAG | None:
        """
        Return the serialized DAG with the given dag_id and dag_version_id.

        First checks the in-memory cache, then loads the serialized DAG from the metadata DB if not cached.
        """
        serialized_dag = self.get_dag_model(dag_id=dag_id, dag_version_id=dag_version_id, session=session)

        if serialized_dag:
            return serialized_dag.dag
        return None

    @classmethod
    def _dag_key(cls, dag_id: str, dag_version_id: str) -> str:
        return f"{dag_id}.{dag_version_id}"

    def __contains__(self, key: tuple[str, str]) -> bool:
        dag_id, dag_version_id = key
        return self._dag_key(dag_id=dag_id, dag_version_id=dag_version_id) in self._serialized_dags

    def __len__(self) -> int:
        return len(self._serialized_dags)
