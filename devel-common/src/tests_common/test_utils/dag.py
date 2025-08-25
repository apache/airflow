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

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.sdk import DAG
    from airflow.serialization.serialized_objects import SerializedDAG


def create_scheduler_dag(dag: DAG | SerializedDAG) -> SerializedDAG:
    from airflow.serialization.serialized_objects import SerializedDAG

    if isinstance(dag, SerializedDAG):
        return dag
    return SerializedDAG.deserialize_dag(SerializedDAG.serialize_dag(dag))


@provide_session
def sync_dag_to_db(dag: DAG, session: Session = NEW_SESSION) -> SerializedDAG:
    return sync_dags_to_db([dag], session=session)[0]


@provide_session
def sync_dags_to_db(dags: Iterable[DAG], session: Session = NEW_SESSION) -> Sequence[SerializedDAG]:
    """
    Sync dags into the database.

    This serializes dags and saves the results to the database. The serialized
    (scheduler-oeirnted) dags are returned. If the input is ordered (e.g. a list),
    the returned sequence is guaranteed to be in the same order.
    """
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.serialization.serialized_objects import SerializedDAG

    session.merge(DagBundleModel(name="testing"))
    session.flush()
    scheduler_dags = [create_scheduler_dag(dag) for dag in dags]
    SerializedDAG.bulk_write_to_db("testing", None, scheduler_dags, session=session)
    for dag in scheduler_dags:
        SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)
    session.flush()
    return scheduler_dags
