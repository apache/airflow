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

from sqlalchemy import exc
from sqlalchemy.orm import Session

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.exceptions import TaskNotFound
from airflow.models import Operator
from airflow.utils.session import NEW_SESSION, provide_session


class InternalApiDagActions:
    @staticmethod
    @internal_api_call
    @provide_session
    def get_serialized_dag(dag_id: str, task_id: str, session: Session = NEW_SESSION) -> Operator | None:
        from airflow.models.serialized_dag import SerializedDagModel

        try:
            model = session.query(SerializedDagModel).get(dag_id)
            if model:
                return model.dag.get_task(task_id)
        except (exc.NoResultFound, TaskNotFound):
            pass

        return None
