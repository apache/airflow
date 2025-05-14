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

from fastapi import HTTPException, status

from airflow.exceptions import DeserializationError

from airflow.api_fastapi.common.dagbag import DagBagDep

if TYPE_CHECKING:
    from airflow.models.dag import DAG


def get_dag_from_dag_bag(dag_bag: DagBagDep, dag_id: str) -> DAG | None:
    """
    Retrieve a DAG from dag_bag with consistent error handling.

    Raises:
        HTTPException: with appropriate status code and message on error.
    """
    try:
        return dag_bag.get_dag(dag_id)

    except DeserializationError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"An unexpected error occurred while trying to deserialize DAG '{dag_id}'.",
        )
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while processing DAG '{dag_id}': {str(err)}",
        )
