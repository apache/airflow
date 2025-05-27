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

import logging

from fastapi import APIRouter, HTTPException, Query, status

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.dag_processing.collection import update_dag_parsing_results_in_db
from airflow.dag_processing.processor import DagFileParsingResult


router = APIRouter()


log = logging.getLogger(__name__)


@router.post(
    "/update_dags",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Something went wrong while updating DAGs"},
    },
)
def update_dags(
    bundle_name: str,
    bundle_version: str,
    parsing_result: DagFileParsingResult,
    session: SessionDep,
):
    """Store DAG parsing results in the database."""
    log.info("Updating DAGs for bundle %s version %s", bundle_name, bundle_version)

    update_dag_parsing_results_in_db(
        bundle_name=bundle_name,
        bundle_version=bundle_version,
        dags=parsing_result.serialized_dags,
        import_errors=parsing_result.import_errors or {},
        warnings=set(parsing_result.warnings or []),
        session=session,
    )
