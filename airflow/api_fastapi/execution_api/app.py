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

from fastapi import FastAPI


def create_task_execution_api_app(app: FastAPI) -> FastAPI:
    """Create FastAPI app for task execution API."""
    from airflow.api_fastapi.execution_api.routes import execution_api_router

    task_exec_api_app = FastAPI(
        title="Airflow Task Execution API",
        description="The private Airflow Task Execution API.",
        include_in_schema=False,
    )

    task_exec_api_app.include_router(execution_api_router)
    return task_exec_api_app
