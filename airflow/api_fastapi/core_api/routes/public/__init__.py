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

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.routes.public.connections import connections_router
from airflow.api_fastapi.core_api.routes.public.dag_run import dag_run_router
from airflow.api_fastapi.core_api.routes.public.dag_warning import dag_warning_router
from airflow.api_fastapi.core_api.routes.public.dags import dags_router
from airflow.api_fastapi.core_api.routes.public.monitor import monitor_router
from airflow.api_fastapi.core_api.routes.public.variables import variables_router

public_router = AirflowRouter(prefix="/public")


public_router.include_router(dags_router)
public_router.include_router(connections_router)
public_router.include_router(variables_router)
public_router.include_router(dag_run_router)
public_router.include_router(monitor_router)
public_router.include_router(dag_warning_router)
