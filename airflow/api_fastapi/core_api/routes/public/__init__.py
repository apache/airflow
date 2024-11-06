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
from airflow.api_fastapi.core_api.routes.public.backfills import backfills_router
from airflow.api_fastapi.core_api.routes.public.connections import connections_router
from airflow.api_fastapi.core_api.routes.public.dag_run import dag_run_router
from airflow.api_fastapi.core_api.routes.public.dag_sources import dag_sources_router
from airflow.api_fastapi.core_api.routes.public.dag_stats import dag_stats_router
from airflow.api_fastapi.core_api.routes.public.dag_warning import dag_warning_router
from airflow.api_fastapi.core_api.routes.public.dags import dags_router
from airflow.api_fastapi.core_api.routes.public.event_logs import event_logs_router
from airflow.api_fastapi.core_api.routes.public.import_error import import_error_router
from airflow.api_fastapi.core_api.routes.public.monitor import monitor_router
from airflow.api_fastapi.core_api.routes.public.plugins import plugins_router
from airflow.api_fastapi.core_api.routes.public.pools import pools_router
from airflow.api_fastapi.core_api.routes.public.providers import providers_router
from airflow.api_fastapi.core_api.routes.public.task_instances import task_instances_router
from airflow.api_fastapi.core_api.routes.public.variables import variables_router
from airflow.api_fastapi.core_api.routes.public.version import version_router

public_router = AirflowRouter(prefix="/public")


public_router.include_router(backfills_router)
public_router.include_router(dags_router)
public_router.include_router(connections_router)
public_router.include_router(dag_run_router)
public_router.include_router(dag_sources_router)
public_router.include_router(dags_router)
public_router.include_router(event_logs_router)
public_router.include_router(import_error_router)
public_router.include_router(monitor_router)
public_router.include_router(dag_warning_router)
public_router.include_router(plugins_router)
public_router.include_router(pools_router)
public_router.include_router(providers_router)
public_router.include_router(task_instances_router)
public_router.include_router(variables_router)
public_router.include_router(variables_router)
public_router.include_router(version_router)
public_router.include_router(dag_stats_router)
