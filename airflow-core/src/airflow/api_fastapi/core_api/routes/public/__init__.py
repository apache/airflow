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

from fastapi import status

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.routes.public.assets import assets_router
from airflow.api_fastapi.core_api.routes.public.auth import auth_router
from airflow.api_fastapi.core_api.routes.public.backfills import backfills_router
from airflow.api_fastapi.core_api.routes.public.config import config_router
from airflow.api_fastapi.core_api.routes.public.connections import connections_router
from airflow.api_fastapi.core_api.routes.public.dag_parsing import dag_parsing_router
from airflow.api_fastapi.core_api.routes.public.dag_run import dag_run_router
from airflow.api_fastapi.core_api.routes.public.dag_sources import dag_sources_router
from airflow.api_fastapi.core_api.routes.public.dag_stats import dag_stats_router
from airflow.api_fastapi.core_api.routes.public.dag_tags import dag_tags_router
from airflow.api_fastapi.core_api.routes.public.dag_versions import dag_versions_router
from airflow.api_fastapi.core_api.routes.public.dag_warning import dag_warning_router
from airflow.api_fastapi.core_api.routes.public.dags import dags_router
from airflow.api_fastapi.core_api.routes.public.event_logs import event_logs_router
from airflow.api_fastapi.core_api.routes.public.extra_links import extra_links_router
from airflow.api_fastapi.core_api.routes.public.hitl import task_instances_hitl_router
from airflow.api_fastapi.core_api.routes.public.import_error import import_error_router
from airflow.api_fastapi.core_api.routes.public.job import job_router
from airflow.api_fastapi.core_api.routes.public.log import task_instances_log_router
from airflow.api_fastapi.core_api.routes.public.monitor import monitor_router
from airflow.api_fastapi.core_api.routes.public.plugins import plugins_router
from airflow.api_fastapi.core_api.routes.public.pools import pools_router
from airflow.api_fastapi.core_api.routes.public.providers import providers_router
from airflow.api_fastapi.core_api.routes.public.task_instances import task_instances_router
from airflow.api_fastapi.core_api.routes.public.tasks import tasks_router
from airflow.api_fastapi.core_api.routes.public.variables import variables_router
from airflow.api_fastapi.core_api.routes.public.version import version_router
from airflow.api_fastapi.core_api.routes.public.xcom import xcom_router

public_router = AirflowRouter(prefix="/api/v2")

# Router with common attributes for all routes
authenticated_router = AirflowRouter(
    responses=create_openapi_http_exception_doc([status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN]),
)


authenticated_router.include_router(assets_router)
authenticated_router.include_router(backfills_router)
authenticated_router.include_router(connections_router)
authenticated_router.include_router(dag_run_router)
authenticated_router.include_router(dag_sources_router)
authenticated_router.include_router(dag_stats_router)
authenticated_router.include_router(config_router)
authenticated_router.include_router(dag_warning_router)
authenticated_router.include_router(dags_router)
authenticated_router.include_router(event_logs_router)
authenticated_router.include_router(extra_links_router)
authenticated_router.include_router(import_error_router)
authenticated_router.include_router(job_router)
authenticated_router.include_router(plugins_router)
authenticated_router.include_router(pools_router)
authenticated_router.include_router(providers_router)
authenticated_router.include_router(xcom_router)
authenticated_router.include_router(task_instances_router)
authenticated_router.include_router(tasks_router)
authenticated_router.include_router(variables_router)
authenticated_router.include_router(task_instances_log_router)
authenticated_router.include_router(dag_parsing_router)
authenticated_router.include_router(dag_tags_router)
authenticated_router.include_router(dag_versions_router)
authenticated_router.include_router(task_instances_hitl_router)


# Include authenticated router in public router
public_router.include_router(authenticated_router)

# Following routers are not included in common router, for now we don't expect it to have authentication
public_router.include_router(monitor_router)
public_router.include_router(version_router)
public_router.include_router(auth_router)
