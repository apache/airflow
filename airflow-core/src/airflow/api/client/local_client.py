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
"""Local client API."""

from __future__ import annotations

import asyncio
import json
from contextlib import AsyncExitStack
from functools import cache, cached_property

import httpx
from a2wsgi import ASGIMiddleware
from airflowctl.api.client import Client as AirflowCtlClient
from airflowctl.api.client import ClientKind as AirflowCtlClientKind
from airflowctl.api.client import ServerResponseError
from airflowctl.api.datamodels.generated import PoolBody, TriggerDAGRunPostBody
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser

from airflow.api.common import delete_dag, trigger_dag
from airflow.api_fastapi.app import cached_app
from airflow.configuration import conf
from airflow.exceptions import AirflowBadRequest, PoolNotFound
from airflow.models.pool import Pool
from airflow.utils.platform import getuser
from airflow.utils.types import DagRunTriggeredByType


class _InProcessCoreAPI:
    @cached_property
    def app(self):
        return cached_app(apps="core")

    @cached_property
    def transport(self) -> httpx.WSGITransport:
        middleware = ASGIMiddleware(self.app)

        async def start_lifespan(cm: AsyncExitStack):
            await cm.enter_async_context(self.app.router.lifespan_context(self.app))

        self._cm = AsyncExitStack()
        asyncio.run_coroutine_threadsafe(start_lifespan(self._cm), middleware.loop)
        return httpx.WSGITransport(app=middleware)


@cache
def _in_process_core_api() -> _InProcessCoreAPI:
    return _InProcessCoreAPI()


class Client:
    """Local API client implementation."""

    def __init__(self, auth=None, session: httpx.Client | None = None):
        self._session: httpx.Client = session or httpx.Client()
        if auth:
            self._session.auth = auth
        self._airflowctl_client = self._create_airflowctl_client()

    def _create_airflowctl_client(self) -> AirflowCtlClient | None:
        app = _in_process_core_api().app
        auth_manager = app.state.auth_manager
        token = auth_manager.generate_jwt(
            user=SimpleAuthManagerUser(username=getuser(), role="admin"),
            expiration_time_in_seconds=conf.getint("api_auth", "jwt_cli_expiration_time"),
        )
        return AirflowCtlClient(
            base_url=conf.get("api", "base_url", fallback="http://localhost:8080") or "http://localhost:8080",
            token=token,
            kind=AirflowCtlClientKind.CLI,
            transport=_in_process_core_api().transport,
        )

    @staticmethod
    def _error_detail(err: ServerResponseError) -> str:
        try:
            payload = err.response.json()
        except ValueError:
            return str(err)
        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, str):
                return detail
        return str(payload)

    def trigger_dag(
        self,
        dag_id,
        run_id=None,
        conf=None,
        logical_date=None,
        triggering_user_name=None,
        replace_microseconds=True,
    ) -> dict | None:
        if self._airflowctl_client is not None:
            parsed_conf = conf
            if isinstance(conf, str):
                parsed_conf = json.loads(conf)
            dag_run = self._airflowctl_client.dags.trigger(
                dag_id,
                TriggerDAGRunPostBody(
                    dag_run_id=run_id,
                    conf=parsed_conf,
                    logical_date=logical_date,
                ),
            )
            return {
                "conf": dag_run.conf,
                "dag_id": dag_run.dag_id,
                "dag_run_id": dag_run.dag_run_id,
                "data_interval_start": dag_run.data_interval_start,
                "data_interval_end": dag_run.data_interval_end,
                "end_date": dag_run.end_date,
                "last_scheduling_decision": dag_run.last_scheduling_decision,
                "logical_date": dag_run.logical_date,
                "run_type": dag_run.run_type,
                "start_date": dag_run.start_date,
                "state": dag_run.state,
                "triggering_user_name": dag_run.triggering_user_name,
            }
        dag_run = trigger_dag.trigger_dag(
            dag_id=dag_id,
            triggered_by=DagRunTriggeredByType.CLI,
            triggering_user_name=triggering_user_name,
            run_id=run_id,
            conf=conf,
            logical_date=logical_date,
            replace_microseconds=replace_microseconds,
        )
        if dag_run:
            return {
                "conf": dag_run.conf,
                "dag_id": dag_run.dag_id,
                "dag_run_id": dag_run.run_id,
                "data_interval_start": dag_run.data_interval_start,
                "data_interval_end": dag_run.data_interval_end,
                "end_date": dag_run.end_date,
                "last_scheduling_decision": dag_run.last_scheduling_decision,
                "logical_date": dag_run.logical_date,
                "run_type": dag_run.run_type,
                "start_date": dag_run.start_date,
                "state": dag_run.state,
                "triggering_user_name": dag_run.triggering_user_name,
            }
        return dag_run

    def delete_dag(self, dag_id):
        if self._airflowctl_client is not None:
            try:
                self._airflowctl_client.dags.delete(dag_id)
            except ServerResponseError as err:
                raise AirflowBadRequest(self._error_detail(err)) from err
            return f"Deleted DAG {dag_id}"
        count = delete_dag.delete_dag(dag_id)
        return f"Removed {count} record(s)"

    def get_pool(self, name):
        if self._airflowctl_client is not None:
            try:
                pool = self._airflowctl_client.pools.get(name)
            except ServerResponseError as err:
                if err.response.status_code == 404:
                    raise PoolNotFound(f"Pool {name} not found") from err
                raise AirflowBadRequest(self._error_detail(err)) from err
            return pool.name, pool.slots, pool.description, pool.include_deferred
        pool = Pool.get_pool(pool_name=name)
        if not pool:
            raise PoolNotFound(f"Pool {name} not found")
        return pool.pool, pool.slots, pool.description, pool.include_deferred

    def get_pools(self):
        if self._airflowctl_client is not None:
            pools = self._airflowctl_client.pools.list()
            return [(pool.name, pool.slots, pool.description, pool.include_deferred) for pool in pools.pools]
        return [(p.pool, p.slots, p.description, p.include_deferred) for p in Pool.get_pools()]

    def create_pool(self, name, slots, description, include_deferred):
        if self._airflowctl_client is not None:
            try:
                pool = self._airflowctl_client.pools.create(
                    PoolBody(
                        name=name,
                        slots=int(slots),
                        description=description,
                        include_deferred=include_deferred,
                    )
                )
            except ValueError as err:
                raise AirflowBadRequest(f"Invalid value for `slots`: {slots}") from err
            except ServerResponseError as err:
                raise AirflowBadRequest(self._error_detail(err)) from err
            return pool.name, pool.slots, pool.description
        if not (name and name.strip()):
            raise AirflowBadRequest("Pool name shouldn't be empty")
        pool_name_length = Pool.pool.property.columns[0].type.length
        if len(name) > pool_name_length:
            raise AirflowBadRequest(f"Pool name cannot be more than {pool_name_length} characters")
        try:
            slots = int(slots)
        except ValueError:
            raise AirflowBadRequest(f"Invalid value for `slots`: {slots}")
        pool = Pool.create_or_update_pool(
            name=name, slots=slots, description=description, include_deferred=include_deferred
        )
        return pool.pool, pool.slots, pool.description

    def delete_pool(self, name):
        if self._airflowctl_client is not None:
            try:
                self._airflowctl_client.pools.delete(name)
            except ServerResponseError as err:
                if err.response.status_code == 404:
                    raise PoolNotFound(f"Pool {name} not found") from err
                raise AirflowBadRequest(self._error_detail(err)) from err
            return name, None, None
        pool = Pool.delete_pool(name=name)
        return pool.pool, pool.slots, pool.description
