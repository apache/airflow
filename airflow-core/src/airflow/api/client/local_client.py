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

import json

import httpx
from airflowctl.api.client import ClientKind as AirflowCtlClientKind, ServerResponseError, provide_api_client
from airflowctl.api.datamodels.generated import PoolBody, TriggerDAGRunPostBody

from airflow.api.common import delete_dag, trigger_dag
from airflow.api_fastapi.app import get_auth_manager, init_auth_manager
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.configuration import conf
from airflow.exceptions import AirflowBadRequest, PoolNotFound
from airflow.models.pool import Pool
from airflow.utils.platform import getuser
from airflow.utils.types import DagRunTriggeredByType


class Client:
    """Local API client implementation."""

    def __init__(self, auth=None, session: httpx.Client | None = None):
        self._session: httpx.Client = session or httpx.Client()
        if auth:
            self._session.auth = auth
        self.api_token = self._create_api_token()

    def _create_api_token(self) -> str:
        try:
            auth_manager = get_auth_manager()
        except RuntimeError:
            auth_manager = init_auth_manager()
        return auth_manager.generate_jwt(
            user=SimpleAuthManagerUser(username=getuser(), role="admin"),
            expiration_time_in_seconds=conf.getint("api_auth", "jwt_cli_expiration_time"),
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

    @provide_api_client(kind=AirflowCtlClientKind.CLI)
    def trigger_dag(
        self,
        dag_id,
        run_id=None,
        conf=None,
        logical_date=None,
        triggering_user_name=None,
        replace_microseconds=True,
        api_client=None,
    ) -> dict | None:
        if api_client is not None:
            parsed_conf = conf
            if isinstance(conf, str):
                try:
                    parsed_conf = json.loads(conf)
                except json.JSONDecodeError as err:
                    raise AirflowBadRequest(f"Invalid configuration JSON: {err}") from err
            dag_run = api_client.dags.trigger(
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

    @provide_api_client(kind=AirflowCtlClientKind.CLI)
    def delete_dag(self, dag_id, api_client=None):
        if api_client is not None:
            try:
                api_client.dags.delete(dag_id)
            except ServerResponseError as err:
                raise AirflowBadRequest(self._error_detail(err)) from err
            return f"Deleted DAG {dag_id}"
        count = delete_dag.delete_dag(dag_id)
        return f"Removed {count} record(s)"

    @provide_api_client(kind=AirflowCtlClientKind.CLI)
    def get_pool(self, name, api_client=None):
        if api_client is not None:
            try:
                pool = api_client.pools.get(name)
            except ServerResponseError as err:
                if err.response.status_code == 404:
                    raise PoolNotFound(f"Pool {name} not found") from err
                raise AirflowBadRequest(self._error_detail(err)) from err
            return pool.name, pool.slots, pool.description, pool.include_deferred
        pool = Pool.get_pool(pool_name=name)
        if not pool:
            raise PoolNotFound(f"Pool {name} not found")
        return pool.pool, pool.slots, pool.description, pool.include_deferred

    @provide_api_client(kind=AirflowCtlClientKind.CLI)
    def get_pools(self, api_client=None):
        if api_client is not None:
            pools = api_client.pools.list()
            return [(pool.name, pool.slots, pool.description, pool.include_deferred) for pool in pools.pools]
        return [(p.pool, p.slots, p.description, p.include_deferred) for p in Pool.get_pools()]

    @provide_api_client(kind=AirflowCtlClientKind.CLI)
    def create_pool(self, name, slots, description, include_deferred, api_client=None):
        if api_client is not None:
            try:
                pool = api_client.pools.create(
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

    @provide_api_client(kind=AirflowCtlClientKind.CLI)
    def delete_pool(self, name, api_client=None):
        if api_client is not None:
            try:
                api_client.pools.delete(name)
            except ServerResponseError as err:
                if err.response.status_code == 404:
                    raise PoolNotFound(f"Pool {name} not found") from err
                raise AirflowBadRequest(self._error_detail(err)) from err
            return name, None, None
        pool = Pool.delete_pool(name=name)
        return pool.pool, pool.slots, pool.description
