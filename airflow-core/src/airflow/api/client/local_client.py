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
from datetime import datetime
from typing import Any

import httpx
from airflowctl.api.client import ClientKind as AirflowCtlClientKind, ServerResponseError, provide_api_client
from airflowctl.api.datamodels.generated import PoolBody, TriggerDAGRunPostBody
from airflowctl.exceptions import AirflowCtlConnectionException
from pydantic import BaseModel, ConfigDict, Field, ValidationError as PydanticValidationError

from airflow.api.common import delete_dag, trigger_dag
from airflow.api_fastapi.app import get_auth_manager, init_auth_manager
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.configuration import conf
from airflow.exceptions import AirflowBadRequest, PoolNotFound
from airflow.models.pool import Pool
from airflow.utils.platform import getuser
from airflow.utils.types import DagRunTriggeredByType


class LocalDagRunResponse(BaseModel):
    """Dag Run response returned by the local fallback client."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    conf: dict[str, Any] | None
    dag_id: str
    dag_run_id: str = Field(validation_alias="run_id")
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    end_date: datetime | None
    last_scheduling_decision: datetime | None
    logical_date: datetime | None
    run_type: str
    start_date: datetime | None
    state: str
    triggering_user_name: str | None


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
        try:
            parsed_conf = json.loads(conf) if isinstance(conf, str) else conf
            dag_run = api_client.dags.trigger(
                dag_id,
                TriggerDAGRunPostBody(dag_run_id=run_id, conf=parsed_conf, logical_date=logical_date),
            )
            return dag_run.model_dump()
        except (AirflowCtlConnectionException, PydanticValidationError):
            pass

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
            return LocalDagRunResponse.model_validate(dag_run).model_dump()
        return None

    @provide_api_client(kind=AirflowCtlClientKind.CLI)
    def delete_dag(self, dag_id, api_client=None):
        try:
            api_client.dags.delete(dag_id)
            return f"Deleted DAG {dag_id}"
        except ServerResponseError as err:
            raise AirflowBadRequest(self._error_detail(err)) from err
        except AirflowCtlConnectionException:
            pass

        count = delete_dag.delete_dag(dag_id)
        return f"Removed {count} record(s)"

    @provide_api_client(kind=AirflowCtlClientKind.CLI)
    def get_pool(self, name, api_client=None):
        try:
            pool = api_client.pools.get(name)
            return pool.name, pool.slots, pool.description, pool.include_deferred
        except ServerResponseError as err:
            if err.response.status_code == 404:
                raise PoolNotFound(f"Pool {name} not found") from err
            raise AirflowBadRequest(self._error_detail(err)) from err
        except AirflowCtlConnectionException:
            pass

        pool = Pool.get_pool(pool_name=name)
        if not pool:
            raise PoolNotFound(f"Pool {name} not found")
        return pool.pool, pool.slots, pool.description, pool.include_deferred

    @provide_api_client(kind=AirflowCtlClientKind.CLI)
    def get_pools(self, api_client=None):
        try:
            pools = api_client.pools.list()
            return [(pool.name, pool.slots, pool.description, pool.include_deferred) for pool in pools.pools]
        except AirflowCtlConnectionException:
            pass

        return [(p.pool, p.slots, p.description, p.include_deferred) for p in Pool.get_pools()]

    @provide_api_client(kind=AirflowCtlClientKind.CLI)
    def create_pool(self, name, slots, description, include_deferred, api_client=None):
        try:
            pool = api_client.pools.create(
                PoolBody(
                    name=name,
                    slots=int(slots),
                    description=description,
                    include_deferred=include_deferred,
                )
            )
            return pool.name, pool.slots, pool.description
        except ValueError as err:
            raise AirflowBadRequest(f"Invalid value for `slots`: {slots}") from err
        except ServerResponseError as err:
            raise AirflowBadRequest(self._error_detail(err)) from err
        except AirflowCtlConnectionException:
            pass

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
        try:
            api_client.pools.delete(name)
            return name, None, None
        except ServerResponseError as err:
            if err.response.status_code == 404:
                raise PoolNotFound(f"Pool {name} not found") from err
            raise AirflowBadRequest(self._error_detail(err)) from err
        except AirflowCtlConnectionException:
            pass

        pool = Pool.delete_pool(name=name)
        return pool.pool, pool.slots, pool.description
