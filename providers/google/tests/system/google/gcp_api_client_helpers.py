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

import json
from typing import Any

from requests.exceptions import HTTPError

from tests_common.test_utils.api_client_helpers import make_authenticated_rest_api_request


def create_connection_request(connection_id: str, connection: dict[str, Any], is_composer: bool = False):
    if is_composer:
        from airflow.configuration import conf
        from airflow.providers.google.cloud.hooks.cloud_composer import CloudComposerHook

        hook = CloudComposerHook()
        composer_airflow_uri = conf.get("api", "base_url").rstrip("/")

        response = hook.make_composer_airflow_api_request(
            method="POST",
            airflow_uri=composer_airflow_uri,
            path="/api/v2/connections",
            data=json.dumps(
                {
                    "connection_id": connection_id,
                    **connection,
                }
            ),
        )
        response.raise_for_status()
        if response.text != "":
            return response.json()
    else:
        return make_authenticated_rest_api_request(
            path="/api/v2/connections",
            method="POST",
            body={
                "connection_id": connection_id,
                **connection,
            },
        )


def delete_connection_request(connection_id: str, is_composer: bool = False):
    if is_composer:
        from airflow.configuration import conf
        from airflow.providers.google.cloud.hooks.cloud_composer import CloudComposerHook

        hook = CloudComposerHook()
        composer_airflow_uri = conf.get("api", "base_url").rstrip("/")

        response = hook.make_composer_airflow_api_request(
            method="DELETE", airflow_uri=composer_airflow_uri, path=f"/api/v2/connections/{connection_id}"
        )
        response.raise_for_status()
        if response.text != "":
            return response.json()
    else:
        return make_authenticated_rest_api_request(
            path=f"/api/v2/connections/{connection_id}",
            method="DELETE",
        )


def create_airflow_connection(
    connection_id: str, connection_conf: dict[str, Any], is_composer: bool = False
) -> None:
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

    print(f"Removing connection '{connection_id}' if it exists")
    if AIRFLOW_V_3_0_PLUS:
        try:
            delete_connection_request(connection_id=connection_id, is_composer=is_composer)
        except HTTPError:
            print(f"Connection '{connection_id}' does not exist. A new one will be created")
        create_connection_request(
            connection_id=connection_id, connection=connection_conf, is_composer=is_composer
        )
    else:
        from airflow.models import Connection
        from airflow.settings import Session

        if Session is None:
            raise RuntimeError("Session not configured. Call configure_orm() first.")
        session = Session()
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()
        connection = Connection(conn_id=connection_id, **connection_conf)
        session.add(connection)
        session.commit()
    print(f"Connection '{connection_id}' created")


def delete_airflow_connection(connection_id: str, is_composer: bool = False) -> None:
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

    print(f"Removing connection '{connection_id}'")
    if AIRFLOW_V_3_0_PLUS:
        delete_connection_request(connection_id=connection_id, is_composer=is_composer)
    else:
        from airflow.models import Connection
        from airflow.settings import Session

        if Session is None:
            raise RuntimeError("Session not configured. Call configure_orm() first.")
        session = Session()
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()
        session.commit()
