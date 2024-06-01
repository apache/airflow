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

import ydb
import ydb.iam.auth as auth
from typing import TYPE_CHECKING, Any
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.models.connection import Connection


def get_credentials_from_connection(
    endpoint: str, database: str, connection: Connection, connection_extra: dict[str:Any] = {}
) -> Any:
    """
    Return YDB credentials object for YDB SDK based on connection settings.

    Credentials will be used with this priority:

    * login
    * token
    * service_account_json_path
    * service_account_json
    * use_vm_metadata
    * anonymous

    :param endpoint: address of YDB cluster, e.g. grpcs://my-server.com:2135
    :param database: YDB database name, e.g. /local
    :param connection: connection object
    :param connection_extra: connection extra settings
    :return: YDB credentials object
    """
    if connection.login:
        driver_config = ydb.DriverConfig(
            endpoint=endpoint,
            database=database,
        )

        return ydb.StaticCredentials(driver_config, user=connection.login, password=connection.password)

    token = connection_extra.get("token")
    if token:
        return ydb.AccessTokenCredentials(token)

    service_account_json_path = connection_extra.get("service_account_json_path")
    if service_account_json_path:
        return auth.BaseJWTCredentials.from_file(auth.ServiceAccountCredentials, service_account_json_path)

    service_account_json = connection_extra.get("service_account_json")
    if service_account_json:
        raise AirflowException("service_account_json parameter is not supported yet")

    use_vm_metadata = connection_extra.get("use_vm_metadata", False)
    if use_vm_metadata:
        return auth.MetadataUrlCredentials()

    return ydb.AnonymousCredentials()
