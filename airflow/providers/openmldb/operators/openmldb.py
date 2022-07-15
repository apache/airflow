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

"""This module contains OpenMLDB operators."""
from enum import Enum
from typing import TYPE_CHECKING

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.openmldb.hooks.openmldb import OpenMLDBHook
from airflow.utils.operator_helpers import determine_kwargs

if TYPE_CHECKING:
    from airflow.utils.context import Context


class Mode(Enum):
    """Available options for OpenMLDB execute mode"""

    OFFSYNC = 'offsync'
    OFFASYNC = 'offasync'
    ONLINE = 'online'


class OpenMLDBSQLOperator(BaseOperator):
    """
    This operator runs any sql on OpenMLDB

    :param db: The database you want to use
    :param mode: The execute mode, offsync, offasync, online.
    :param sql: The sql you want to deploy
    :param openmldb_conn_id: The Airflow connection used for OpenMLDB.
    :keyword disable_response_check: If true, do not do response check
    :keyword response_check: custom response check function. If None, check if the response code equals 0.
    """

    def __init__(
        self,
        db: str,
        mode: Mode,
        sql: str,
        openmldb_conn_id: str = 'openmldb_default',
        **kwargs,
    ) -> None:
        if kwargs.pop("disable_response_check", False):
            self.response_check = None
        else:
            self.response_check = kwargs.pop("response_check", lambda response: response.json()["code"] == 0)
        super().__init__(**kwargs)
        self.openmldb_conn_id = openmldb_conn_id
        self.db = db
        self.mode = mode
        self.sql = sql

    def execute(self, context: 'Context'):
        openmldb_hook = OpenMLDBHook(openmldb_conn_id=self.openmldb_conn_id)
        response = openmldb_hook.submit_job(db=self.db, mode=self.mode.value, sql=self.sql)

        if self.response_check:
            kwargs = determine_kwargs(self.response_check, [response], context)
            if not self.response_check(response, **kwargs):
                raise AirflowException(
                    f"Response check returned False. Resp: {response.text}"
                )


class OpenMLDBLoadDataOperator(OpenMLDBSQLOperator):
    """
    This operator loads data to OpenMLDB

    :param db: The database you want to use
    :param mode: The execute mode
    :param table: The table you want to load data to
    :param file: The data path you want to load, local or hdfs
    :param openmldb_conn_id: The Airflow connection used for OpenMLDB.
    :keyword options: load data options
    """

    def __init__(
        self,
        db: str,
        mode: Mode,
        table: str,
        file: str,
        openmldb_conn_id: str = 'openmldb_default',
        **kwargs,
    ) -> None:
        load_data_options = kwargs.pop('options', None)
        sql = f"LOAD DATA INFILE '{file}' INTO TABLE {table}"
        if load_data_options:
            sql += f" OPTIONS({load_data_options})"
        super().__init__(db=db, mode=mode, sql=sql, **kwargs)
        self.openmldb_conn_id = openmldb_conn_id


class OpenMLDBSelectIntoOperator(OpenMLDBSQLOperator):
    """
    This operator extracts feature from OpenMLDB and save it

    :param db: The database you want to use
    :param mode: The execute mode
    :param table: The table you want to select
    :param file: The data path you want to save features, local or hdfs
    :param openmldb_conn_id: The Airflow connection used for OpenMLDB.
    :keyword options: select into options
    """

    def __init__(
        self,
        db: str,
        mode: Mode,
        sql: str,
        file: str,
        openmldb_conn_id: str = 'openmldb_default',
        **kwargs,
    ) -> None:
        select_out_options = kwargs.pop('options', None)
        sql = f"{sql} INTO OUTFILE '{file}'"
        if select_out_options:
            sql += f" OPTIONS({select_out_options})"
        super().__init__(db=db, mode=mode, sql=sql, **kwargs)
        self.openmldb_conn_id = openmldb_conn_id


class OpenMLDBDeployOperator(OpenMLDBSQLOperator):
    """
    This operator deploys a sql to OpenMLDB

    :param db: The database you want to use
    :param deploy_name: The deployment name
    :param sql: The sql you want to deploy
    :param openmldb_conn_id: The Airflow connection used for OpenMLDB.
    :keyword options: load data options
    """

    def __init__(
        self,
        db: str,
        deploy_name: str,
        sql: str,
        openmldb_conn_id: str = 'openmldb_default',
        **kwargs,
    ) -> None:
        super().__init__(
            db=db, mode=Mode.ONLINE, sql=f"DEPLOY {deploy_name} {sql}", **kwargs  # mode can be any one
        )
        self.openmldb_conn_id = openmldb_conn_id
