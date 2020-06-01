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

from distutils.util import strtobool
from typing import Dict, Iterable, List, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults

ALLOWED_CONN_TYPE = {
    "google_cloud_platform",
    "jdbc",
    "mssql",
    "mysql",
    "odbc",
    "oracle",
    "postgres",
    "presto",
    "sqlite",
    "vertica",
}


class BranchSqlOperator(BaseOperator, SkipMixin):
    """
    Executes sql code in a specific database

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement or reference to a template file.
               Template reference are recognized by str ending in '.sql'.
               Expected SQL query to return Boolean (True/False), integer (0 = False, Otherwise = 1)
               or string (true/y/yes/1/on/false/n/no/0/off).
    :param follow_task_ids_if_true: task id or task ids to follow if query return true
    :type follow_task_ids_if_true: str or list
    :param follow_task_ids_if_false: task id or task ids to follow if query return true
    :type follow_task_ids_if_false: str or list
    :param conn_id: reference to a specific database
    :type conn_id: str
    :param database: name of database which overwrite defined one in connection
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping or iterable
    """

    template_fields = ("sql",)
    template_ext = (".sql",)
    ui_color = "#a22034"
    ui_fgcolor = "#F7F7F7"

    @apply_defaults
    def __init__(
        self,
        sql: str,
        follow_task_ids_if_true: List[str],
        follow_task_ids_if_false: List[str],
        conn_id: str = "default_conn_id",
        database: Optional[str] = None,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql
        self.parameters = parameters
        self.follow_task_ids_if_true = follow_task_ids_if_true
        self.follow_task_ids_if_false = follow_task_ids_if_false
        self.database = database
        self._hook = None

    def _get_hook(self):
        self.log.debug("Get connection for %s", self.conn_id)
        conn = BaseHook.get_connection(self.conn_id)

        if conn.conn_type not in ALLOWED_CONN_TYPE:
            raise AirflowException(
                "The connection type is not supported by BranchSqlOperator. "
                + "Supported connection types: {}".format(list(ALLOWED_CONN_TYPE))
            )

        if not self._hook:
            self._hook = conn.get_hook()
            if self.database:
                self._hook.schema = self.database

        return self._hook

    def execute(self, context: Dict):
        # get supported hook
        self._hook = self._get_hook()

        if self._hook is None:
            raise AirflowException(
                "Failed to establish connection to '%s'" % self.conn_id
            )

        if self.sql is None:
            raise AirflowException("Expected 'sql' parameter is missing.")

        if self.follow_task_ids_if_true is None:
            raise AirflowException(
                "Expected 'follow_task_ids_if_true' paramter is missing."
            )

        if self.follow_task_ids_if_false is None:
            raise AirflowException(
                "Expected 'follow_task_ids_if_false' parameter is missing."
            )

        self.log.info(
            "Executing: %s (with parameters %s) with connection: %s",
            self.sql,
            self.parameters,
            self._hook,
        )
        record = self._hook.get_first(self.sql, self.parameters)
        if not record:
            raise AirflowException(
                "No rows returned from sql query. Operator expected True or False return value."
            )

        if isinstance(record, list):
            if isinstance(record[0], list):
                query_result = record[0][0]
            else:
                query_result = record[0]
        elif isinstance(record, tuple):
            query_result = record[0]
        else:
            query_result = record

        self.log.info("Query returns %s, type '%s'", query_result, type(query_result))

        follow_branch = None
        try:
            if isinstance(query_result, bool):
                if query_result:
                    follow_branch = self.follow_task_ids_if_true
            elif isinstance(query_result, str):
                # return result is not Boolean, try to convert from String to Boolean
                if bool(strtobool(query_result)):
                    follow_branch = self.follow_task_ids_if_true
            elif isinstance(query_result, int):
                if bool(query_result):
                    follow_branch = self.follow_task_ids_if_true
            else:
                raise AirflowException(
                    "Unexpected query return result '%s' type '%s'"
                    % (query_result, type(query_result))
                )

            if follow_branch is None:
                follow_branch = self.follow_task_ids_if_false
        except ValueError:
            raise AirflowException(
                "Unexpected query return result '%s' type '%s'"
                % (query_result, type(query_result))
            )

        self.skip_all_except(context["ti"], follow_branch)
