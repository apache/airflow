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
#
"""This module contains Databricks operators."""

import csv
import json
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DatabricksSqlOperator(BaseOperator):
    """
    Executes SQL code in a Databricks SQL endpoint or a Databricks cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksSqlOperator`

    :param databricks_conn_id: Reference to
        :ref:`Databricks connection id<howto/connection:databricks>`
    :param http_path: Optional string specifying HTTP path of Databricks SQL Endpoint or cluster.
        If not specified, it should be either specified in the Databricks connection's extra parameters,
        or ``sql_endpoint_name`` must be specified.
    :param sql_endpoint_name: Optional name of Databricks SQL Endpoint. If not specified, ``http_path`` must
        be provided as described above.
    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param parameters: (optional) the parameters to render the SQL query with.
    :param session_configuration: An optional dictionary of Spark session parameters. Defaults to None.
        If not specified, it could be specified in the Databricks connection's extra parameters.
    :param output_path: optional string specifying the file to which write selected data.
    :param output_format: format of output data if ``output_path` is specified.
        Possible values are ``csv``, ``json``, ``jsonl``. Default is ``csv``.
    :param csv_params: parameters that will be passed to the ``csv.DictWriter`` class used to write CSV data.
    """

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}

    def __init__(
        self,
        *,
        sql: Union[str, List[str]],
        databricks_conn_id: str = DatabricksSqlHook.default_conn_name,
        http_path: Optional[str] = None,
        sql_endpoint_name: Optional[str] = None,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        session_configuration=None,
        do_xcom_push: bool = False,
        output_path: Optional[str] = None,
        output_format: str = 'csv',
        csv_params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksSqlOperator``."""
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.sql = sql
        self._http_path = http_path
        self._sql_endpoint_name = sql_endpoint_name
        self._output_path = output_path
        self._output_format = output_format
        self._csv_params = csv_params
        self.parameters = parameters
        self.do_xcom_push = do_xcom_push
        self.session_config = session_configuration

    def _get_hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id,
            http_path=self._http_path,
            session_configuration=self.session_config,
            sql_endpoint_name=self._sql_endpoint_name,
        )

    def _format_output(self, schema, results):
        if not self._output_path:
            return
        if not self._output_format:
            raise AirflowException("Output format should be specified!")
        field_names = [field[0] for field in schema]
        if self._output_format.lower() == "csv":
            with open(self._output_path, "w", newline='') as file:
                if self._csv_params:
                    csv_params = self._csv_params
                else:
                    csv_params = {}
                write_header = csv_params.get("header", True)
                if "header" in csv_params:
                    del csv_params["header"]
                writer = csv.DictWriter(file, fieldnames=field_names, **csv_params)
                if write_header:
                    writer.writeheader()
                for row in results:
                    writer.writerow(row.asDict())
        elif self._output_format.lower() == "json":
            with open(self._output_path, "w") as file:
                file.write(json.dumps([row.asDict() for row in results]))
        elif self._output_format.lower() == "jsonl":
            with open(self._output_path, "w") as file:
                for row in results:
                    file.write(json.dumps(row.asDict()))
                    file.write("\n")
        else:
            raise AirflowException(f"Unsupported output format: '{self._output_format}'")

    def execute(self, context: 'Context') -> Any:
        self.log.info('Executing: %s', self.sql)
        hook = self._get_hook()
        schema, results = hook.run(self.sql, parameters=self.parameters)
        # self.log.info('Schema: %s', schema)
        # self.log.info('Results: %s', results)
        self._format_output(schema, results)
        if self.do_xcom_push:
            return results
