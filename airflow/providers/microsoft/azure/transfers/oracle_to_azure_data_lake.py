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

import os
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Optional, Sequence, Union

import unicodecsv as csv

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OracleToAzureDataLakeOperator(BaseOperator):
    """
    Moves data from Oracle to Azure Data Lake. The operator runs the query against
    Oracle and stores the file locally before loading it into Azure Data Lake.


    :param filename: file name to be used by the csv file.
    :param azure_data_lake_conn_id: destination azure data lake connection.
    :param azure_data_lake_path: destination path in azure data lake to put the file.
    :param oracle_conn_id: :ref:`Source Oracle connection <howto/connection:oracle>`.
    :param sql: SQL query to execute against the Oracle database. (templated)
    :param sql_params: Parameters to use in sql query. (templated)
    :param delimiter: field delimiter in the file.
    :param encoding: encoding type for the file.
    :param quotechar: Character to use in quoting.
    :param quoting: Quoting strategy. See unicodecsv quoting for more information.
    """

    template_fields: Sequence[str] = ('filename', 'sql', 'sql_params')
    template_fields_renderers = {"sql_params": "py"}
    ui_color = '#e08c8c'

    def __init__(
        self,
        *,
        filename: str,
        azure_data_lake_conn_id: str,
        azure_data_lake_path: str,
        oracle_conn_id: str,
        sql: str,
        sql_params: Optional[dict] = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
        quotechar: str = '"',
        quoting: str = csv.QUOTE_MINIMAL,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if sql_params is None:
            sql_params = {}
        self.filename = filename
        self.oracle_conn_id = oracle_conn_id
        self.sql = sql
        self.sql_params = sql_params
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.azure_data_lake_path = azure_data_lake_path
        self.delimiter = delimiter
        self.encoding = encoding
        self.quotechar = quotechar
        self.quoting = quoting

    def _write_temp_file(self, cursor: Any, path_to_save: Union[str, bytes, int]) -> None:
        with open(path_to_save, 'wb') as csvfile:
            csv_writer = csv.writer(
                csvfile,
                delimiter=self.delimiter,
                encoding=self.encoding,
                quotechar=self.quotechar,
                quoting=self.quoting,
            )
            csv_writer.writerow(map(lambda field: field[0], cursor.description))
            csv_writer.writerows(cursor)
            csvfile.flush()

    def execute(self, context: "Context") -> None:
        oracle_hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        azure_data_lake_hook = AzureDataLakeHook(azure_data_lake_conn_id=self.azure_data_lake_conn_id)

        self.log.info("Dumping Oracle query results to local file")
        conn = oracle_hook.get_conn()
        cursor = conn.cursor()  # type: ignore[attr-defined]
        cursor.execute(self.sql, self.sql_params)

        with TemporaryDirectory(prefix='airflow_oracle_to_azure_op_') as temp:
            self._write_temp_file(cursor, os.path.join(temp, self.filename))
            self.log.info("Uploading local file to Azure Data Lake")
            azure_data_lake_hook.upload_file(
                os.path.join(temp, self.filename), os.path.join(self.azure_data_lake_path, self.filename)
            )
        cursor.close()
        conn.close()  # type: ignore[attr-defined]
