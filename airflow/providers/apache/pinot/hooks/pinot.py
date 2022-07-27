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
import subprocess
from typing import Any, Iterable, List, Mapping, Optional, Union

from pinotdb import connect

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook


class PinotAdminHook(BaseHook):
    """
    This hook is a wrapper around the pinot-admin.sh script.
    For now, only small subset of its subcommands are implemented,
    which are required to ingest offline data into Apache Pinot
    (i.e., AddSchema, AddTable, CreateSegment, and UploadSegment).
    Their command options are based on Pinot v0.1.0.

    Unfortunately, as of v0.1.0, pinot-admin.sh always exits with
    status code 0. To address this behavior, users can use the
    pinot_admin_system_exit flag. If its value is set to false,
    this hook evaluates the result based on the output message
    instead of the status code. This Pinot's behavior is supposed
    to be improved in the next release, which will include the
    following PR: https://github.com/apache/incubator-pinot/pull/4110

    :param conn_id: The name of the connection to use.
    :param cmd_path: The filepath to the pinot-admin.sh executable
    :param pinot_admin_system_exit: If true, the result is evaluated based on the status code.
                                    Otherwise, the result is evaluated as a failure if "Error" or
                                    "Exception" is in the output message.
    """

    def __init__(
        self,
        conn_id: str = "pinot_admin_default",
        cmd_path: str = "pinot-admin.sh",
        pinot_admin_system_exit: bool = False,
    ) -> None:
        super().__init__()
        conn = self.get_connection(conn_id)
        self.host = conn.host
        self.port = str(conn.port)
        self.cmd_path = conn.extra_dejson.get("cmd_path", cmd_path)
        self.pinot_admin_system_exit = conn.extra_dejson.get(
            "pinot_admin_system_exit", pinot_admin_system_exit
        )
        self.conn = conn

    def get_conn(self) -> Any:
        return self.conn

    def add_schema(self, schema_file: str, with_exec: bool = True) -> Any:
        """
        Add Pinot schema by run AddSchema command

        :param schema_file: Pinot schema file
        :param with_exec: bool
        """
        cmd = ["AddSchema"]
        cmd += ["-controllerHost", self.host]
        cmd += ["-controllerPort", self.port]
        cmd += ["-schemaFile", schema_file]
        if with_exec:
            cmd += ["-exec"]
        self.run_cli(cmd)

    def add_table(self, file_path: str, with_exec: bool = True) -> Any:
        """
        Add Pinot table with run AddTable command

        :param file_path: Pinot table configure file
        :param with_exec: bool
        """
        cmd = ["AddTable"]
        cmd += ["-controllerHost", self.host]
        cmd += ["-controllerPort", self.port]
        cmd += ["-filePath", file_path]
        if with_exec:
            cmd += ["-exec"]
        self.run_cli(cmd)

    def create_segment(
        self,
        generator_config_file: Optional[str] = None,
        data_dir: Optional[str] = None,
        segment_format: Optional[str] = None,
        out_dir: Optional[str] = None,
        overwrite: Optional[str] = None,
        table_name: Optional[str] = None,
        segment_name: Optional[str] = None,
        time_column_name: Optional[str] = None,
        schema_file: Optional[str] = None,
        reader_config_file: Optional[str] = None,
        enable_star_tree_index: Optional[str] = None,
        star_tree_index_spec_file: Optional[str] = None,
        hll_size: Optional[str] = None,
        hll_columns: Optional[str] = None,
        hll_suffix: Optional[str] = None,
        num_threads: Optional[str] = None,
        post_creation_verification: Optional[str] = None,
        retry: Optional[str] = None,
    ) -> Any:
        """Create Pinot segment by run CreateSegment command"""
        cmd = ["CreateSegment"]

        if generator_config_file:
            cmd += ["-generatorConfigFile", generator_config_file]

        if data_dir:
            cmd += ["-dataDir", data_dir]

        if segment_format:
            cmd += ["-format", segment_format]

        if out_dir:
            cmd += ["-outDir", out_dir]

        if overwrite:
            cmd += ["-overwrite", overwrite]

        if table_name:
            cmd += ["-tableName", table_name]

        if segment_name:
            cmd += ["-segmentName", segment_name]

        if time_column_name:
            cmd += ["-timeColumnName", time_column_name]

        if schema_file:
            cmd += ["-schemaFile", schema_file]

        if reader_config_file:
            cmd += ["-readerConfigFile", reader_config_file]

        if enable_star_tree_index:
            cmd += ["-enableStarTreeIndex", enable_star_tree_index]

        if star_tree_index_spec_file:
            cmd += ["-starTreeIndexSpecFile", star_tree_index_spec_file]

        if hll_size:
            cmd += ["-hllSize", hll_size]

        if hll_columns:
            cmd += ["-hllColumns", hll_columns]

        if hll_suffix:
            cmd += ["-hllSuffix", hll_suffix]

        if num_threads:
            cmd += ["-numThreads", num_threads]

        if post_creation_verification:
            cmd += ["-postCreationVerification", post_creation_verification]

        if retry:
            cmd += ["-retry", retry]

        self.run_cli(cmd)

    def upload_segment(self, segment_dir: str, table_name: Optional[str] = None) -> Any:
        """
        Upload Segment with run UploadSegment command

        :param segment_dir:
        :param table_name:
        :return:
        """
        cmd = ["UploadSegment"]
        cmd += ["-controllerHost", self.host]
        cmd += ["-controllerPort", self.port]
        cmd += ["-segmentDir", segment_dir]
        if table_name:
            cmd += ["-tableName", table_name]
        self.run_cli(cmd)

    def run_cli(self, cmd: List[str], verbose: bool = True) -> str:
        """
        Run command with pinot-admin.sh

        :param cmd: List of command going to be run by pinot-admin.sh script
        :param verbose:
        """
        command = [self.cmd_path, *cmd]
        env = None
        if self.pinot_admin_system_exit:
            env = os.environ.copy()
            java_opts = "-Dpinot.admin.system.exit=true " + os.environ.get("JAVA_OPTS", "")
            env.update({"JAVA_OPTS": java_opts})

        if verbose:
            self.log.info(" ".join(command))

        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True, env=env
        ) as sub_process:
            stdout = ""
            if sub_process.stdout:
                for line in iter(sub_process.stdout.readline, b''):
                    stdout += line.decode("utf-8")
                    if verbose:
                        self.log.info(line.decode("utf-8").strip())

            sub_process.wait()

            # As of Pinot v0.1.0, either of "Error: ..." or "Exception caught: ..."
            # is expected to be in the output messages. See:
            # https://github.com/apache/incubator-pinot/blob/release-0.1.0/pinot-tools/src/main/java/org/apache/pinot/tools/admin/PinotAdministrator.java#L98-L101
            if (self.pinot_admin_system_exit and sub_process.returncode) or (
                "Error" in stdout or "Exception" in stdout
            ):
                raise AirflowException(stdout)

        return stdout


class PinotDbApiHook(DbApiHook):
    """
    Interact with Pinot Broker Query API

    This hook uses standard-SQL endpoint since PQL endpoint is soon to be deprecated.
    https://docs.pinot.apache.org/users/api/querying-pinot-using-standard-sql
    """

    conn_name_attr = 'pinot_broker_conn_id'
    default_conn_name = 'pinot_broker_default'
    supports_autocommit = False

    def get_conn(self) -> Any:
        """Establish a connection to pinot broker through pinot dbapi."""
        conn = self.get_connection(self.pinot_broker_conn_id)  # type: ignore

        pinot_broker_conn = connect(
            host=conn.host,
            port=conn.port,
            path=conn.extra_dejson.get('endpoint', '/query/sql'),
            scheme=conn.extra_dejson.get('schema', 'http'),
        )
        self.log.info('Get the connection to pinot broker on %s', conn.host)
        return pinot_broker_conn

    def get_uri(self) -> str:
        """
        Get the connection uri for pinot broker.

        e.g: http://localhost:9000/query/sql
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        host = conn.host
        if conn.port is not None:
            host += f':{conn.port}'
        conn_type = conn.conn_type or 'http'
        endpoint = conn.extra_dejson.get('endpoint', 'query/sql')
        return f'{conn_type}://{host}/{endpoint}'

    def get_records(
        self, sql: Union[str, List[str]], parameters: Optional[Union[Iterable, Mapping]] = None, **kwargs
    ) -> Any:
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        """
        with self.get_conn() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def get_first(
        self, sql: Union[str, List[str]], parameters: Optional[Union[Iterable, Mapping]] = None
    ) -> Any:
        """
        Executes the sql and returns the first resulting row.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        """
        with self.get_conn() as cur:
            cur.execute(sql)
            return cur.fetchone()

    def set_autocommit(self, conn: Connection, autocommit: Any) -> Any:
        raise NotImplementedError()

    def insert_rows(
        self,
        table: str,
        rows: str,
        target_fields: Optional[str] = None,
        commit_every: int = 1000,
        replace: bool = False,
        **kwargs: Any,
    ) -> Any:
        raise NotImplementedError()
