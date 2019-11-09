# -*- coding: utf-8 -*-
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

from pinotdb import connect

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.dbapi_hook import DbApiHook


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
    :type conn_id: str
    :param cmd_path: The filepath to the pinot-admin.sh executable
    :type cmd_path: str
    :param pinot_admin_system_exit: If true, the result is evaluated based on the status code.
                                    Otherwise, the result is evaluated as a failure if "Error" or
                                    "Exception" is in the output message.
    :type pinot_admin_system_exit: bool
    """
    def __init__(self,
                 conn_id="pinot_admin_default",
                 cmd_path="pinot-admin.sh",
                 pinot_admin_system_exit=False):
        conn = self.get_connection(conn_id)
        self.host = conn.host
        self.port = str(conn.port)
        self.cmd_path = conn.extra_dejson.get("cmd_path", cmd_path)
        self.pinot_admin_system_exit = conn.extra_dejson.get("pinot_admin_system_exit",
                                                             pinot_admin_system_exit)
        self.conn = conn

    def get_conn(self):
        return self.conn

    def add_schema(self, schema_file, exec=True):
        cmd = ["AddSchema"]
        cmd += ["-controllerHost", self.host]
        cmd += ["-controllerPort", self.port]
        cmd += ["-schemaFile", schema_file]
        if exec:
            cmd += ["-exec"]
        self.run_cli(cmd)

    def add_table(self, file_path, exec=True):
        cmd = ["AddTable"]
        cmd += ["-controllerHost", self.host]
        cmd += ["-controllerPort", self.port]
        cmd += ["-filePath", file_path]
        if exec:
            cmd += ["-exec"]
        self.run_cli(cmd)

    def create_segment(self,
                       generator_config_file=None,
                       data_dir=None,
                       format=None,
                       out_dir=None,
                       overwrite=None,
                       table_name=None,
                       segment_name=None,
                       time_column_name=None,
                       schema_file=None,
                       reader_config_file=None,
                       enable_star_tree_index=None,
                       star_tree_index_spec_file=None,
                       hll_size=None,
                       hll_columns=None,
                       hll_suffix=None,
                       num_threads=None,
                       post_creation_verification=None,
                       retry=None):
        cmd = ["CreateSegment"]

        if generator_config_file:
            cmd += ["-generatorConfigFile", generator_config_file]

        if data_dir:
            cmd += ["-dataDir", data_dir]

        if format:
            cmd += ["-format", format]

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

    def upload_segment(self, segment_dir, table_name=None):
        cmd = ["UploadSegment"]
        cmd += ["-controllerHost", self.host]
        cmd += ["-controllerPort", self.port]
        cmd += ["-segmentDir", segment_dir]
        if table_name:
            cmd += ["-tableName", table_name]
        self.run_cli(cmd)

    def run_cli(self, cmd, verbose=True):
        command = [self.cmd_path]
        command.extend(cmd)

        env = None
        if self.pinot_admin_system_exit:
            env = os.environ.copy()
            java_opts = "-Dpinot.admin.system.exit=true " + os.environ.get("JAVA_OPTS", "")
            env.update({"JAVA_OPTS": java_opts})

        if verbose:
            self.log.info(" ".join(command))

        sp = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            close_fds=True,
            env=env)

        stdout = ""
        for line in iter(sp.stdout):
            line = line.decode()
            stdout += line
            if verbose:
                self.log.info(line.strip())

        sp.wait()

        # As of Pinot v0.1.0, either of "Error: ..." or "Exception caught: ..."
        # is expected to be in the output messages. See:
        # https://github.com/apache/incubator-pinot/blob/release-0.1.0/pinot-tools/src/main/java/org/apache/pinot/tools/admin/PinotAdministrator.java#L98-L101
        if ((self.pinot_admin_system_exit and sp.returncode) or
                ("Error" in stdout or "Exception" in stdout)):
            raise AirflowException(stdout)

        return stdout


class PinotDbApiHook(DbApiHook):
    """
    Connect to pinot db (https://github.com/apache/incubator-pinot) to issue pql
    """
    conn_name_attr = 'pinot_broker_conn_id'
    default_conn_name = 'pinot_broker_default'
    supports_autocommit = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_conn(self):
        """
        Establish a connection to pinot broker through pinot dbapi.
        """
        conn = self.get_connection(self.pinot_broker_conn_id)
        pinot_broker_conn = connect(
            host=conn.host,
            port=conn.port,
            path=conn.extra_dejson.get('endpoint', '/pql'),
            scheme=conn.extra_dejson.get('schema', 'http')
        )
        self.log.info('Get the connection to pinot '
                      'broker on {host}'.format(host=conn.host))
        return pinot_broker_conn

    def get_uri(self):
        """
        Get the connection uri for pinot broker.

        e.g: http://localhost:9000/pql
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        host = conn.host
        if conn.port is not None:
            host += ':{port}'.format(port=conn.port)
        conn_type = 'http' if not conn.conn_type else conn.conn_type
        endpoint = conn.extra_dejson.get('endpoint', 'pql')
        return '{conn_type}://{host}/{endpoint}'.format(
            conn_type=conn_type, host=host, endpoint=endpoint)

    def get_records(self, sql):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str
        """
        with self.get_conn() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def get_first(self, sql):
        """
        Executes the sql and returns the first resulting row.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        """
        with self.get_conn() as cur:
            cur.execute(sql)
            return cur.fetchone()

    def set_autocommit(self, conn, autocommit):
        raise NotImplementedError()

    def get_pandas_df(self, sql, parameters=None):
        raise NotImplementedError()

    def insert_rows(self, table, rows, target_fields=None, commit_every=1000):
        raise NotImplementedError()
