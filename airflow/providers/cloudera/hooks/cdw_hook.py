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

import csv
import os
import subprocess
import time
from io import StringIO
from tempfile import NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING

HIVE_QUEUE_PRIORITIES = ["VERY_HIGH", "HIGH", "NORMAL", "LOW", "VERY_LOW"]
JDBC_BACKEND_HIVE = "hive2"
JDBC_BACKEND_IMPALA = "impala"


def get_context_from_env_var():
    """
    Extract context from env variable, e.g. dag_id, task_id and execution_date,
    so that they can be used inside BashOperator and PythonOperator.

    :return: The context of interest.
    """
    return {
        format_map["default"]: os.environ.get(format_map["env_var_format"], "")
        for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()
    }


class CdwHook(HiveCliHook):
    """Simple CDW hive cli hook which extends the functionality of HiveCliHook
    in order to conform the parameter needs.

    :param cli_conn_id: airflow connection id to be used.
    :param query_isolation: controls whether to use cdw's query isolation feature.
        Only hive warehouses support this at the moment.
    :jdbc_driver: a safety valve for JDBC driver class. It's not supposed to be changed by default as
        CdwHook guesses and uses the correct driver for impala. The environment provides both JDBC 4.1
        and JDBC 4.2 driver. Currently, JDBC 4.1 is used for cdw.
        For hive, the driver class is not defined at all in beeline cli.
    """

    def __init__(
        self,
        cli_conn_id=None,
        query_isolation=True,
        jdbc_driver="com.cloudera.impala.jdbc41.Driver",
    ):
        super().__init__(cli_conn_id)
        self.conn = self.get_connection(cli_conn_id)
        self.query_isolation = query_isolation
        self.jdbc_driver = jdbc_driver if jdbc_driver is not None else "com.cloudera.impala.jdbc41.Driver"
        self.sub_process = None

    def get_cli_cmd(self):
        """This is supposed to be visible for testing."""
        return self._prepare_cli_cmd()

    def _prepare_cli_cmd(self, hide_secrets=False):
        """
        This function creates the command list from available information.
        :param hide_secrets: whether to mask secrets with asterisk
        """
        conn = self.conn
        cmd_extra = []

        hive_bin = "beeline"  # only beeline is supported as client while connecting to CDW
        jdbc_backend = CdwHook.get_jdbc_backend(conn)

        jdbc_url = f"jdbc:{jdbc_backend}://{conn.host}{CdwHook.get_port_string(conn)}/{conn.schema}"

        # HTTP+SSL is default for CDW, but it can be overwritten in connection extra params if needed
        if jdbc_backend == JDBC_BACKEND_IMPALA:
            jdbc_url = self.add_parameter_to_jdbc_url(conn.extra_dejson, jdbc_url, "AuthMech", "3")
        jdbc_url = self.add_parameter_to_jdbc_url(conn.extra_dejson, jdbc_url, "transportMode", "http")
        jdbc_url = self.add_parameter_to_jdbc_url(conn.extra_dejson, jdbc_url, "httpPath", "cliservice")
        jdbc_url = self.add_parameter_to_jdbc_url(
            conn.extra_dejson, jdbc_url, "ssl", CdwHook.get_ssl_parameter(conn)
        )

        if jdbc_backend == JDBC_BACKEND_IMPALA:
            cmd_extra += ["-d", self.jdbc_driver]

        cmd_extra += ["-u", jdbc_url]
        if conn.login:
            cmd_extra += ["-n", conn.login]
        if conn.password:
            cmd_extra += ["-p", conn.password if not hide_secrets else "********"]

        self.add_extra_parameters(jdbc_backend, cmd_extra)

        return [hive_bin] + cmd_extra

    def add_extra_parameters(self, jdbc_backend, cmd_extra):
        """
        Adds extra parameters to the beeline command in addition to the basic, needed ones.
        This can be overridden in subclasses in order to change beeline behavior.
        """
        # this hive option is supposed to enforce query isolation regardless
        # of the initial settings used while creating the virtual warehouse
        if self.query_isolation and jdbc_backend == JDBC_BACKEND_HIVE:
            cmd_extra += ["--hiveconf", "hive.query.isolation.scan.size.threshold=0B"]
            cmd_extra += ["--hiveconf", "hive.query.results.cache.enabled=false"]
            cmd_extra += [
                "--hiveconf",
                "hive.auto.convert.join.noconditionaltask.size=2505397589",
            ]

    @staticmethod
    def get_jdbc_backend(conn):
        """
        Tries to guess the underlying database from connection host. In CDW, JDBC urls are like below:
        hive:
        - hs2-lbodor-airflow-hive.env-xkg48s.dwx.dev.cldr.work
        impala:
        - impala-proxy-lbodor-airflow-impala.env-xkg48s.dwx.dev.cldr.work:443
        - coordinator-lbodor-impala-test.env-xkg48s.dwx.dev.cldr.work:443
        So this method returns the database kind string which can be used in jdbc string:
        hive: 'hive2'
        impala: 'impala'
        """
        return (
            JDBC_BACKEND_IMPALA
            if (conn.host.find("coordinator-") == 0 or conn.host.find("impala-proxy") == 0)
            else JDBC_BACKEND_HIVE
        )

    @staticmethod
    def get_port_string(conn):
        """
        hive: ''
        impala: ':443'
        """
        backend = CdwHook.get_jdbc_backend(conn)
        return ":443" if backend == JDBC_BACKEND_IMPALA else ""

    @staticmethod
    def get_ssl_parameter(conn):
        """
        hive: 'true'
        impala: '1'
        """
        backend = CdwHook.get_jdbc_backend(conn)
        return "1" if backend == JDBC_BACKEND_IMPALA else "true"

    def run_cli(self, hql, schema="default", verbose=True, hive_conf=None):
        """Copied from hive hook, but removed unnecessary parts, e.g. mapred queue."""
        conn = self.conn
        schema = schema or conn.schema
        if schema:
            hql = f"USE {schema};\n{hql}"

        with TemporaryDirectory(prefix="airflow_hiveop_") as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir) as f:
                hql = hql + "\n"
                f.write(hql.encode("UTF-8"))
                f.flush()
                hive_cmd = self._prepare_cli_cmd()
                env_context = get_context_from_env_var()
                # Only extend the hive_conf if it is defined.
                if hive_conf:
                    env_context.update(hive_conf)
                hive_conf_params = self._prepare_hiveconf(env_context)
                hive_cmd.extend(hive_conf_params)
                hive_cmd.extend(["-f", f.name])

                if verbose:
                    self.log.info("%s", " ".join(self._prepare_cli_cmd(hide_secrets=True)))
                sub_process = subprocess.Popen(
                    hive_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=tmp_dir,
                    close_fds=True,
                )
                self.sub_process = sub_process
                stdout = ""
                while True:
                    line = sub_process.stdout.readline()
                    if not line:
                        break
                    stdout += line.decode("UTF-8")
                    if verbose:
                        self.log.info(line.decode("UTF-8").strip())
                sub_process.wait()

                if sub_process.returncode:
                    raise AirflowException(stdout)

                return stdout

    def kill(self):
        if hasattr(self, "sub_process") and self.sub_process is not None:
            if self.sub_process.poll() is None:
                print("Killing the Hive job")
                self.sub_process.terminate()
                time.sleep(60)
                self.sub_process.kill()

    @staticmethod
    def add_parameter_to_jdbc_url(extra_dejson, jdbc_url, parameter_name, default_value=None):
        """
        Appends a parameter to jdbc url if found in connection json extras
        or there is a not None default value.
        """
        if extra_dejson is None or extra_dejson.get(parameter_name, default_value) is None:
            return jdbc_url

        return jdbc_url + f";{parameter_name}={extra_dejson.get(parameter_name, default_value)}"


class CdwHiveMetastoreHook(CdwHook):
    """A hive metastore hook which should behave the same as HiveMetastoreHook,
    but instead of a kerberized, binary thrift connection it uses beeline as the client,
    which connects to sys database.
    """

    def __init__(self, cli_conn_id="metastore_default"):
        """
        In CdwHiveMetastoreHook this is supposed to be a beeline connection,
        pointing to sys schema, so the conn should point to a hive cli wrapper connection in airflow,
        similarly to CdwHook's cli_conn_id.
        """
        super().__init__(cli_conn_id=cli_conn_id)
        self.conn.schema = "sys"  # metastore database

    def check_for_partition(self, schema, table, partition):
        """
        Checks whether a partition exists

        :param schema: Name of hive schema (database) @table belongs to
        :param table: Name of hive table @partition belongs to
        :partition: Expression that matches the partitions to check for
        :rtype: bool
        """
        hql = (
            "select dbs.name as db_name, tbls.tbl_name as tbl_name, partitions.part_name as "
            "part_name from partitions left outer join tbls on tbls.tbl_id = partitions.tbl_id left "
            f"outer join dbs on dbs.db_id = tbls.db_id where dbs.name = '{schema}' and tbls.tbl_name "
            f"= '{table}' and partitions.part_name = '{partition}';"
        )

        response = self.run_cli(hql, self.conn.schema, verbose=True, hive_conf=None)
        result_lines = CdwHiveMetastoreHook.parse_csv_lines(response)
        results_without_header = CdwHiveMetastoreHook.get_results_without_header(
            result_lines, "db_name,tbl_name,part_name"
        )

        self.log.info("partitions: %s", results_without_header)
        return len(results_without_header) > 0

    def check_for_named_partition(self, schema, table, partition):
        """
        Checks whether a partition with a given name exists

        :param schema: Name of hive schema (database) @table belongs to
        :param table: Name of hive table @partition belongs to
        :partition: Name of the partitions to check for (eg `a=b/c=d`)
        :rtype: bool
        """
        raise Exception("TODO IMPLEMENT")

    def get_table(self, table_name, db="default"):
        """Get a metastore table object"""
        if db == "default" and "." in table_name:
            db, table_name = table_name.split(".")[:2]
        hql = (
            "select dbs.name as db_name, tbls.tbl_name as tbl_namefrom tbls left outer join dbs on "
            f"dbs.db_id = tbls.db_id where dbs.name = '{db}' and tbls.tbl_name = '{table_name}' "
        )

        response = self.run_cli(hql, self.conn.schema, verbose=True, hive_conf=None)
        result_lines = CdwHiveMetastoreHook.parse_csv_lines(response)

        tables = CdwHiveMetastoreHook.get_results_without_header(result_lines, "db_name,tbl_name")
        return tables

    def get_tables(self, db, pattern="*"):
        """Get a metastore table object."""
        hql = (
            "select dbs.name as db_name, tbls.tbl_name as tbl_namefrom tbls left outer join dbs on dbs.db_id"
            f" = tbls.db_id where dbs.name = '{db}' and tbls.tbl_name like '{pattern.replace('*', '%')}' "
        )
        response = self.run_cli(hql, self.conn.schema, verbose=True, hive_conf=None)
        result_lines = CdwHiveMetastoreHook.parse_csv_lines(response)

        tables = CdwHiveMetastoreHook.get_results_without_header(result_lines, "db_name,tbl_name")

        self.log.info("tables: %s", tables)
        return len(tables) > 0

    def get_databases(self, pattern="*"):
        """Get a metastore table object."""
        hql = f"select dbs.name from dbs where dbs.name LIKE '{pattern.replace('*', '%')}' "

        response = self.run_cli(hql, self.conn.schema, verbose=True, hive_conf=None)
        result_lines = CdwHiveMetastoreHook.parse_csv_lines(response)

        databases = CdwHiveMetastoreHook.get_results_without_header(result_lines, "db_name,tbl_name")

        self.log.info("databases: %s", databases)
        return databases

    def get_partitions(self, schema, table_name, partition_filter=None):
        """Returns a list of all partitions in a table."""
        raise Exception("TODO IMPLEMENT")

    def max_partition(self, schema, table_name, field=None, filter_map=None):
        """
        Returns the maximum value for all partitions with given field in a table.
        If only one partition key exist in the table, the key will be used as field.
        filter_map should be a partition_key:partition_value map and will be used to
        filter out partitions.

        :param schema: schema name.
        :param table_name: table name.
        :param field: partition key to get max partition from.
        :param filter_map: partition_key:partition_value map used for partition filtering.
        """
        raise Exception("TODO IMPLEMENT")

    def table_exists(self, table_name, db="default"):
        """Check if table exists."""
        tables = self.get_table(table_name, db)
        return len(tables) > 0

    @staticmethod
    def parse_csv_lines(response):
        r"""
        Parses a csv string by generating a list of lists.
        E.g.
        Input: 'cdw,no\ne,two'
        Output: [['cdw', 'no'], ['e', 'two']]
        """
        readable_input = StringIO(response)
        return list(csv.reader(readable_input, delimiter=","))

    @staticmethod
    def get_results_without_header(result_lines, header):
        """Parses beeline output and removes noise before the given reader (e.g. SLF4J warnings)."""
        final_list = []
        add_line = False
        for line in result_lines:
            if add_line:
                final_list.append(line)
            elif ",".join(line) == header:
                add_line = True
        return final_list

    def add_extra_parameters(self, jdbc_backend, cmd_extra):
        """
        Overrides CdwHook.add_extra_parameters in order to enable behavior
        which is optimal for fetching and parsing metadata in csv.
        """
        cmd_extra += ["--hiveconf", "hive.query.isolation.scan.size.threshold=1GB"]
        cmd_extra += ["--silent=true"]
        cmd_extra += ["--outputformat=csv2"]
        cmd_extra += ["--showHeader=true"]
