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
from __future__ import annotations

import contextlib
import os
import re
import socket
import subprocess
import time
from collections.abc import Iterable, Mapping
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import TYPE_CHECKING, Any, Literal

from deprecated import deprecated
from typing_extensions import overload

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

import csv

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.apache.hive.version_compat import AIRFLOW_VAR_NAME_FORMAT_MAPPING, BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.security import utils
from airflow.utils.helpers import as_flattened_list

HIVE_QUEUE_PRIORITIES = ["VERY_HIGH", "HIGH", "NORMAL", "LOW", "VERY_LOW"]


def get_context_from_env_var() -> dict[Any, Any]:
    """
    Extract context from env variable, (dag_id, task_id, etc) for use in BashOperator and PythonOperator.

    :return: The context of interest.
    """
    return {
        format_map["default"]: os.environ.get(format_map["env_var_format"], "")
        for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()
    }


class HiveCliHook(BaseHook):
    """
    Simple wrapper around the hive CLI.

    It also supports the ``beeline``
    a lighter CLI that runs JDBC and is replacing the heavier
    traditional CLI. To enable ``beeline``, set the use_beeline param in the
    extra field of your connection as in ``{ "use_beeline": true }``

    Note that you can also set default hive CLI parameters by passing ``hive_cli_params``
    space separated list of parameters to add to the hive command.

    The extra connection parameter ``auth`` gets passed as in the ``jdbc``
    connection string as is.

    :param hive_cli_conn_id: Reference to the
        :ref:`Hive CLI connection id <howto/connection:hive_cli>`.
    :param mapred_queue: queue used by the Hadoop Scheduler (Capacity or Fair)
    :param mapred_queue_priority: priority within the job queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    :param hive_cli_params: Space separated list of hive command parameters to add to the
        hive command.
    :param proxy_user: Run HQL code as this user.
    """

    conn_name_attr = "hive_cli_conn_id"
    default_conn_name = "hive_cli_default"
    conn_type = "hive_cli"
    hook_name = "Hive Client Wrapper"

    def __init__(
        self,
        hive_cli_conn_id: str = default_conn_name,
        mapred_queue: str | None = None,
        mapred_queue_priority: str | None = None,
        mapred_job_name: str | None = None,
        hive_cli_params: str = "",
        auth: str | None = None,
        proxy_user: str | None = None,
    ) -> None:
        super().__init__()
        conn = self.get_connection(hive_cli_conn_id)
        self.hive_cli_params: str = hive_cli_params
        self.use_beeline: bool = conn.extra_dejson.get("use_beeline", False)
        self.auth = auth
        self.conn = conn
        self.sub_process: Any = None
        if mapred_queue_priority:
            mapred_queue_priority = mapred_queue_priority.upper()
            if mapred_queue_priority not in HIVE_QUEUE_PRIORITIES:
                raise AirflowException(
                    f"Invalid Mapred Queue Priority. Valid values are: {', '.join(HIVE_QUEUE_PRIORITIES)}"
                )

        self.mapred_queue = mapred_queue or conf.get("hive", "default_hive_mapred_queue")
        self.mapred_queue_priority = mapred_queue_priority
        self.mapred_job_name = mapred_job_name
        self.proxy_user = proxy_user
        self.high_availability = self.conn.extra_dejson.get("high_availability", False)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to Hive Client Wrapper connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField

        return {
            "use_beeline": BooleanField(lazy_gettext("Use Beeline"), default=True),
            "proxy_user": StringField(lazy_gettext("Proxy User"), widget=BS3TextFieldWidget(), default=""),
            "principal": StringField(
                lazy_gettext("Principal"), widget=BS3TextFieldWidget(), default="hive/_HOST@EXAMPLE.COM"
            ),
            "high_availability": BooleanField(lazy_gettext("High Availability mode"), default=False),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Hive Client Wrapper connection."""
        return {
            "hidden_fields": ["extra"],
            "relabeling": {},
        }

    def _get_proxy_user(self) -> str:
        """Set the proper proxy_user value in case the user overwrite the default."""
        conn = self.conn
        if self.proxy_user is not None:
            return f"hive.server2.proxy.user={self.proxy_user}"
        proxy_user_value: str = conn.extra_dejson.get("proxy_user", "")
        if proxy_user_value != "":
            return f"hive.server2.proxy.user={proxy_user_value}"
        return ""

    def _prepare_cli_cmd(self) -> list[Any]:
        """Create the command list from available information."""
        conn = self.conn
        hive_bin = "hive"
        cmd_extra = []

        if self.use_beeline:
            hive_bin = "beeline"
            self._validate_beeline_parameters(conn)
            if self.high_availability:
                jdbc_url = f"jdbc:hive2://{conn.host}/{conn.schema}"
                self.log.info("High Availability selected, setting JDBC url as %s", jdbc_url)
            else:
                jdbc_url = f"jdbc:hive2://{conn.host}:{conn.port}/{conn.schema}"
                self.log.info("High Availability not selected, setting JDBC url as %s", jdbc_url)
            if conf.get("core", "security") == "kerberos":
                template = conn.extra_dejson.get("principal", "hive/_HOST@EXAMPLE.COM")
                if "_HOST" in template:
                    template = utils.replace_hostname_pattern(utils.get_components(template))
                proxy_user = self._get_proxy_user()
                if ";" in template:
                    raise RuntimeError("The principal should not contain the ';' character")
                if ";" in proxy_user:
                    raise RuntimeError("The proxy_user should not contain the ';' character")
                jdbc_url += f";principal={template};{proxy_user}"
                if self.high_availability:
                    if not jdbc_url.endswith(";"):
                        jdbc_url += ";"
                    jdbc_url += "serviceDiscoveryMode=zooKeeper;ssl=true;zooKeeperNamespace=hiveserver2"
            elif self.auth:
                jdbc_url += ";auth=" + self.auth

            jdbc_url = f'"{jdbc_url}"'

            cmd_extra += ["-u", jdbc_url]
            if conn.login:
                cmd_extra += ["-n", conn.login]
            if conn.password:
                cmd_extra += ["-p", conn.password]

        hive_params_list = self.hive_cli_params.split()

        return [hive_bin, *cmd_extra, *hive_params_list]

    def _validate_beeline_parameters(self, conn):
        if self.high_availability:
            if ";" in conn.schema:
                raise ValueError(
                    f"The schema used in beeline command ({conn.schema}) should not contain ';' character)"
                )
            return
        if ":" in conn.host or "/" in conn.host or ";" in conn.host:
            raise ValueError(
                f"The host used in beeline command ({conn.host}) should not contain ':/;' characters)"
            )
        try:
            int_port = int(conn.port)
            if not 0 < int_port <= 65535:
                raise ValueError(
                    f"The port used in beeline command ({conn.port}) should be in range 0-65535)"
                )
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"The port used in beeline command ({conn.port}) should be a valid integer: {e})"
            )
        if ";" in conn.schema:
            raise ValueError(
                f"The schema used in beeline command ({conn.schema}) should not contain ';' character)"
            )

    @staticmethod
    def _prepare_hiveconf(d: dict[Any, Any]) -> list[Any]:
        """
        Prepare a list of hiveconf params from a dictionary of key value pairs.

        :param d:

        >>> hh = HiveCliHook()
        >>> hive_conf = {"hive.exec.dynamic.partition": "true",
        ... "hive.exec.dynamic.partition.mode": "nonstrict"}
        >>> hh._prepare_hiveconf(hive_conf)
        ["-hiveconf", "hive.exec.dynamic.partition=true",\
        "-hiveconf", "hive.exec.dynamic.partition.mode=nonstrict"]
        """
        if not d:
            return []
        return as_flattened_list(zip(["-hiveconf"] * len(d), [f"{k}={v}" for k, v in d.items()]))

    def run_cli(
        self,
        hql: str,
        schema: str | None = None,
        verbose: bool = True,
        hive_conf: dict[Any, Any] | None = None,
    ) -> Any:
        """
        Run an hql statement using the hive cli.

        If hive_conf is specified it should be a dict and the entries
        will be set as key/value pairs in HiveConf.

        :param hql: an hql (hive query language) statement to run with hive cli
        :param schema: Name of hive schema (database) to use
        :param verbose: Provides additional logging. Defaults to True.
        :param hive_conf: if specified these key value pairs will be passed
            to hive as ``-hiveconf "key"="value"``. Note that they will be
            passed after the ``hive_cli_params`` and thus will override
            whatever values are specified in the database.

        >>> hh = HiveCliHook()
        >>> result = hh.run_cli("USE airflow;")
        >>> ("OK" in result)
        True
        """
        conn = self.conn
        schema = schema or conn.schema or ""

        invalid_chars_list = re.findall(r"[^a-z0-9_]", schema)
        if invalid_chars_list:
            invalid_chars = "".join(invalid_chars_list)
            raise RuntimeError(f"The schema `{schema}` contains invalid characters: {invalid_chars}")

        if schema:
            hql = f"USE {schema};\n{hql}"

        with TemporaryDirectory(prefix="airflow_hiveop_") as tmp_dir, NamedTemporaryFile(dir=tmp_dir) as f:
            hql += "\n"
            f.write(hql.encode("UTF-8"))
            f.flush()
            hive_cmd = self._prepare_cli_cmd()
            env_context = get_context_from_env_var()
            # Only extend the hive_conf if it is defined.
            if hive_conf:
                env_context.update(hive_conf)
            hive_conf_params = self._prepare_hiveconf(env_context)
            if self.mapred_queue:
                hive_conf_params.extend(
                    [
                        "-hiveconf",
                        f"mapreduce.job.queuename={self.mapred_queue}",
                        "-hiveconf",
                        f"mapred.job.queue.name={self.mapred_queue}",
                        "-hiveconf",
                        f"tez.queue.name={self.mapred_queue}",
                    ]
                )

            if self.mapred_queue_priority:
                hive_conf_params.extend(["-hiveconf", f"mapreduce.job.priority={self.mapred_queue_priority}"])

            if self.mapred_job_name:
                hive_conf_params.extend(["-hiveconf", f"mapred.job.name={self.mapred_job_name}"])

            hive_cmd.extend(hive_conf_params)
            hive_cmd.extend(["-f", f.name])

            if verbose:
                self.log.info("%s", " ".join(hive_cmd))
            sub_process: Any = subprocess.Popen(
                hive_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=tmp_dir, close_fds=True
            )
            self.sub_process = sub_process
            stdout = ""
            for line in iter(sub_process.stdout.readline, b""):
                line = line.decode()
                stdout += line
                if verbose:
                    self.log.info(line.strip())
            sub_process.wait()

            if sub_process.returncode:
                raise AirflowException(stdout)

            return stdout

    def test_hql(self, hql: str) -> None:
        """Test an hql statement using the hive cli and EXPLAIN."""
        create, insert, other = [], [], []
        for query in hql.split(";"):  # naive
            query_original = query
            query = query.lower().strip()

            if query.startswith("create table"):
                create.append(query_original)
            elif query.startswith(("set ", "add jar ", "create temporary function")):
                other.append(query_original)
            elif query.startswith("insert"):
                insert.append(query_original)
        other_ = ";".join(other)
        for query_set in [create, insert]:
            for query in query_set:
                query_preview = " ".join(query.split())[:50]
                self.log.info("Testing HQL [%s (...)]", query_preview)
                if query_set == insert:
                    query = other_ + "; explain " + query
                else:
                    query = "explain " + query
                try:
                    self.run_cli(query, verbose=False)
                except AirflowException as e:
                    message = e.args[0].splitlines()[-2]
                    self.log.info(message)
                    error_loc = re.search(r"(\d+):(\d+)", message)
                    if error_loc:
                        lst = int(error_loc.group(1))
                        begin = max(lst - 2, 0)
                        end = min(lst + 3, len(query.splitlines()))
                        context = "\n".join(query.splitlines()[begin:end])
                        self.log.info("Context :\n %s", context)
                else:
                    self.log.info("SUCCESS")

    def load_df(
        self,
        df: pd.DataFrame,
        table: str,
        field_dict: dict[Any, Any] | None = None,
        delimiter: str = ",",
        encoding: str = "utf8",
        pandas_kwargs: Any = None,
        **kwargs: Any,
    ) -> None:
        """
        Load a pandas DataFrame into hive.

        Hive data types will be inferred if not passed but column names will
        not be sanitized.

        :param df: DataFrame to load into a Hive table
        :param table: target Hive table, use dot notation to target a
            specific database
        :param field_dict: mapping from column name to hive data type.
            Note that Python dict is ordered so it keeps columns' order.
        :param delimiter: field delimiter in the file
        :param encoding: str encoding to use when writing DataFrame to file
        :param pandas_kwargs: passed to DataFrame.to_csv
        :param kwargs: passed to self.load_file
        """

        def _infer_field_types_from_df(df: pd.DataFrame) -> dict[Any, Any]:
            dtype_kind_hive_type = {
                "b": "BOOLEAN",  # boolean
                "i": "BIGINT",  # signed integer
                "u": "BIGINT",  # unsigned integer
                "f": "DOUBLE",  # floating-point
                "c": "STRING",  # complex floating-point
                "M": "TIMESTAMP",  # datetime
                "O": "STRING",  # object
                "S": "STRING",  # (byte-)string
                "U": "STRING",  # Unicode
                "V": "STRING",  # void
            }

            order_type = {}
            for col, dtype in df.dtypes.items():
                order_type[col] = dtype_kind_hive_type[dtype.kind]
            return order_type

        if pandas_kwargs is None:
            pandas_kwargs = {}

        with (
            TemporaryDirectory(prefix="airflow_hiveop_") as tmp_dir,
            NamedTemporaryFile(dir=tmp_dir, mode="w") as f,
        ):
            if field_dict is None:
                field_dict = _infer_field_types_from_df(df)

            df.to_csv(
                path_or_buf=f,
                sep=delimiter,
                header=False,
                index=False,
                encoding=encoding,
                date_format="%Y-%m-%d %H:%M:%S",
                **pandas_kwargs,
            )
            f.flush()

            return self.load_file(
                filepath=f.name, table=table, delimiter=delimiter, field_dict=field_dict, **kwargs
            )

    def load_file(
        self,
        filepath: str,
        table: str,
        delimiter: str = ",",
        field_dict: dict[Any, Any] | None = None,
        create: bool = True,
        overwrite: bool = True,
        partition: dict[str, Any] | None = None,
        recreate: bool = False,
        tblproperties: dict[str, Any] | None = None,
    ) -> None:
        """
        Load a local file into Hive.

        Note that the table generated in Hive uses ``STORED AS textfile``
        which isn't the most efficient serialization format. If a
        large amount of data is loaded and/or if the tables gets
        queried considerably, you may want to use this operator only to
        stage the data into a temporary table before loading it into its
        final destination using a ``HiveOperator``.

        :param filepath: local filepath of the file to load
        :param table: target Hive table, use dot notation to target a
            specific database
        :param delimiter: field delimiter in the file
        :param field_dict: A dictionary of the fields name in the file
            as keys and their Hive types as values.
            Note that Python dict is ordered so it keeps columns' order.
        :param create: whether to create the table if it doesn't exist
        :param overwrite: whether to overwrite the data in table or partition
        :param partition: target partition as a dict of partition columns
            and values
        :param recreate: whether to drop and recreate the table at every
            execution
        :param tblproperties: TBLPROPERTIES of the hive table being created
        """
        hql = ""
        if recreate:
            hql += f"DROP TABLE IF EXISTS {table};\n"
        if create or recreate:
            if field_dict is None:
                raise ValueError("Must provide a field dict when creating a table")
            fields = ",\n    ".join(f"`{k.strip('`')}` {v}" for k, v in field_dict.items())
            hql += f"CREATE TABLE IF NOT EXISTS {table} (\n{fields})\n"
            if partition:
                pfields = ",\n    ".join(p + " STRING" for p in partition)
                hql += f"PARTITIONED BY ({pfields})\n"
            hql += "ROW FORMAT DELIMITED\n"
            hql += f"FIELDS TERMINATED BY '{delimiter}'\n"
            hql += "STORED AS textfile\n"
            if tblproperties is not None:
                tprops = ", ".join(f"'{k}'='{v}'" for k, v in tblproperties.items())
                hql += f"TBLPROPERTIES({tprops})\n"
            hql += ";"
            self.log.info(hql)
            self.run_cli(hql)
        hql = f"LOAD DATA LOCAL INPATH '{filepath}' "
        if overwrite:
            hql += "OVERWRITE "
        hql += f"INTO TABLE {table} "
        if partition:
            pvals = ", ".join(f"{k}='{v}'" for k, v in partition.items())
            hql += f"PARTITION ({pvals})"

        # Add a newline character as a workaround for https://issues.apache.org/jira/browse/HIVE-10541,
        hql += ";\n"

        self.log.info(hql)
        self.run_cli(hql)

    def kill(self) -> None:
        """Kill Hive cli command."""
        if hasattr(self, "sub_process"):
            if self.sub_process.poll() is None:
                print("Killing the Hive job")
                self.sub_process.terminate()
                time.sleep(60)
                self.sub_process.kill()


class HiveMetastoreHook(BaseHook):
    """
    Wrapper to interact with the Hive Metastore.

    :param metastore_conn_id: reference to the
        :ref: `metastore thrift service connection id <howto/connection:hive_metastore>`.
    """

    # java short max val
    MAX_PART_COUNT = 32767

    conn_name_attr = "metastore_conn_id"
    default_conn_name = "metastore_default"
    conn_type = "hive_metastore"
    hook_name = "Hive Metastore Thrift"

    def __init__(self, metastore_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn = self.get_connection(metastore_conn_id)
        self.metastore = self.get_metastore_client()

    def __getstate__(self) -> dict[str, Any]:
        # This is for pickling to work despite the thrift hive client not
        # being picklable
        """Serialize object and omit non-serializable attributes."""
        state = dict(self.__dict__)
        del state["metastore"]
        return state

    def __setstate__(self, d: dict[str, Any]) -> None:
        """Deserialize object and restore non-serializable attributes."""
        self.__dict__.update(d)
        self.__dict__["metastore"] = self.get_metastore_client()

    def get_metastore_client(self) -> Any:
        """Return a Hive thrift client."""
        import hmsclient
        from thrift.protocol import TBinaryProtocol
        from thrift.transport import TSocket, TTransport

        host = self._find_valid_host()
        conn = self.conn

        if not host:
            raise AirflowException("Failed to locate the valid server.")

        auth_mechanism = conn.extra_dejson.get("auth_mechanism", "NOSASL")

        if conf.get("core", "security") == "kerberos":
            auth_mechanism = conn.extra_dejson.get("auth_mechanism", "GSSAPI")
            kerberos_service_name = conn.extra_dejson.get("kerberos_service_name", "hive")

        conn_socket = TSocket.TSocket(host, conn.port)

        if conf.get("core", "security") == "kerberos" and auth_mechanism == "GSSAPI":
            try:
                import saslwrapper as sasl
            except ImportError:
                import sasl

            def sasl_factory() -> sasl.Client:
                sasl_client = sasl.Client()
                sasl_client.setAttr("host", host)
                sasl_client.setAttr("service", kerberos_service_name)
                sasl_client.init()
                return sasl_client

            from thrift_sasl import TSaslClientTransport

            transport = TSaslClientTransport(sasl_factory, "GSSAPI", conn_socket)
        else:
            transport = TTransport.TBufferedTransport(conn_socket)

        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        return hmsclient.HMSClient(iprot=protocol)

    def _find_valid_host(self) -> Any:
        conn = self.conn
        hosts = []
        if conn.host:
            hosts = conn.host.split(",")
        for host in hosts:
            host_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.log.info("Trying to connect to %s:%s", host, conn.port)
            if host_socket.connect_ex((host, conn.port)) == 0:
                self.log.info("Connected to %s:%s", host, conn.port)
                host_socket.close()
                return host
            self.log.error("Could not connect to %s:%s", host, conn.port)
        return None

    def get_conn(self) -> Any:
        return self.metastore

    def check_for_partition(self, schema: str, table: str, partition: str) -> bool:
        """
        Check whether a partition exists.

        :param schema: Name of hive schema (database) @table belongs to
        :param table: Name of hive table @partition belongs to
        :param partition: Expression that matches the partitions to check for (e.g. `a = 'b' AND c = 'd'`)

        >>> hh = HiveMetastoreHook()
        >>> t = "static_babynames_partitioned"
        >>> hh.check_for_partition("airflow", t, "ds='2015-01-01'")
        True
        """
        with self.metastore as client:
            partitions = client.get_partitions_by_filter(
                schema, table, partition, HiveMetastoreHook.MAX_PART_COUNT
            )

        return bool(partitions)

    def check_for_named_partition(self, schema: str, table: str, partition_name: str) -> Any:
        """
        Check whether a partition with a given name exists.

        :param schema: Name of hive schema (database) @table belongs to
        :param table: Name of hive table @partition belongs to
        :param partition_name: Name of the partitions to check for (eg `a=b/c=d`)

        >>> hh = HiveMetastoreHook()
        >>> t = "static_babynames_partitioned"
        >>> hh.check_for_named_partition("airflow", t, "ds=2015-01-01")
        True
        >>> hh.check_for_named_partition("airflow", t, "ds=xxx")
        False
        """
        with self.metastore as client:
            return client.check_for_named_partition(schema, table, partition_name)

    def get_table(self, table_name: str, db: str = "default") -> Any:
        """
        Get a metastore table object.

        >>> hh = HiveMetastoreHook()
        >>> t = hh.get_table(db="airflow", table_name="static_babynames")
        >>> t.tableName
        'static_babynames'
        >>> [col.name for col in t.sd.cols]
        ['state', 'year', 'name', 'gender', 'num']
        """
        if db == "default" and "." in table_name:
            db, table_name = table_name.split(".")[:2]
        with self.metastore as client:
            return client.get_table(dbname=db, tbl_name=table_name)

    def get_tables(self, db: str, pattern: str = "*") -> Any:
        """Get a metastore table object."""
        with self.metastore as client:
            tables = client.get_tables(db_name=db, pattern=pattern)
            return client.get_table_objects_by_name(db, tables)

    def get_databases(self, pattern: str = "*") -> Any:
        """Get a metastore table object."""
        with self.metastore as client:
            return client.get_databases(pattern)

    def get_partitions(self, schema: str, table_name: str, partition_filter: str | None = None) -> list[Any]:
        """
        Return a list of all partitions in a table.

        Works only for tables with less than 32767 (java short max val).
        For subpartitioned table, the number might easily exceed this.

        >>> hh = HiveMetastoreHook()
        >>> t = "static_babynames_partitioned"
        >>> parts = hh.get_partitions(schema="airflow", table_name=t)
        >>> len(parts)
        1
        >>> parts
        [{'ds': '2015-01-01'}]
        """
        with self.metastore as client:
            table = client.get_table(dbname=schema, tbl_name=table_name)
            if table.partitionKeys:
                if partition_filter:
                    parts = client.get_partitions_by_filter(
                        db_name=schema,
                        tbl_name=table_name,
                        filter=partition_filter,
                        max_parts=HiveMetastoreHook.MAX_PART_COUNT,
                    )
                else:
                    parts = client.get_partitions(
                        db_name=schema, tbl_name=table_name, max_parts=HiveMetastoreHook.MAX_PART_COUNT
                    )

                pnames = [p.name for p in table.partitionKeys]
                return [dict(zip(pnames, p.values)) for p in parts]
            raise AirflowException("The table isn't partitioned")

    @staticmethod
    def _get_max_partition_from_part_specs(
        part_specs: list[Any], partition_key: str | None, filter_map: dict[str, Any] | None
    ) -> Any:
        """
        Get max partition of partitions with partition_key from part specs.

        key:value pair in filter_map will be used to filter out partitions.

        :param part_specs: list of partition specs.
        :param partition_key: partition key name.
        :param filter_map: partition_key:partition_value map used for partition filtering,
                           e.g. {'key1': 'value1', 'key2': 'value2'}.
                           Only partitions matching all partition_key:partition_value
                           pairs will be considered as candidates of max partition.
        :return: Max partition or None if part_specs is empty.
        """
        if not part_specs:
            return None

        # Assuming all specs have the same keys.
        if partition_key not in part_specs[0].keys():
            raise AirflowException(f"Provided partition_key {partition_key} is not in part_specs.")
        if filter_map and not set(filter_map).issubset(part_specs[0]):
            raise AirflowException(
                f"Keys in provided filter_map {', '.join(filter_map.keys())} "
                f"are not subset of part_spec keys: {', '.join(part_specs[0].keys())}"
            )

        return max(
            (
                p_dict[partition_key]
                for p_dict in part_specs
                if filter_map is None or all(item in p_dict.items() for item in filter_map.items())
            ),
            default=None,
        )

    def max_partition(
        self,
        schema: str,
        table_name: str,
        field: str | None = None,
        filter_map: dict[Any, Any] | None = None,
    ) -> Any:
        """
        Return the maximum value for all partitions with given field in a table.

        If only one partition key exist in the table, the key will be used as field.
        filter_map should be a partition_key:partition_value map and will be used to
        filter out partitions.

        :param schema: schema name.
        :param table_name: table name.
        :param field: partition key to get max partition from.
        :param filter_map: partition_key:partition_value map used for partition filtering.

        >>> hh = HiveMetastoreHook()
        >>> filter_map = {'ds': '2015-01-01'}
        >>> t = 'static_babynames_partitioned'
        >>> hh.max_partition(schema='airflow',\
        ... table_name=t, field='ds', filter_map=filter_map)
        '2015-01-01'
        """
        with self.metastore as client:
            table = client.get_table(dbname=schema, tbl_name=table_name)
            key_name_set = {key.name for key in table.partitionKeys}
            if len(table.partitionKeys) == 1:
                field = table.partitionKeys[0].name
            elif not field:
                raise AirflowException("Please specify the field you want the max value for.")
            elif field not in key_name_set:
                raise AirflowException("Provided field is not a partition key.")

            if filter_map and not set(filter_map.keys()).issubset(key_name_set):
                raise AirflowException("Provided filter_map contains keys that are not partition key.")

            part_names = client.get_partition_names(
                schema, table_name, max_parts=HiveMetastoreHook.MAX_PART_COUNT
            )
            part_specs = [client.partition_name_to_spec(part_name) for part_name in part_names]

        return HiveMetastoreHook._get_max_partition_from_part_specs(part_specs, field, filter_map)

    def table_exists(self, table_name: str, db: str = "default") -> bool:
        """
        Check if table exists.

        >>> hh = HiveMetastoreHook()
        >>> hh.table_exists(db="airflow", table_name="static_babynames")
        True
        >>> hh.table_exists(db="airflow", table_name="does_not_exist")
        False
        """
        try:
            self.get_table(table_name, db)
            return True
        except Exception:
            return False

    def drop_partitions(self, table_name, part_vals, delete_data=False, db="default"):
        """
        Drop partitions from the given table matching the part_vals input.

        :param table_name: table name.
        :param part_vals: list of partition specs.
        :param delete_data: Setting to control if underlying data have to deleted
                            in addition to dropping partitions.
        :param db: Name of hive schema (database) @table belongs to

        >>> hh = HiveMetastoreHook()
        >>> hh.drop_partitions(db='airflow', table_name='static_babynames',
        part_vals="['2020-05-01']")
        True
        """
        if self.table_exists(table_name, db):
            with self.metastore as client:
                self.log.info(
                    "Dropping partition of table %s.%s matching the spec: %s", db, table_name, part_vals
                )
                return client.drop_partition(db, table_name, part_vals, delete_data)
        else:
            self.log.info("Table %s.%s does not exist!", db, table_name)
            return False


class HiveServer2Hook(DbApiHook):
    """
    Wrapper around the pyhive library.

    Notes:
    * the default auth_mechanism is PLAIN, to override it you
    can specify it in the ``extra`` of your connection in the UI
    * the default for run_set_variable_statements is true, if you
    are using impala you may need to set it to false in the
    ``extra`` of your connection in the UI

    :param hiveserver2_conn_id: Reference to the
        :ref: `Hive Server2 thrift service connection id <howto/connection:hiveserver2>`.
    :param schema: Hive database name.
    """

    conn_name_attr = "hiveserver2_conn_id"
    default_conn_name = "hiveserver2_default"
    conn_type = "hiveserver2"
    hook_name = "Hive Server 2 Thrift"
    supports_autocommit = False

    def get_conn(self, schema: str | None = None) -> Any:
        """Return a Hive connection object."""
        username: str | None = None
        password: str | None = None

        db = self.get_connection(self.get_conn_id())

        auth_mechanism = db.extra_dejson.get("auth_mechanism", "NONE")
        if auth_mechanism == "NONE" and db.login is None:
            # we need to give a username
            username = "airflow"
        kerberos_service_name = None
        if conf.get("core", "security") == "kerberos":
            auth_mechanism = db.extra_dejson.get("auth_mechanism", "KERBEROS")
            kerberos_service_name = db.extra_dejson.get("kerberos_service_name", "hive")

        # Password should be set if and only if in LDAP or CUSTOM mode
        if auth_mechanism in ("LDAP", "CUSTOM"):
            password = db.password

        from pyhive.hive import connect

        return connect(
            host=db.host,
            port=db.port,
            auth=auth_mechanism,
            kerberos_service_name=kerberos_service_name,
            username=db.login or username,
            password=password,
            database=schema or db.schema or "default",
        )

    def _get_results(
        self,
        sql: str | list[str],
        schema: str = "default",
        fetch_size: int | None = None,
        hive_conf: Iterable | Mapping | None = None,
    ) -> Any:
        from pyhive.exc import ProgrammingError

        if isinstance(sql, str):
            sql = [sql]
        previous_description = None
        with contextlib.closing(self.get_conn(schema)) as conn, contextlib.closing(conn.cursor()) as cur:
            cur.arraysize = fetch_size or 1000

            db = self.get_connection(self.get_conn_id())
            # Not all query services (e.g. impala) support the set command
            if db.extra_dejson.get("run_set_variable_statements", True):
                env_context = get_context_from_env_var()
                if hive_conf:
                    env_context.update(hive_conf)
                for k, v in env_context.items():
                    cur.execute(f"set {k}={v}")

            for statement in sql:
                cur.execute(statement)
                # we only get results of statements that returns
                lowered_statement = statement.lower().strip()
                if lowered_statement.startswith(("select", "with", "show")) or (
                    lowered_statement.startswith("set") and "=" not in lowered_statement
                ):
                    description = cur.description
                    if previous_description and previous_description != description:
                        message = f"""The statements are producing different descriptions:
                                     Current: {description!r}
                                     Previous: {previous_description!r}"""
                        raise ValueError(message)
                    elif not previous_description:
                        previous_description = description
                        yield description
                    try:
                        # DB API 2 raises when no results are returned
                        # we're silencing here as some statements in the list
                        # may be `SET` or DDL
                        yield from cur
                    except ProgrammingError:
                        self.log.debug("get_results returned no records")

    def get_results(
        self,
        sql: str | list[str],
        schema: str = "default",
        fetch_size: int | None = None,
        hive_conf: Iterable | Mapping | None = None,
    ) -> dict[str, Any]:
        """
        Get results of the provided hql in target schema.

        :param sql: hql to be executed.
        :param schema: target schema, default to 'default'.
        :param fetch_size: max size of result to fetch.
        :param hive_conf: hive_conf to execute alone with the hql.
        :return: results of hql execution, dict with data (list of results) and header
        """
        results_iter = self._get_results(sql, schema, fetch_size=fetch_size, hive_conf=hive_conf)
        header = next(results_iter)
        results = {"data": list(results_iter), "header": header}
        return results

    def to_csv(
        self,
        sql: str,
        csv_filepath: str,
        schema: str = "default",
        delimiter: str = ",",
        lineterminator: str = "\r\n",
        output_header: bool = True,
        fetch_size: int = 1000,
        hive_conf: dict[Any, Any] | None = None,
    ) -> None:
        """
        Execute hql in target schema and write results to a csv file.

        :param sql: hql to be executed.
        :param csv_filepath: filepath of csv to write results into.
        :param schema: target schema, default to 'default'.
        :param delimiter: delimiter of the csv file, default to ','.
        :param lineterminator: lineterminator of the csv file.
        :param output_header: header of the csv file, default to True.
        :param fetch_size: number of result rows to write into the csv file, default to 1000.
        :param hive_conf: hive_conf to execute alone with the hql.

        """
        results_iter = self._get_results(sql, schema, fetch_size=fetch_size, hive_conf=hive_conf)
        header = next(results_iter)
        message = None

        i = 0
        with open(csv_filepath, "w", encoding="utf-8") as file:
            writer = csv.writer(file, delimiter=delimiter, lineterminator=lineterminator)
            try:
                if output_header:
                    self.log.debug("Cursor description is %s", header)
                    writer.writerow([c[0] for c in header])

                for i, row in enumerate(results_iter, 1):
                    writer.writerow(row)
                    if i % fetch_size == 0:
                        self.log.info("Written %s rows so far.", i)
            except ValueError as exception:
                message = str(exception)

        if message:
            # need to clean up the file first
            os.remove(csv_filepath)
            raise ValueError(message)

        self.log.info("Done. Loaded a total of %s rows.", i)

    def get_records(
        self, sql: str | list[str], parameters: Iterable | Mapping[str, Any] | None = None, **kwargs
    ) -> Any:
        """
        Get a set of records from a Hive query; optionally pass a 'schema' kwarg to specify target schema.

        :param sql: hql to be executed.
        :param parameters: optional configuration passed to get_results
        :return: result of hive execution

        >>> hh = HiveServer2Hook()
        >>> sql = "SELECT * FROM airflow.static_babynames LIMIT 100"
        >>> len(hh.get_records(sql))
        100
        """
        schema = kwargs["schema"] if "schema" in kwargs else "default"
        return self.get_results(sql, schema=schema, hive_conf=parameters)["data"]

    def _get_pandas_df(
        self,
        sql,
        parameters: list[Any] | tuple[Any, ...] | Mapping[str, Any] | None = None,
        schema: str = "default",
        hive_conf: dict[Any, Any] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        try:
            import pandas as pd
        except ImportError as e:
            from airflow.exceptions import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(e)

        res = self.get_results(sql, schema=schema, hive_conf=hive_conf)
        df = pd.DataFrame(res["data"], columns=[c[0] for c in res["header"]], **kwargs)
        return df

    def _get_polars_df(
        self,
        sql,
        parameters: list[Any] | tuple[Any, ...] | Mapping[str, Any] | None = None,
        schema: str = "default",
        hive_conf: dict[Any, Any] | None = None,
        **kwargs,
    ) -> pl.DataFrame:
        try:
            import polars as pl
        except ImportError as e:
            from airflow.exceptions import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(e)

        res = self.get_results(sql, schema=schema, hive_conf=hive_conf)
        df = pl.DataFrame(res["data"], schema=[c[0] for c in res["header"]], orient="row", **kwargs)
        return df

    @overload  # type: ignore[override]
    def get_df(
        self,
        sql: str,
        schema: str = "default",
        hive_conf: dict[Any, Any] | None = None,
        *,
        df_type: Literal["pandas"] = "pandas",
        **kwargs: Any,
    ) -> pd.DataFrame: ...

    @overload
    def get_df(
        self,
        sql: str,
        schema: str = "default",
        hive_conf: dict[Any, Any] | None = None,
        *,
        df_type: Literal["polars"],
        **kwargs: Any,
    ) -> pl.DataFrame: ...

    def get_df(
        self,
        sql: str,
        schema: str = "default",
        hive_conf: dict[Any, Any] | None = None,
        *,
        df_type: Literal["pandas", "polars"] = "pandas",
        **kwargs,
    ) -> pd.DataFrame | pl.DataFrame:
        """
        Get a pandas / polars dataframe from a Hive query.

        :param sql: hql to be executed.
        :param schema: target schema, default to 'default'.
        :param hive_conf: hive_conf to execute alone with the hql.
        :param df_type: type of dataframe to return, either 'pandas' or 'polars'
        :param kwargs: (optional) passed into pandas.DataFrame constructor
        :return: result of hive execution

        >>> hh = HiveServer2Hook()
        >>> sql = "SELECT * FROM airflow.static_babynames LIMIT 100"
        >>> df = hh.get_df(sql, df_type="pandas")
        >>> len(df.index)
        100

        :return: pandas.DateFrame | polars.DataFrame
        """
        if df_type == "pandas":
            return self._get_pandas_df(sql, schema=schema, hive_conf=hive_conf, **kwargs)
        if df_type == "polars":
            return self._get_polars_df(sql, schema=schema, hive_conf=hive_conf, **kwargs)

    @deprecated(
        reason="Replaced by function `get_df`.",
        category=AirflowProviderDeprecationWarning,
        action="ignore",
    )
    def get_pandas_df(  # type: ignore
        self,
        sql: str,
        schema: str = "default",
        hive_conf: dict[Any, Any] | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        return self._get_pandas_df(sql, schema=schema, hive_conf=hive_conf, **kwargs)
