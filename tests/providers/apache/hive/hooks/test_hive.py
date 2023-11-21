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

import pytest

from airflow import PY311

if PY311:
    pytest.skip(
        "The tests are skipped because Apache Hive provider is not supported on Python 3.11",
        allow_module_level=True,
    )

import datetime
import itertools
from collections import namedtuple
from unittest import mock

import pandas as pd
from hmsclient import HMSClient

from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.apache.hive.hooks.hive import HiveCliHook, HiveMetastoreHook, HiveServer2Hook
from airflow.secrets.environment_variables import CONN_ENV_PREFIX
from airflow.utils import timezone
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING
from tests.providers.apache.hive import (
    BaseMockConnectionCursor,
    InvalidHiveCliHook,
    MockHiveCliHook,
    MockHiveServer2Hook,
    MockSubProcess,
)
from tests.test_utils.asserts import assert_equal_ignore_multiple_spaces

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


class EmptyMockConnectionCursor(BaseMockConnectionCursor):
    def __init__(self):
        super().__init__()
        self.iterable = []


@pytest.mark.db_test
class TestHiveCliHook:
    @mock.patch("tempfile.tempdir", "/tmp/")
    @mock.patch("tempfile._RandomNameSequence.__next__")
    @mock.patch("subprocess.Popen")
    def test_run_cli(self, mock_popen, mock_temp_dir):
        mock_subprocess = MockSubProcess()
        mock_popen.return_value = mock_subprocess
        mock_temp_dir.return_value = "test_run_cli"

        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW_CTX_DAG_ID": "test_dag_id",
                "AIRFLOW_CTX_TASK_ID": "test_task_id",
                "AIRFLOW_CTX_EXECUTION_DATE": "2015-01-01T00:00:00+00:00",
                "AIRFLOW_CTX_TRY_NUMBER": "1",
                "AIRFLOW_CTX_DAG_RUN_ID": "55",
                "AIRFLOW_CTX_DAG_OWNER": "airflow",
                "AIRFLOW_CTX_DAG_EMAIL": "test@airflow.com",
            },
        ):
            hook = MockHiveCliHook()
            hook.run_cli("SHOW DATABASES")

        hive_cmd = [
            "beeline",
            "-u",
            '"jdbc:hive2://localhost:10000/default"',
            "-hiveconf",
            "airflow.ctx.dag_id=test_dag_id",
            "-hiveconf",
            "airflow.ctx.task_id=test_task_id",
            "-hiveconf",
            "airflow.ctx.execution_date=2015-01-01T00:00:00+00:00",
            "-hiveconf",
            "airflow.ctx.try_number=1",
            "-hiveconf",
            "airflow.ctx.dag_run_id=55",
            "-hiveconf",
            "airflow.ctx.dag_owner=airflow",
            "-hiveconf",
            "airflow.ctx.dag_email=test@airflow.com",
            "-hiveconf",
            "mapreduce.job.queuename=airflow",
            "-hiveconf",
            "mapred.job.queue.name=airflow",
            "-hiveconf",
            "tez.queue.name=airflow",
            "-f",
            "/tmp/airflow_hiveop_test_run_cli/tmptest_run_cli",
        ]

        mock_popen.assert_called_with(
            hive_cmd,
            stdout=mock_subprocess.PIPE,
            stderr=mock_subprocess.STDOUT,
            cwd="/tmp/airflow_hiveop_test_run_cli",
            close_fds=True,
        )

    def test_hive_cli_hook_invalid_schema(self):
        with pytest.raises(RuntimeError) as error:
            hook = InvalidHiveCliHook()
            hook.run_cli("SHOW DATABASES")

        assert str(error.value) == "The schema `default;` contains invalid characters: ;"

    @mock.patch("subprocess.Popen")
    def test_run_cli_with_hive_conf(self, mock_popen):
        hql = (
            "set key;\n"
            "set airflow.ctx.dag_id;\nset airflow.ctx.dag_run_id;\n"
            "set airflow.ctx.task_id;\nset airflow.ctx.execution_date;\n"
        )

        dag_id_ctx_var_name = AIRFLOW_VAR_NAME_FORMAT_MAPPING["AIRFLOW_CONTEXT_DAG_ID"]["env_var_format"]
        task_id_ctx_var_name = AIRFLOW_VAR_NAME_FORMAT_MAPPING["AIRFLOW_CONTEXT_TASK_ID"]["env_var_format"]
        execution_date_ctx_var_name = AIRFLOW_VAR_NAME_FORMAT_MAPPING["AIRFLOW_CONTEXT_EXECUTION_DATE"][
            "env_var_format"
        ]
        dag_run_id_ctx_var_name = AIRFLOW_VAR_NAME_FORMAT_MAPPING["AIRFLOW_CONTEXT_DAG_RUN_ID"][
            "env_var_format"
        ]

        mock_output = [
            "Connecting to jdbc:hive2://localhost:10000/default",
            "log4j:WARN No appenders could be found for logger (org.apache.hive.jdbc.Utils).",
            "log4j:WARN Please initialize the log4j system properly.",
            "log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.",
            "Connected to: Apache Hive (version 1.2.1.2.3.2.0-2950)",
            "Driver: Hive JDBC (version 1.2.1.spark2)",
            "Transaction isolation: TRANSACTION_REPEATABLE_READ",
            "0: jdbc:hive2://localhost:10000/default> USE default;",
            "No rows affected (0.37 seconds)",
            "0: jdbc:hive2://localhost:10000/default> set key;",
            "+------------+--+",
            "|    set     |",
            "+------------+--+",
            "| key=value  |",
            "+------------+--+",
            "1 row selected (0.133 seconds)",
            "0: jdbc:hive2://localhost:10000/default> set airflow.ctx.dag_id;",
            "+---------------------------------+--+",
            "|               set               |",
            "+---------------------------------+--+",
            "| airflow.ctx.dag_id=test_dag_id  |",
            "+---------------------------------+--+",
            "1 row selected (0.008 seconds)",
            "0: jdbc:hive2://localhost:10000/default> set airflow.ctx.dag_run_id;",
            "+-----------------------------------------+--+",
            "|                   set                   |",
            "+-----------------------------------------+--+",
            "| airflow.ctx.dag_run_id=test_dag_run_id  |",
            "+-----------------------------------------+--+",
            "1 row selected (0.007 seconds)",
            "0: jdbc:hive2://localhost:10000/default> set airflow.ctx.task_id;",
            "+-----------------------------------+--+",
            "|                set                |",
            "+-----------------------------------+--+",
            "| airflow.ctx.task_id=test_task_id  |",
            "+-----------------------------------+--+",
            "1 row selected (0.009 seconds)",
            "0: jdbc:hive2://localhost:10000/default> set airflow.ctx.execution_date;",
            "+-------------------------------------------------+--+",
            "|                       set                       |",
            "+-------------------------------------------------+--+",
            "| airflow.ctx.execution_date=test_execution_date  |",
            "+-------------------------------------------------+--+",
            "1 row selected (0.006 seconds)",
            "0: jdbc:hive2://localhost:10000/default> ",
            "0: jdbc:hive2://localhost:10000/default> ",
            "Closing: 0: jdbc:hive2://localhost:10000/default",
            "",
        ]

        with mock.patch.dict(
            "os.environ",
            {
                dag_id_ctx_var_name: "test_dag_id",
                task_id_ctx_var_name: "test_task_id",
                execution_date_ctx_var_name: "test_execution_date",
                dag_run_id_ctx_var_name: "test_dag_run_id",
            },
        ):
            hook = MockHiveCliHook()
            mock_popen.return_value = MockSubProcess(output=mock_output)

            output = hook.run_cli(hql=hql, hive_conf={"key": "value"})
            process_inputs = " ".join(mock_popen.call_args_list[0][0][0])

            assert "value" in process_inputs
            assert "test_dag_id" in process_inputs
            assert "test_task_id" in process_inputs
            assert "test_execution_date" in process_inputs
            assert "test_dag_run_id" in process_inputs

            assert "value" in output
            assert "test_dag_id" in output
            assert "test_task_id" in output
            assert "test_execution_date" in output
            assert "test_dag_run_id" in output

    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliHook.run_cli")
    def test_load_file_without_create_table(self, mock_run_cli):
        filepath = "/path/to/input/file"
        table = "output_table"

        hook = MockHiveCliHook()
        hook.load_file(filepath=filepath, table=table, create=False)

        query = f"LOAD DATA LOCAL INPATH '{filepath}' OVERWRITE INTO TABLE {table} ;\n"
        calls = [mock.call(query)]
        mock_run_cli.assert_has_calls(calls, any_order=True)

    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliHook.run_cli")
    def test_load_file_create_table(self, mock_run_cli):
        filepath = "/path/to/input/file"
        table = "output_table"
        field_dict = {"name": "string", "gender": "string"}
        fields = ",\n    ".join(f"`{k.strip('`')}` {v}" for k, v in field_dict.items())

        hook = MockHiveCliHook()
        hook.load_file(filepath=filepath, table=table, field_dict=field_dict, create=True, recreate=True)

        create_table = (
            f"DROP TABLE IF EXISTS {table};\n"
            f"CREATE TABLE IF NOT EXISTS {table} (\n{fields})\n"
            "ROW FORMAT DELIMITED\n"
            "FIELDS TERMINATED BY ','\n"
            "STORED AS textfile\n;"
        )

        load_data = f"LOAD DATA LOCAL INPATH '{filepath}' OVERWRITE INTO TABLE {table} ;\n"
        calls = [mock.call(create_table), mock.call(load_data)]
        mock_run_cli.assert_has_calls(calls, any_order=True)

    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliHook.load_file")
    @mock.patch("pandas.DataFrame.to_csv")
    def test_load_df(self, mock_to_csv, mock_load_file):
        df = pd.DataFrame({"c": ["foo", "bar", "baz"]})
        table = "t"
        delimiter = ","
        encoding = "utf-8"

        hook = MockHiveCliHook()
        hook.load_df(df=df, table=table, delimiter=delimiter, encoding=encoding)

        assert mock_to_csv.call_count == 1
        kwargs = mock_to_csv.call_args.kwargs
        assert kwargs["header"] is False
        assert kwargs["index"] is False
        assert kwargs["sep"] == delimiter

        assert mock_load_file.call_count == 1
        kwargs = mock_load_file.call_args.kwargs
        assert kwargs["delimiter"] == delimiter
        assert kwargs["field_dict"] == {"c": "STRING"}
        assert isinstance(kwargs["field_dict"], dict)
        assert kwargs["table"] == table

    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliHook.load_file")
    @mock.patch("pandas.DataFrame.to_csv")
    def test_load_df_with_optional_parameters(self, mock_to_csv, mock_load_file):
        hook = MockHiveCliHook()
        bools = (True, False)
        for create, recreate in itertools.product(bools, bools):
            mock_load_file.reset_mock()
            hook.load_df(df=pd.DataFrame({"c": range(10)}), table="t", create=create, recreate=recreate)

            assert mock_load_file.call_count == 1
            kwargs = mock_load_file.call_args.kwargs
            assert kwargs["create"] == create
            assert kwargs["recreate"] == recreate

    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliHook.run_cli")
    def test_load_df_with_data_types(self, mock_run_cli):
        ord_dict = {
            "b": [True],
            "i": [-1],
            "t": [1],
            "f": [0.0],
            "c": ["c"],
            "M": [datetime.datetime(2018, 1, 1)],
            "O": [object()],
            "S": [b"STRING"],
            "U": ["STRING"],
            "V": [None],
        }
        df = pd.DataFrame(ord_dict)

        hook = MockHiveCliHook()
        hook.load_df(df, "t")

        query = """
            CREATE TABLE IF NOT EXISTS t (
                `b` BOOLEAN,
                `i` BIGINT,
                `t` BIGINT,
                `f` DOUBLE,
                `c` STRING,
                `M` TIMESTAMP,
                `O` STRING,
                `S` STRING,
                `U` STRING,
                `V` STRING)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS textfile
            ;
        """
        assert_equal_ignore_multiple_spaces(mock_run_cli.call_args_list[0][0][0], query)


class TestHiveMetastoreHook:
    def setup_method(self):
        self.next_day = (DEFAULT_DATE + datetime.timedelta(days=1)).isoformat()[:10]
        self.database = "airflow"
        self.partition_by = "ds"
        self.table = "static_babynames_partitioned"
        with mock.patch(
            "airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook.get_metastore_client"
        ) as get_metastore_mock, mock.patch(
            "airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook.get_connection"
        ):
            get_metastore_mock.return_value = mock.MagicMock()

            self.hook = HiveMetastoreHook()

    VALID_FILTER_MAP = {"key2": "value2"}

    def test_get_max_partition_from_empty_part_specs(self):
        max_partition = HiveMetastoreHook._get_max_partition_from_part_specs(
            [], "key1", self.VALID_FILTER_MAP
        )
        assert max_partition is None

    def test_get_max_partition_from_valid_part_specs_and_invalid_filter_map(self):
        with pytest.raises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{"key1": "value1", "key2": "value2"}, {"key1": "value3", "key2": "value4"}],
                "key1",
                {"key3": "value5"},
            )

    def test_get_max_partition_from_valid_part_specs_and_invalid_partition_key(self):
        with pytest.raises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{"key1": "value1", "key2": "value2"}, {"key1": "value3", "key2": "value4"}],
                "key3",
                self.VALID_FILTER_MAP,
            )

    def test_get_max_partition_from_valid_part_specs_and_none_partition_key(self):
        with pytest.raises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{"key1": "value1", "key2": "value2"}, {"key1": "value3", "key2": "value4"}],
                None,
                self.VALID_FILTER_MAP,
            )

    def test_get_max_partition_from_valid_part_specs_and_none_filter_map(self):
        max_partition = HiveMetastoreHook._get_max_partition_from_part_specs(
            [{"key1": "value1", "key2": "value2"}, {"key1": "value3", "key2": "value4"}], "key1", None
        )

        # No partition will be filtered out.
        assert max_partition == "value3"

    def test_get_max_partition_from_valid_part_specs(self):
        max_partition = HiveMetastoreHook._get_max_partition_from_part_specs(
            [{"key1": "value1", "key2": "value2"}, {"key1": "value3", "key2": "value4"}],
            "key1",
            self.VALID_FILTER_MAP,
        )
        assert max_partition == "value1"

    def test_get_max_partition_from_valid_part_specs_return_type(self):
        max_partition = HiveMetastoreHook._get_max_partition_from_part_specs(
            [{"key1": "value1", "key2": "value2"}, {"key1": "value3", "key2": "value4"}],
            "key1",
            self.VALID_FILTER_MAP,
        )
        assert isinstance(max_partition, str)

    @mock.patch(
        "airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook._find_valid_host",
        return_value="localhost",
    )
    @mock.patch("airflow.providers.apache.hive.hooks.hive.socket")
    def test_error_metastore_client(self, socket_mock, _find_valid_host_mock):
        socket_mock.socket.return_value.connect_ex.return_value = 0
        self.hook.get_metastore_client()

    @mock.patch("airflow.providers.apache.hive.hooks.hive.socket")
    def test_ha_hosts(self, socket_mock):
        with mock.patch(
            "airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook.get_connection",
            return_value=Connection(host="metastore1.host,metastore2.host", port=9802),
        ):
            socket_mock.socket.return_value.connect_ex.return_value = 1
            with pytest.raises(AirflowException):
                HiveMetastoreHook()
            assert socket_mock.socket.call_count == 2

    def test_get_conn(self):
        with mock.patch(
            "airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook._find_valid_host"
        ) as find_valid_host, mock.patch(
            "airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook.get_connection"
        ) as get_connection:
            find_valid_host.return_value = mock.MagicMock(return_value="")
            get_connection.return_value = mock.MagicMock(return_value="")
            metastore_hook = HiveMetastoreHook()

        assert isinstance(metastore_hook.get_conn(), HMSClient)

    def test_check_for_partition(self):
        # Check for existent partition.
        FakePartition = namedtuple("FakePartition", ["values"])
        fake_partition = FakePartition(["2015-01-01"])

        metastore = self.hook.metastore.__enter__()

        partition = f"{self.partition_by}='{DEFAULT_DATE_DS}'"

        metastore.get_partitions_by_filter = mock.MagicMock(return_value=[fake_partition])

        assert self.hook.check_for_partition(self.database, self.table, partition)

        metastore.get_partitions_by_filter(
            self.database, self.table, partition, HiveMetastoreHook.MAX_PART_COUNT
        )

        # Check for non-existent partition.
        missing_partition = f"{self.partition_by}='{self.next_day}'"
        metastore.get_partitions_by_filter = mock.MagicMock(return_value=[])

        assert not self.hook.check_for_partition(self.database, self.table, missing_partition)

        metastore.get_partitions_by_filter.assert_called_with(
            self.database, self.table, missing_partition, HiveMetastoreHook.MAX_PART_COUNT
        )

    def test_check_for_named_partition(self):
        # Check for existing partition.

        partition = f"{self.partition_by}={DEFAULT_DATE_DS}"

        self.hook.metastore.__enter__().check_for_named_partition = mock.MagicMock(return_value=True)

        assert self.hook.check_for_named_partition(self.database, self.table, partition)

        self.hook.metastore.__enter__().check_for_named_partition.assert_called_with(
            self.database, self.table, partition
        )

        # Check for non-existent partition
        missing_partition = f"{self.partition_by}={self.next_day}"

        self.hook.metastore.__enter__().check_for_named_partition = mock.MagicMock(return_value=False)

        assert not self.hook.check_for_named_partition(self.database, self.table, missing_partition)
        self.hook.metastore.__enter__().check_for_named_partition.assert_called_with(
            self.database, self.table, missing_partition
        )

    def test_get_table(self):
        self.hook.metastore.__enter__().get_table = mock.MagicMock()
        self.hook.get_table(db=self.database, table_name=self.table)
        self.hook.metastore.__enter__().get_table.assert_called_with(
            dbname=self.database, tbl_name=self.table
        )

    def test_get_tables(self):  # static_babynames_partitioned
        self.hook.metastore.__enter__().get_tables = mock.MagicMock(
            return_value=["static_babynames_partitioned"]
        )

        self.hook.get_tables(db=self.database, pattern=self.table + "*")
        self.hook.metastore.__enter__().get_tables.assert_called_with(
            db_name="airflow", pattern="static_babynames_partitioned*"
        )

        self.hook.metastore.__enter__().get_table_objects_by_name.assert_called_with(
            "airflow", ["static_babynames_partitioned"]
        )

    def test_get_databases(self):
        metastore = self.hook.metastore.__enter__()
        metastore.get_databases = mock.MagicMock()

        self.hook.get_databases(pattern="*")
        metastore.get_databases.assert_called_with("*")

    def test_get_partitions(self):
        FakeFieldSchema = namedtuple("FakeFieldSchema", ["name"])
        fake_schema = FakeFieldSchema("ds")
        FakeTable = namedtuple("FakeTable", ["partitionKeys"])
        fake_table = FakeTable([fake_schema])
        FakePartition = namedtuple("FakePartition", ["values"])
        fake_partition = FakePartition(["2015-01-01"])

        metastore = self.hook.metastore.__enter__()
        metastore.get_table = mock.MagicMock(return_value=fake_table)

        metastore.get_partitions = mock.MagicMock(return_value=[fake_partition])

        partitions = self.hook.get_partitions(schema=self.database, table_name=self.table)
        assert len(partitions) == 1
        assert partitions == [{self.partition_by: DEFAULT_DATE_DS}]

        metastore.get_table.assert_called_with(dbname=self.database, tbl_name=self.table)
        metastore.get_partitions.assert_called_with(
            db_name=self.database, tbl_name=self.table, max_parts=HiveMetastoreHook.MAX_PART_COUNT
        )

    def test_max_partition(self):
        FakeFieldSchema = namedtuple("FakeFieldSchema", ["name"])
        fake_schema = FakeFieldSchema("ds")
        FakeTable = namedtuple("FakeTable", ["partitionKeys"])
        fake_table = FakeTable([fake_schema])

        metastore = self.hook.metastore.__enter__()
        metastore.get_table = mock.MagicMock(return_value=fake_table)

        metastore.get_partition_names = mock.MagicMock(return_value=["ds=2015-01-01"])
        metastore.partition_name_to_spec = mock.MagicMock(return_value={"ds": "2015-01-01"})

        filter_map = {self.partition_by: DEFAULT_DATE_DS}
        partition = self.hook.max_partition(
            schema=self.database, table_name=self.table, field=self.partition_by, filter_map=filter_map
        )
        assert partition == DEFAULT_DATE_DS

        metastore.get_table.assert_called_with(dbname=self.database, tbl_name=self.table)
        metastore.get_partition_names.assert_called_with(
            self.database, self.table, max_parts=HiveMetastoreHook.MAX_PART_COUNT
        )
        metastore.partition_name_to_spec.assert_called_with("ds=2015-01-01")

    def test_table_exists(self):
        # Test with existent table.
        self.hook.metastore.__enter__().get_table = mock.MagicMock(return_value=True)

        assert self.hook.table_exists(self.table, db=self.database)
        self.hook.metastore.__enter__().get_table.assert_called_with(
            dbname="airflow", tbl_name="static_babynames_partitioned"
        )

        # Test with non-existent table.
        self.hook.metastore.__enter__().get_table = mock.MagicMock(side_effect=Exception())

        assert not self.hook.table_exists("does-not-exist")
        self.hook.metastore.__enter__().get_table.assert_called_with(
            dbname="default", tbl_name="does-not-exist"
        )

    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook.table_exists")
    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook.get_metastore_client")
    def test_drop_partition(self, get_metastore_client_mock, table_exist_mock):
        metastore_mock = get_metastore_client_mock.return_value
        table_exist_mock.return_value = True
        ret = self.hook.drop_partitions(self.table, db=self.database, part_vals=[DEFAULT_DATE_DS])
        table_exist_mock.assert_called_once_with(self.table, self.database)
        assert metastore_mock.drop_partition(self.table, db=self.database, part_vals=[DEFAULT_DATE_DS]), ret


@pytest.mark.db_test
class TestHiveServer2Hook:
    def _upload_dataframe(self):
        df = pd.DataFrame({"a": [1, 2], "b": [1, 2]})
        self.local_path = "/tmp/TestHiveServer2Hook.csv"
        df.to_csv(self.local_path, header=False, index=False)

    def setup_method(self):
        self._upload_dataframe()
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", default_args=args)
        self.database = "airflow"
        self.table = "hive_server_hook"

        self.hql = """
        CREATE DATABASE IF NOT EXISTS {{ params.database }};
        USE {{ params.database }};
        DROP TABLE IF EXISTS {{ params.table }};
        CREATE TABLE IF NOT EXISTS {{ params.table }} (
            a int,
            b int)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ',';
        LOAD DATA LOCAL INPATH '{{ params.csv_path }}'
        OVERWRITE INTO TABLE {{ params.table }};
        """
        self.columns = [f"{self.table}.a", f"{self.table}.b"]

        with mock.patch(
            "airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook.get_metastore_client"
        ) as get_metastore_mock:
            get_metastore_mock.return_value = mock.MagicMock()

            self.hook = HiveMetastoreHook()

    def test_get_conn(self):
        hook = MockHiveServer2Hook()
        hook.get_conn()

    @mock.patch("pyhive.hive.connect")
    def test_get_conn_with_password(self, mock_connect):
        conn_id = "conn_with_password"
        conn_env = CONN_ENV_PREFIX + conn_id.upper()

        with mock.patch.dict(
            "os.environ",
            {conn_env: "jdbc+hive2://conn_id:conn_pass@localhost:10000/default?auth_mechanism=LDAP"},
        ):
            HiveServer2Hook(hiveserver2_conn_id=conn_id).get_conn()
            mock_connect.assert_called_once_with(
                host="localhost",
                port=10000,
                auth="LDAP",
                kerberos_service_name=None,
                username="conn_id",
                password="conn_pass",
                database="default",
            )

    @pytest.mark.parametrize(
        "host, port, schema, message",
        [
            ("localhost", "10000", "default", None),
            ("localhost:", "10000", "default", "The host used in beeline command"),
            (";ocalhost", "10000", "default", "The host used in beeline command"),
            (";ocalho/", "10000", "default", "The host used in beeline command"),
            ("localhost", "as", "default", "The port used in beeline command"),
            ("localhost", "0;", "default", "The port used in beeline command"),
            ("localhost", "10/", "default", "The port used in beeline command"),
            ("localhost", ":", "default", "The port used in beeline command"),
            ("localhost", "-1", "default", "The port used in beeline command"),
            ("localhost", "655536", "default", "The port used in beeline command"),
            ("localhost", "1234", "default;", "The schema used in beeline command"),
        ],
    )
    def test_get_conn_with_wrong_connection_parameters(self, host, port, schema, message):
        connection = Connection(
            conn_id="test",
            conn_type="hive",
            host=host,
            port=port,
            schema=schema,
        )
        hook = HiveCliHook()
        if message:
            with pytest.raises(Exception, match=message):
                hook._validate_beeline_parameters(connection)
        else:
            hook._validate_beeline_parameters(connection)

    def test_get_records(self):
        hook = MockHiveServer2Hook()
        query = f"SELECT * FROM {self.table}"

        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW_CTX_DAG_ID": "test_dag_id",
                "AIRFLOW_CTX_TASK_ID": "HiveHook_3835",
                "AIRFLOW_CTX_EXECUTION_DATE": "2015-01-01T00:00:00+00:00",
                "AIRFLOW_CTX_DAG_RUN_ID": "55",
                "AIRFLOW_CTX_DAG_OWNER": "airflow",
                "AIRFLOW_CTX_DAG_EMAIL": "test@airflow.com",
            },
        ):
            results = hook.get_records(query, schema=self.database)

        assert results == [(1, 1), (2, 2)]

        hook.get_conn.assert_called_with(self.database)
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_id=test_dag_id")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.task_id=HiveHook_3835")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.execution_date=2015-01-01T00:00:00+00:00")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_run_id=55")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_owner=airflow")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_email=test@airflow.com")

    def test_get_pandas_df(self):
        hook = MockHiveServer2Hook()
        query = f"SELECT * FROM {self.table}"

        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW_CTX_DAG_ID": "test_dag_id",
                "AIRFLOW_CTX_TASK_ID": "HiveHook_3835",
                "AIRFLOW_CTX_EXECUTION_DATE": "2015-01-01T00:00:00+00:00",
                "AIRFLOW_CTX_DAG_RUN_ID": "55",
                "AIRFLOW_CTX_DAG_OWNER": "airflow",
                "AIRFLOW_CTX_DAG_EMAIL": "test@airflow.com",
            },
        ):
            df = hook.get_pandas_df(query, schema=self.database)

        assert len(df) == 2
        assert df["hive_server_hook.a"].values.tolist() == [1, 2]

        hook.get_conn.assert_called_with(self.database)
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_id=test_dag_id")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.task_id=HiveHook_3835")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.execution_date=2015-01-01T00:00:00+00:00")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_run_id=55")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_owner=airflow")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_email=test@airflow.com")

        hook = MockHiveServer2Hook(connection_cursor=EmptyMockConnectionCursor())
        query = f"SELECT * FROM {self.table}"

        df = hook.get_pandas_df(query, schema=self.database)

        assert len(df) == 0

    def test_get_results_header(self):
        hook = MockHiveServer2Hook()

        query = f"SELECT * FROM {self.table}"
        results = hook.get_results(query, schema=self.database)

        assert [col[0] for col in results["header"]] == self.columns

    def test_get_results_data(self):
        hook = MockHiveServer2Hook()

        query = f"SELECT * FROM {self.table}"
        results = hook.get_results(query, schema=self.database)

        assert results["data"] == [(1, 1), (2, 2)]

    def test_to_csv(self):
        hook = MockHiveServer2Hook()
        hook._get_results = mock.MagicMock(
            return_value=iter(
                [
                    [
                        ("hive_server_hook.a", "INT_TYPE", None, None, None, None, True),
                        ("hive_server_hook.b", "INT_TYPE", None, None, None, None, True),
                    ],
                    (1, 1),
                    (2, 2),
                ]
            )
        )
        query = f"SELECT * FROM {self.table}"
        csv_filepath = "query_results.csv"
        hook.to_csv(
            query,
            csv_filepath,
            schema=self.database,
            delimiter=",",
            lineterminator="\n",
            output_header=True,
            fetch_size=2,
        )
        df = pd.read_csv(csv_filepath, sep=",")
        assert df.columns.tolist() == self.columns
        assert df[self.columns[0]].values.tolist() == [1, 2]
        assert len(df) == 2

    def test_multi_statements(self):
        sqls = [
            "CREATE TABLE IF NOT EXISTS test_multi_statements (i INT)",
            f"SELECT * FROM {self.table}",
            "DROP TABLE test_multi_statements",
        ]

        hook = MockHiveServer2Hook()

        with mock.patch.dict(
            "os.environ",
            {
                "AIRFLOW_CTX_DAG_ID": "test_dag_id",
                "AIRFLOW_CTX_TASK_ID": "HiveHook_3835",
                "AIRFLOW_CTX_EXECUTION_DATE": "2015-01-01T00:00:00+00:00",
                "AIRFLOW_CTX_DAG_RUN_ID": "55",
                "AIRFLOW_CTX_DAG_OWNER": "airflow",
                "AIRFLOW_CTX_DAG_EMAIL": "test@airflow.com",
            },
        ):
            results = hook.get_records(sqls, schema=self.database)
        assert results == [(1, 1), (2, 2)]

        hook.get_conn.assert_called_with(self.database)
        hook.mock_cursor.execute.assert_any_call("CREATE TABLE IF NOT EXISTS test_multi_statements (i INT)")
        hook.mock_cursor.execute.assert_any_call(f"SELECT * FROM {self.table}")
        hook.mock_cursor.execute.assert_any_call("DROP TABLE test_multi_statements")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_id=test_dag_id")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.task_id=HiveHook_3835")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.execution_date=2015-01-01T00:00:00+00:00")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_run_id=55")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_owner=airflow")
        hook.mock_cursor.execute.assert_any_call("set airflow.ctx.dag_email=test@airflow.com")

    def test_get_results_with_hive_conf(self):
        hql = [
            "set key",
            "set airflow.ctx.dag_id",
            "set airflow.ctx.dag_run_id",
            "set airflow.ctx.task_id",
            "set airflow.ctx.execution_date",
        ]

        dag_id_ctx_var_name = AIRFLOW_VAR_NAME_FORMAT_MAPPING["AIRFLOW_CONTEXT_DAG_ID"]["env_var_format"]
        task_id_ctx_var_name = AIRFLOW_VAR_NAME_FORMAT_MAPPING["AIRFLOW_CONTEXT_TASK_ID"]["env_var_format"]
        execution_date_ctx_var_name = AIRFLOW_VAR_NAME_FORMAT_MAPPING["AIRFLOW_CONTEXT_EXECUTION_DATE"][
            "env_var_format"
        ]
        dag_run_id_ctx_var_name = AIRFLOW_VAR_NAME_FORMAT_MAPPING["AIRFLOW_CONTEXT_DAG_RUN_ID"][
            "env_var_format"
        ]

        with mock.patch.dict(
            "os.environ",
            {
                dag_id_ctx_var_name: "test_dag_id",
                task_id_ctx_var_name: "test_task_id",
                execution_date_ctx_var_name: "test_execution_date",
                dag_run_id_ctx_var_name: "test_dag_run_id",
            },
        ):
            hook = MockHiveServer2Hook()
            hook._get_results = mock.MagicMock(
                return_value=iter(
                    [
                        "header",
                        ("value", "test"),
                        ("test_dag_id", "test"),
                        ("test_task_id", "test"),
                        ("test_execution_date", "test"),
                        ("test_dag_run_id", "test"),
                    ]
                )
            )

            output = "\n".join(
                res_tuple[0] for res_tuple in hook.get_results(hql, hive_conf={"key": "value"})["data"]
            )
        assert "value" in output
        assert "test_dag_id" in output
        assert "test_task_id" in output
        assert "test_execution_date" in output
        assert "test_dag_run_id" in output


@pytest.mark.db_test
@mock.patch.dict("os.environ", AIRFLOW__CORE__SECURITY="kerberos")
class TestHiveCli:
    def setup_method(self):
        self.nondefault_schema = "nondefault"

    def test_get_proxy_user_value(self):
        hook = MockHiveCliHook()
        returner = mock.MagicMock()
        returner.extra_dejson = {"proxy_user": "a_user_proxy"}
        hook.use_beeline = True
        hook.conn = returner

        # Run
        result = hook._prepare_cli_cmd()

        # Verify
        assert "hive.server2.proxy.user=a_user_proxy" in result[2]

    def test_get_wrong_principal(self):
        hook = MockHiveCliHook()
        returner = mock.MagicMock()
        returner.extra_dejson = {"principal": "principal with ; semicolon"}
        hook.use_beeline = True
        hook.conn = returner

        # Run
        with pytest.raises(RuntimeError, match="The principal should not contain the ';' character"):
            hook._prepare_cli_cmd()
