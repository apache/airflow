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
#

import datetime
import itertools
import os
import random
import unittest
from collections import OrderedDict
from unittest import mock

import pandas as pd
from hmsclient import HMSClient

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.hive_hooks import HiveCliHook, HiveMetastoreHook, HiveServer2Hook
from airflow.models.connection import Connection
from airflow.operators.hive_operator import HiveOperator
from airflow.utils import timezone
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING
from tests.test_utils.asserts import assert_equal_ignore_multiple_spaces

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]


class TestHiveEnvironment(unittest.TestCase):

    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('test_dag_id', default_args=args)
        self.next_day = (DEFAULT_DATE +
                         datetime.timedelta(days=1)).isoformat()[:10]
        self.database = 'airflow'
        self.partition_by = 'ds'
        self.table = 'static_babynames_partitioned'
        self.hql = """
        CREATE DATABASE IF NOT EXISTS {{ params.database }};
        USE {{ params.database }};
        DROP TABLE IF EXISTS {{ params.table }};
        CREATE TABLE IF NOT EXISTS {{ params.table }} (
            state string,
            year string,
            name string,
            gender string,
            num int)
        PARTITIONED BY ({{ params.partition_by }} string);
        ALTER TABLE {{ params.table }}
        ADD PARTITION({{ params.partition_by }}='{{ ds }}');
        """
        self.hook = HiveMetastoreHook()
        op = HiveOperator(
            task_id='HiveHook_' + str(random.randint(1, 10000)),
            params={
                'database': self.database,
                'table': self.table,
                'partition_by': self.partition_by
            },
            hive_cli_conn_id='hive_cli_default',
            hql=self.hql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def tearDown(self):
        hook = HiveMetastoreHook()
        with hook.get_conn() as metastore:
            metastore.drop_table(self.database, self.table, deleteData=True)


class TestHiveCliHook(unittest.TestCase):

    def test_run_cli(self):
        hook = HiveCliHook()
        hook.run_cli("SHOW DATABASES")

    def test_run_cli_with_hive_conf(self):
        hql = "set key;\n" \
              "set airflow.ctx.dag_id;\nset airflow.ctx.dag_run_id;\n" \
              "set airflow.ctx.task_id;\nset airflow.ctx.execution_date;\n"

        dag_id_ctx_var_name = \
            AIRFLOW_VAR_NAME_FORMAT_MAPPING['AIRFLOW_CONTEXT_DAG_ID']['env_var_format']
        task_id_ctx_var_name = \
            AIRFLOW_VAR_NAME_FORMAT_MAPPING['AIRFLOW_CONTEXT_TASK_ID']['env_var_format']
        execution_date_ctx_var_name = \
            AIRFLOW_VAR_NAME_FORMAT_MAPPING['AIRFLOW_CONTEXT_EXECUTION_DATE'][
                'env_var_format']
        dag_run_id_ctx_var_name = \
            AIRFLOW_VAR_NAME_FORMAT_MAPPING['AIRFLOW_CONTEXT_DAG_RUN_ID'][
                'env_var_format']
        with mock.patch.dict('os.environ', {
            dag_id_ctx_var_name: 'test_dag_id',
            task_id_ctx_var_name: 'test_task_id',
            execution_date_ctx_var_name: 'test_execution_date',
            dag_run_id_ctx_var_name: 'test_dag_run_id',
        }):
            hook = HiveCliHook()
            output = hook.run_cli(hql=hql, hive_conf={'key': 'value'})
            self.assertIn('value', output)
            self.assertIn('test_dag_id', output)
            self.assertIn('test_task_id', output)
            self.assertIn('test_execution_date', output)
            self.assertIn('test_dag_run_id', output)

    @mock.patch('airflow.hooks.hive_hooks.HiveCliHook.run_cli')
    def test_load_file_without_create_table(self, mock_run_cli):
        filepath = "/path/to/input/file"
        table = "output_table"

        hook = HiveCliHook()
        hook.load_file(filepath=filepath, table=table, create=False)

        query = (
            "LOAD DATA LOCAL INPATH '{filepath}' "
            "OVERWRITE INTO TABLE {table} ;\n"
            .format(filepath=filepath, table=table)
        )
        calls = [
            mock.call(query)
        ]
        mock_run_cli.assert_has_calls(calls, any_order=True)

    @mock.patch('airflow.hooks.hive_hooks.HiveCliHook.run_cli')
    def test_load_file_create_table(self, mock_run_cli):
        filepath = "/path/to/input/file"
        table = "output_table"
        field_dict = OrderedDict([("name", "string"), ("gender", "string")])
        fields = ",\n    ".join(['`{k}` {v}'.format(k=k.strip('`'), v=v) for k, v in field_dict.items()])

        hook = HiveCliHook()
        hook.load_file(filepath=filepath, table=table, field_dict=field_dict, create=True, recreate=True)

        create_table = (
            "DROP TABLE IF EXISTS {table};\n"
            "CREATE TABLE IF NOT EXISTS {table} (\n{fields})\n"
            "ROW FORMAT DELIMITED\n"
            "FIELDS TERMINATED BY ','\n"
            "STORED AS textfile\n;".format(table=table, fields=fields)
        )

        load_data = (
            "LOAD DATA LOCAL INPATH '{filepath}' "
            "OVERWRITE INTO TABLE {table} ;\n"
            .format(filepath=filepath, table=table)
        )
        calls = [
            mock.call(create_table),
            mock.call(load_data)
        ]
        mock_run_cli.assert_has_calls(calls, any_order=True)

    @mock.patch('airflow.hooks.hive_hooks.HiveCliHook.load_file')
    @mock.patch('pandas.DataFrame.to_csv')
    def test_load_df(self, mock_to_csv, mock_load_file):
        df = pd.DataFrame({"c": ["foo", "bar", "baz"]})
        table = "t"
        delimiter = ","
        encoding = "utf-8"

        hook = HiveCliHook()
        hook.load_df(df=df,
                     table=table,
                     delimiter=delimiter,
                     encoding=encoding)

        assert mock_to_csv.call_count == 1
        kwargs = mock_to_csv.call_args[1]
        self.assertEqual(kwargs["header"], False)
        self.assertEqual(kwargs["index"], False)
        self.assertEqual(kwargs["sep"], delimiter)

        assert mock_load_file.call_count == 1
        kwargs = mock_load_file.call_args[1]
        self.assertEqual(kwargs["delimiter"], delimiter)
        self.assertEqual(kwargs["field_dict"], {"c": "STRING"})
        self.assertTrue(isinstance(kwargs["field_dict"], OrderedDict))
        self.assertEqual(kwargs["table"], table)

    @mock.patch('airflow.hooks.hive_hooks.HiveCliHook.load_file')
    @mock.patch('pandas.DataFrame.to_csv')
    def test_load_df_with_optional_parameters(self, mock_to_csv, mock_load_file):
        hook = HiveCliHook()
        bools = (True, False)
        for create, recreate in itertools.product(bools, bools):
            mock_load_file.reset_mock()
            hook.load_df(df=pd.DataFrame({"c": range(0, 10)}),
                         table="t",
                         create=create,
                         recreate=recreate)

            assert mock_load_file.call_count == 1
            kwargs = mock_load_file.call_args[1]
            self.assertEqual(kwargs["create"], create)
            self.assertEqual(kwargs["recreate"], recreate)

    @mock.patch('airflow.hooks.hive_hooks.HiveCliHook.run_cli')
    def test_load_df_with_data_types(self, mock_run_cli):
        ord_dict = OrderedDict()
        ord_dict['b'] = [True]
        ord_dict['i'] = [-1]
        ord_dict['t'] = [1]
        ord_dict['f'] = [0.0]
        ord_dict['c'] = ['c']
        ord_dict['M'] = [datetime.datetime(2018, 1, 1)]
        ord_dict['O'] = [object()]
        ord_dict['S'] = [b'STRING']
        ord_dict['U'] = ['STRING']
        ord_dict['V'] = [None]
        df = pd.DataFrame(ord_dict)

        hook = HiveCliHook()
        hook.load_df(df, 't')

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
        assert_equal_ignore_multiple_spaces(self, mock_run_cli.call_args_list[0][0][0], query)


class TestHiveMetastoreHook(TestHiveEnvironment):
    VALID_FILTER_MAP = {'key2': 'value2'}

    def test_get_max_partition_from_empty_part_specs(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_specs([],
                                                                 'key1',
                                                                 self.VALID_FILTER_MAP)
        self.assertIsNone(max_partition)

    def test_get_max_partition_from_valid_part_specs_and_invalid_filter_map(self):
        with self.assertRaises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key1',
                {'key3': 'value5'})

    def test_get_max_partition_from_valid_part_specs_and_invalid_partition_key(self):
        with self.assertRaises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key3',
                self.VALID_FILTER_MAP)

    def test_get_max_partition_from_valid_part_specs_and_none_partition_key(self):
        with self.assertRaises(AirflowException):
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                None,
                self.VALID_FILTER_MAP)

    def test_get_max_partition_from_valid_part_specs_and_none_filter_map(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key1',
                None)

        # No partition will be filtered out.
        self.assertEqual(max_partition, b'value3')

    def test_get_max_partition_from_valid_part_specs(self):
        max_partition = \
            HiveMetastoreHook._get_max_partition_from_part_specs(
                [{'key1': 'value1', 'key2': 'value2'},
                 {'key1': 'value3', 'key2': 'value4'}],
                'key1',
                self.VALID_FILTER_MAP)
        self.assertEqual(max_partition, b'value1')

    def test_get_metastore_client(self):
        self.assertIsInstance(self.hook.get_metastore_client(), HMSClient)

    @mock.patch("airflow.hooks.hive_hooks.HiveMetastoreHook.get_connection",
                return_value=[Connection(host="localhost", port="9802")])
    @mock.patch("airflow.hooks.hive_hooks.socket")
    def test_error_metastore_client(self, socket_mock, _find_vaild_server_mock):
        socket_mock.socket.return_value.connect_ex.return_value = 0
        self.hook.get_metastore_client()

    def test_get_conn(self):
        self.assertIsInstance(self.hook.get_conn(), HMSClient)

    def test_check_for_partition(self):
        partition = "{p_by}='{date}'".format(date=DEFAULT_DATE_DS,
                                             p_by=self.partition_by)
        missing_partition = "{p_by}='{date}'".format(date=self.next_day,
                                                     p_by=self.partition_by)
        self.assertTrue(
            self.hook.check_for_partition(self.database, self.table,
                                          partition)
        )
        self.assertFalse(
            self.hook.check_for_partition(self.database, self.table,
                                          missing_partition)
        )

    def test_check_for_named_partition(self):
        partition = "{p_by}={date}".format(date=DEFAULT_DATE_DS,
                                           p_by=self.partition_by)
        missing_partition = "{p_by}={date}".format(date=self.next_day,
                                                   p_by=self.partition_by)
        self.assertTrue(
            self.hook.check_for_named_partition(self.database,
                                                self.table,
                                                partition)
        )
        self.assertFalse(
            self.hook.check_for_named_partition(self.database,
                                                self.table,
                                                missing_partition)
        )

    def test_get_table(self):
        table_info = self.hook.get_table(db=self.database,
                                         table_name=self.table)
        self.assertEqual(table_info.tableName, self.table)
        columns = ['state', 'year', 'name', 'gender', 'num']
        self.assertEqual([col.name for col in table_info.sd.cols], columns)

    def test_get_tables(self):
        tables = self.hook.get_tables(db=self.database,
                                      pattern=self.table + "*")
        self.assertIn(self.table, {table.tableName for table in tables})

    def test_get_databases(self):
        databases = self.hook.get_databases(pattern='*')
        self.assertIn(self.database, databases)

    def test_get_partitions(self):
        partitions = self.hook.get_partitions(schema=self.database,
                                              table_name=self.table)
        self.assertEqual(len(partitions), 1)
        self.assertEqual(partitions, [{self.partition_by: DEFAULT_DATE_DS}])

    def test_max_partition(self):
        filter_map = {self.partition_by: DEFAULT_DATE_DS}
        partition = self.hook.max_partition(schema=self.database,
                                            table_name=self.table,
                                            field=self.partition_by,
                                            filter_map=filter_map)
        self.assertEqual(partition, DEFAULT_DATE_DS.encode('utf-8'))

    def test_table_exists(self):
        self.assertTrue(self.hook.table_exists(self.table, db=self.database))
        self.assertFalse(
            self.hook.table_exists(str(random.randint(1, 10000)))
        )


class TestHiveServer2Hook(unittest.TestCase):

    def _upload_dataframe(self):
        df = pd.DataFrame({'a': [1, 2], 'b': [1, 2]})
        self.local_path = '/tmp/TestHiveServer2Hook.csv'
        df.to_csv(self.local_path, header=False, index=False)

    def setUp(self):
        self._upload_dataframe()
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG('test_dag_id', default_args=args)
        self.database = 'airflow'
        self.table = 'hive_server_hook'
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
        self.columns = ['{}.a'.format(self.table),
                        '{}.b'.format(self.table)]
        self.hook = HiveMetastoreHook()
        op = HiveOperator(
            task_id='HiveHook_' + str(random.randint(1, 10000)),
            params={
                'database': self.database,
                'table': self.table,
                'csv_path': self.local_path
            },
            hive_cli_conn_id='hive_cli_default',
            hql=self.hql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)

    def tearDown(self):
        hook = HiveMetastoreHook()
        with hook.get_conn() as metastore:
            metastore.drop_table(self.database, self.table, deleteData=True)
        os.remove(self.local_path)

    def test_get_conn(self):
        hook = HiveServer2Hook()
        hook.get_conn()

    @mock.patch('pyhive.hive.connect')
    def test_get_conn_with_password(self, mock_connect):
        from airflow.hooks.base_hook import CONN_ENV_PREFIX
        conn_id = "conn_with_password"
        conn_env = CONN_ENV_PREFIX + conn_id.upper()

        with mock.patch.dict(
            'os.environ',
            {conn_env: "jdbc+hive2://conn_id:conn_pass@localhost:10000/default?authMechanism=LDAP"}
        ):
            HiveServer2Hook(hiveserver2_conn_id=conn_id).get_conn()
            mock_connect.assert_called_once_with(
                host='localhost',
                port=10000,
                auth='LDAP',
                kerberos_service_name=None,
                username='conn_id',
                password='conn_pass',
                database='default')

    def test_get_records(self):
        hook = HiveServer2Hook()
        query = "SELECT * FROM {}".format(self.table)
        results = hook.get_records(query, schema=self.database)
        self.assertListEqual(results, [(1, 1), (2, 2)])

    def test_get_pandas_df(self):
        hook = HiveServer2Hook()
        query = "SELECT * FROM {}".format(self.table)
        df = hook.get_pandas_df(query, schema=self.database)
        self.assertEqual(len(df), 2)
        self.assertListEqual(df.columns.tolist(), self.columns)
        self.assertListEqual(df[self.columns[0]].values.tolist(), [1, 2])

    def test_get_results_header(self):
        hook = HiveServer2Hook()
        query = "SELECT * FROM {}".format(self.table)
        results = hook.get_results(query, schema=self.database)
        self.assertListEqual([col[0] for col in results['header']],
                             self.columns)

    def test_get_results_data(self):
        hook = HiveServer2Hook()
        query = "SELECT * FROM {}".format(self.table)
        results = hook.get_results(query, schema=self.database)
        self.assertListEqual(results['data'], [(1, 1), (2, 2)])

    def test_to_csv(self):
        hook = HiveServer2Hook()
        query = "SELECT * FROM {}".format(self.table)
        csv_filepath = 'query_results.csv'
        hook.to_csv(query, csv_filepath, schema=self.database,
                    delimiter=',', lineterminator='\n', output_header=True, fetch_size=2)
        df = pd.read_csv(csv_filepath, sep=',')
        self.assertListEqual(df.columns.tolist(), self.columns)
        self.assertListEqual(df[self.columns[0]].values.tolist(), [1, 2])
        self.assertEqual(len(df), 2)

    def test_multi_statements(self):
        sqls = [
            "CREATE TABLE IF NOT EXISTS test_multi_statements (i INT)",
            "SELECT * FROM {}".format(self.table),
            "DROP TABLE test_multi_statements",
        ]
        hook = HiveServer2Hook()
        results = hook.get_records(sqls, schema=self.database)
        self.assertListEqual(results, [(1, 1), (2, 2)])

    def test_get_results_with_hive_conf(self):
        hql = ["set key",
               "set airflow.ctx.dag_id",
               "set airflow.ctx.dag_run_id",
               "set airflow.ctx.task_id",
               "set airflow.ctx.execution_date"]

        dag_id_ctx_var_name = \
            AIRFLOW_VAR_NAME_FORMAT_MAPPING['AIRFLOW_CONTEXT_DAG_ID']['env_var_format']
        task_id_ctx_var_name = \
            AIRFLOW_VAR_NAME_FORMAT_MAPPING['AIRFLOW_CONTEXT_TASK_ID']['env_var_format']
        execution_date_ctx_var_name = \
            AIRFLOW_VAR_NAME_FORMAT_MAPPING['AIRFLOW_CONTEXT_EXECUTION_DATE'][
                'env_var_format']
        dag_run_id_ctx_var_name = \
            AIRFLOW_VAR_NAME_FORMAT_MAPPING['AIRFLOW_CONTEXT_DAG_RUN_ID'][
                'env_var_format']

        with mock.patch.dict('os.environ', {
            dag_id_ctx_var_name: 'test_dag_id',
            task_id_ctx_var_name: 'test_task_id',
            execution_date_ctx_var_name: 'test_execution_date',
            dag_run_id_ctx_var_name: 'test_dag_run_id',

        }):
            hook = HiveServer2Hook()
            output = '\n'.join(res_tuple[0]
                               for res_tuple
                               in hook.get_results(hql=hql,
                                                   hive_conf={'key': 'value'})['data'])
        self.assertIn('value', output)
        self.assertIn('test_dag_id', output)
        self.assertIn('test_task_id', output)
        self.assertIn('test_execution_date', output)
        self.assertIn('test_dag_run_id', output)
