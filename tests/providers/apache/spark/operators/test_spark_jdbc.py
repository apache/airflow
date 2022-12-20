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

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_jdbc import SparkJDBCOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestSparkJDBCOperator:
    _config = {
        "spark_app_name": "{{ task_instance.task_id }}",
        "spark_conf": {"parquet.compression": "SNAPPY"},
        "spark_files": "hive-site.xml",
        "spark_py_files": "sample_library.py",
        "spark_jars": "parquet.jar",
        "num_executors": 4,
        "executor_cores": 4,
        "executor_memory": "22g",
        "driver_memory": "3g",
        "verbose": True,
        "keytab": "privileged_user.keytab",
        "principal": "user/spark@airflow.org",
        "cmd_type": "spark_to_jdbc",
        "jdbc_table": "tableMcTableFace",
        "jdbc_driver": "org.postgresql.Driver",
        "metastore_table": "hiveMcHiveFace",
        "jdbc_truncate": False,
        "save_mode": "append",
        "save_format": "parquet",
        "batch_size": 100,
        "fetch_size": 200,
        "num_partitions": 10,
        "partition_column": "columnMcColumnFace",
        "lower_bound": "10",
        "upper_bound": "20",
        "create_table_column_types": "columnMcColumnFace INTEGER(100), name CHAR(64),"
        "comments VARCHAR(1024)",
    }

    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", default_args=args)

    def test_execute(self):
        # Given / When
        spark_conn_id = "spark-default"
        jdbc_conn_id = "jdbc-default"

        operator = SparkJDBCOperator(task_id="spark_jdbc_job", dag=self.dag, **self._config)

        # Then
        expected_dict = {
            "spark_app_name": "{{ task_instance.task_id }}",
            "spark_conf": {"parquet.compression": "SNAPPY"},
            "spark_files": "hive-site.xml",
            "spark_py_files": "sample_library.py",
            "spark_jars": "parquet.jar",
            "num_executors": 4,
            "executor_cores": 4,
            "executor_memory": "22g",
            "driver_memory": "3g",
            "verbose": True,
            "keytab": "privileged_user.keytab",
            "principal": "user/spark@airflow.org",
            "cmd_type": "spark_to_jdbc",
            "jdbc_table": "tableMcTableFace",
            "jdbc_driver": "org.postgresql.Driver",
            "metastore_table": "hiveMcHiveFace",
            "jdbc_truncate": False,
            "save_mode": "append",
            "save_format": "parquet",
            "batch_size": 100,
            "fetch_size": 200,
            "num_partitions": 10,
            "partition_column": "columnMcColumnFace",
            "lower_bound": "10",
            "upper_bound": "20",
            "create_table_column_types": "columnMcColumnFace INTEGER(100), name CHAR(64),"
            "comments VARCHAR(1024)",
        }

        assert spark_conn_id == operator._spark_conn_id
        assert jdbc_conn_id == operator._jdbc_conn_id
        assert expected_dict["spark_app_name"] == operator._spark_app_name
        assert expected_dict["spark_conf"] == operator._spark_conf
        assert expected_dict["spark_files"] == operator._spark_files
        assert expected_dict["spark_py_files"] == operator._spark_py_files
        assert expected_dict["spark_jars"] == operator._spark_jars
        assert expected_dict["num_executors"] == operator._num_executors
        assert expected_dict["executor_cores"] == operator._executor_cores
        assert expected_dict["executor_memory"] == operator._executor_memory
        assert expected_dict["driver_memory"] == operator._driver_memory
        assert expected_dict["verbose"] == operator._verbose
        assert expected_dict["keytab"] == operator._keytab
        assert expected_dict["principal"] == operator._principal
        assert expected_dict["cmd_type"] == operator._cmd_type
        assert expected_dict["jdbc_table"] == operator._jdbc_table
        assert expected_dict["jdbc_driver"] == operator._jdbc_driver
        assert expected_dict["metastore_table"] == operator._metastore_table
        assert expected_dict["jdbc_truncate"] == operator._jdbc_truncate
        assert expected_dict["save_mode"] == operator._save_mode
        assert expected_dict["save_format"] == operator._save_format
        assert expected_dict["batch_size"] == operator._batch_size
        assert expected_dict["fetch_size"] == operator._fetch_size
        assert expected_dict["num_partitions"] == operator._num_partitions
        assert expected_dict["partition_column"] == operator._partition_column
        assert expected_dict["lower_bound"] == operator._lower_bound
        assert expected_dict["upper_bound"] == operator._upper_bound
        assert expected_dict["create_table_column_types"] == operator._create_table_column_types
