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

import unittest

import requests
import requests_mock

from airflow.models.dag import DAG
from airflow.providers.apache.druid.transfers.hive_to_druid import HiveToDruidOperator


class TestDruidHook(unittest.TestCase):

    # To debug the large json diff
    maxDiff = None

    hook_config = {
        "sql": "SELECT * FROM table",
        "druid_datasource": "our_datasource",
        "ts_dim": "timedimension_column",
        "metric_spec": [
            {"name": "count", "type": "count"},
            {"name": "amountSum", "type": "doubleSum", "fieldName": "amount"},
        ],
        "hive_cli_conn_id": "hive_cli_custom",
        "druid_ingest_conn_id": "druid_ingest_default",
        "metastore_conn_id": "metastore_default",
        "hadoop_dependency_coordinates": "org.apache.spark:spark-core_2.10:1.5.2-mmx1",
        "intervals": "2016-01-01/2017-01-01",
        "num_shards": -1,
        "target_partition_size": 1925,
        "query_granularity": "month",
        "segment_granularity": "week",
        "job_properties": {
            "mapreduce.job.user.classpath.first": "false",
            "mapreduce.map.output.compress": "false",
            "mapreduce.output.fileoutputformat.compress": "false",
        },
    }

    index_spec_config = {"static_path": "/apps/db/warehouse/hive/", "columns": ["country", "segment"]}

    def setUp(self):
        super().setUp()

        args = {"owner": "airflow", "start_date": "2017-01-01"}
        self.dag = DAG("hive_to_druid", default_args=args)

        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount("mock", adapter)

    def test_construct_ingest_query(self):
        operator = HiveToDruidOperator(task_id="hive_to_druid", dag=self.dag, **self.hook_config)

        provided_index_spec = operator.construct_ingest_query(**self.index_spec_config)

        expected_index_spec = {
            "hadoopDependencyCoordinates": self.hook_config["hadoop_dependency_coordinates"],
            "type": "index_hadoop",
            "spec": {
                "dataSchema": {
                    "metricsSpec": self.hook_config["metric_spec"],
                    "granularitySpec": {
                        "queryGranularity": self.hook_config["query_granularity"],
                        "intervals": self.hook_config["intervals"],
                        "type": "uniform",
                        "segmentGranularity": self.hook_config["segment_granularity"],
                    },
                    "parser": {
                        "type": "string",
                        "parseSpec": {
                            "columns": self.index_spec_config["columns"],
                            "dimensionsSpec": {
                                "dimensionExclusions": [],
                                "dimensions": self.index_spec_config["columns"],
                                "spatialDimensions": [],
                            },
                            "timestampSpec": {"column": self.hook_config["ts_dim"], "format": "auto"},
                            "format": "tsv",
                        },
                    },
                    "dataSource": self.hook_config["druid_datasource"],
                },
                "tuningConfig": {
                    "type": "hadoop",
                    "jobProperties": self.hook_config["job_properties"],
                    "partitionsSpec": {
                        "type": "hashed",
                        "targetPartitionSize": self.hook_config["target_partition_size"],
                        "numShards": self.hook_config["num_shards"],
                    },
                },
                "ioConfig": {
                    "inputSpec": {"paths": self.index_spec_config["static_path"], "type": "static"},
                    "type": "hadoop",
                },
            },
        }

        # Make sure it is like we expect it
        assert provided_index_spec == expected_index_spec
