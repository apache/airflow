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

"""
This module contains operator to move data from Hive to Druid.
"""

from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.providers.apache.druid.hooks.druid import DruidHook
from airflow.providers.apache.hive.hooks.hive import HiveCliHook, HiveMetastoreHook
from airflow.utils.decorators import apply_defaults

LOAD_CHECK_INTERVAL = 5
DEFAULT_TARGET_PARTITION_SIZE = 5000000


class HiveToDruidOperator(BaseOperator):
    """
    Moves data from Hive to Druid, [del]note that for now the data is loaded
    into memory before being pushed to Druid, so this operator should
    be used for smallish amount of data.[/del]

    :param sql: SQL query to execute against the Druid database. (templated)
    :type sql: str
    :param druid_datasource: the datasource you want to ingest into in druid
    :type druid_datasource: str
    :param ts_dim: the timestamp dimension
    :type ts_dim: str
    :param metric_spec: the metrics you want to define for your data
    :type metric_spec: list
    :param hive_cli_conn_id: the hive connection id
    :type hive_cli_conn_id: str
    :param druid_ingest_conn_id: the druid ingest connection id
    :type druid_ingest_conn_id: str
    :param metastore_conn_id: the metastore connection id
    :type metastore_conn_id: str
    :param hadoop_dependency_coordinates: list of coordinates to squeeze
        int the ingest json
    :type hadoop_dependency_coordinates: list[str]
    :param intervals: list of time intervals that defines segments,
        this is passed as is to the json object. (templated)
    :type intervals: list
    :param num_shards: Directly specify the number of shards to create.
    :type num_shards: float
    :param target_partition_size: Target number of rows to include in a partition,
    :type target_partition_size: int
    :param query_granularity: The minimum granularity to be able to query results at and the granularity of
        the data inside the segment. E.g. a value of "minute" will mean that data is aggregated at minutely
        granularity. That is, if there are collisions in the tuple (minute(timestamp), dimensions), then it
        will aggregate values together using the aggregators instead of storing individual rows.
        A granularity of 'NONE' means millisecond granularity.
    :type query_granularity: str
    :param segment_granularity: The granularity to create time chunks at. Multiple segments can be created per
        time chunk. For example, with 'DAY' segmentGranularity, the events of the same day fall into the
        same time chunk which can be optionally further partitioned into multiple segments based on other
        configurations and input size.
    :type segment_granularity: str
    :param hive_tblproperties: additional properties for tblproperties in
        hive for the staging table
    :type hive_tblproperties: dict
    :param job_properties: additional properties for job
    :type job_properties: dict
    """

    template_fields = ('sql', 'intervals')
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        sql: str,
        druid_datasource: str,
        ts_dim: str,
        metric_spec: Optional[List[Any]] = None,
        hive_cli_conn_id: str = 'hive_cli_default',
        druid_ingest_conn_id: str = 'druid_ingest_default',
        metastore_conn_id: str = 'metastore_default',
        hadoop_dependency_coordinates: Optional[List[str]] = None,
        intervals: Optional[List[Any]] = None,
        num_shards: float = -1,
        target_partition_size: int = -1,
        query_granularity: str = "NONE",
        segment_granularity: str = "DAY",
        hive_tblproperties: Optional[Dict[Any, Any]] = None,
        job_properties: Optional[Dict[Any, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.druid_datasource = druid_datasource
        self.ts_dim = ts_dim
        self.intervals = intervals or ['{{ ds }}/{{ tomorrow_ds }}']
        self.num_shards = num_shards
        self.target_partition_size = target_partition_size
        self.query_granularity = query_granularity
        self.segment_granularity = segment_granularity
        self.metric_spec = metric_spec or [{"name": "count", "type": "count"}]
        self.hive_cli_conn_id = hive_cli_conn_id
        self.hadoop_dependency_coordinates = hadoop_dependency_coordinates
        self.druid_ingest_conn_id = druid_ingest_conn_id
        self.metastore_conn_id = metastore_conn_id
        self.hive_tblproperties = hive_tblproperties or {}
        self.job_properties = job_properties

    def execute(self, context: Dict[str, Any]) -> None:
        hive = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id)
        self.log.info("Extracting data from Hive")
        hive_table = 'druid.' + context['task_instance_key_str'].replace('.', '_')
        sql = self.sql.strip().strip(';')
        tblproperties = ''.join([", '{}' = '{}'".format(k, v) for k, v in self.hive_tblproperties.items()])
        hql = f"""\
        SET mapred.output.compress=false;
        SET hive.exec.compress.output=false;
        DROP TABLE IF EXISTS {hive_table};
        CREATE TABLE {hive_table}
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE
        TBLPROPERTIES ('serialization.null.format' = ''{tblproperties})
        AS
        {sql}
        """
        self.log.info("Running command:\n %s", hql)
        hive.run_cli(hql)

        meta_hook = HiveMetastoreHook(self.metastore_conn_id)

        # Get the Hive table and extract the columns
        table = meta_hook.get_table(hive_table)
        columns = [col.name for col in table.sd.cols]

        # Get the path on hdfs
        static_path = meta_hook.get_table(hive_table).sd.location

        druid = DruidHook(druid_ingest_conn_id=self.druid_ingest_conn_id)

        try:
            index_spec = self.construct_ingest_query(
                static_path=static_path,
                columns=columns,
            )

            self.log.info("Inserting rows into Druid, hdfs path: %s", static_path)

            druid.submit_indexing_job(index_spec)

            self.log.info("Load seems to have succeeded!")
        finally:
            self.log.info("Cleaning up by dropping the temp Hive table %s", hive_table)
            hql = "DROP TABLE IF EXISTS {}".format(hive_table)
            hive.run_cli(hql)

    def construct_ingest_query(self, static_path: str, columns: List[str]) -> Dict[str, Any]:
        """
        Builds an ingest query for an HDFS TSV load.

        :param static_path: The path on hdfs where the data is
        :type static_path: str
        :param columns: List of all the columns that are available
        :type columns: list
        """

        # backward compatibility for num_shards,
        # but target_partition_size is the default setting
        # and overwrites the num_shards
        num_shards = self.num_shards
        target_partition_size = self.target_partition_size
        if self.target_partition_size == -1:
            if self.num_shards == -1:
                target_partition_size = DEFAULT_TARGET_PARTITION_SIZE
        else:
            num_shards = -1

        metric_names = [m['fieldName'] for m in self.metric_spec if m['type'] != 'count']

        # Take all the columns, which are not the time dimension
        # or a metric, as the dimension columns
        dimensions = [c for c in columns if c not in metric_names and c != self.ts_dim]

        ingest_query_dict: Dict[str, Any] = {
            "type": "index_hadoop",
            "spec": {
                "dataSchema": {
                    "metricsSpec": self.metric_spec,
                    "granularitySpec": {
                        "queryGranularity": self.query_granularity,
                        "intervals": self.intervals,
                        "type": "uniform",
                        "segmentGranularity": self.segment_granularity,
                    },
                    "parser": {
                        "type": "string",
                        "parseSpec": {
                            "columns": columns,
                            "dimensionsSpec": {
                                "dimensionExclusions": [],
                                "dimensions": dimensions,  # list of names
                                "spatialDimensions": [],
                            },
                            "timestampSpec": {"column": self.ts_dim, "format": "auto"},
                            "format": "tsv",
                        },
                    },
                    "dataSource": self.druid_datasource,
                },
                "tuningConfig": {
                    "type": "hadoop",
                    "jobProperties": {
                        "mapreduce.job.user.classpath.first": "false",
                        "mapreduce.map.output.compress": "false",
                        "mapreduce.output.fileoutputformat.compress": "false",
                    },
                    "partitionsSpec": {
                        "type": "hashed",
                        "targetPartitionSize": target_partition_size,
                        "numShards": num_shards,
                    },
                },
                "ioConfig": {"inputSpec": {"paths": static_path, "type": "static"}, "type": "hadoop"},
            },
        }

        if self.job_properties:
            ingest_query_dict['spec']['tuningConfig']['jobProperties'].update(self.job_properties)

        if self.hadoop_dependency_coordinates:
            ingest_query_dict['hadoopDependencyCoordinates'] = self.hadoop_dependency_coordinates

        return ingest_query_dict
