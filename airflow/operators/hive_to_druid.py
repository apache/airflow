# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.hooks.hive_hooks import HiveCliHook, HiveMetastoreHook
from airflow.hooks.druid_hook import DruidHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HiveToDruidTransfer(BaseOperator):
    """
    Moves data from Hive to Druid, [del]note that for now the data is loaded
    into memory before being pushed to Druid, so this operator should
    be used for smallish amount of data.[/del]

    :param sql: SQL query to execute against the Druid database
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
    :type hadoop_dependency_coordinates: list of str
    :param intervals: list of time intervals that defines segments, this
        is passed as is to the json object
    :type intervals: list
    """

    template_fields = ('sql', 'intervals')
    template_ext = ('.sql',)
    #ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            sql,
            druid_datasource,
            ts_dim,
            metric_spec=None,
            hive_cli_conn_id='hive_cli_default',
            druid_ingest_conn_id='druid_ingest_default',
            metastore_conn_id='metastore_default',
            hadoop_dependency_coordinates=None,
            intervals=None,
            num_shards=-1,
            target_partition_size=-1,
            query_granularity=None,
            segment_granularity=None,
            *args, **kwargs):
        super(HiveToDruidTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.druid_datasource = druid_datasource
        self.ts_dim = ts_dim
        self.intervals = intervals or ['{{ ds }}/{{ tomorrow_ds }}']
        self.num_shards = num_shards
        self.target_partition_size = target_partition_size
        self.query_granularity = query_granularity
        self.segment_granularity = segment_granularity
        self.metric_spec = metric_spec or [{
            "name": "count",
            "type": "count"}]
        self.hive_cli_conn_id = hive_cli_conn_id
        self.hadoop_dependency_coordinates = hadoop_dependency_coordinates
        self.druid_ingest_conn_id = druid_ingest_conn_id
        self.metastore_conn_id = metastore_conn_id

    def execute(self, context):
        hive = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id)
        logging.info("Extracting data from Hive")
        hive_table = 'druid.' + context['task_instance_key_str'].replace('.', '_')
        sql = self.sql.strip().strip(';')
        hql = """\
        set mapred.output.compress=false;
        set hive.exec.compress.output=false;
        DROP TABLE IF EXISTS {hive_table};
        CREATE TABLE {hive_table}
        ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t'
        STORED AS TEXTFILE
        TBLPROPERTIES ('serialization.null.format' = '')
        AS
        {sql}
        """.format(**locals())
        logging.info("Running command:\n {}".format(hql))
        hive.run_cli(hql)

        m = HiveMetastoreHook(self.metastore_conn_id)
        t = m.get_table(hive_table)

        columns = [col.name for col in t.sd.cols]

        hdfs_uri = m.get_table(hive_table).sd.location
        pos = hdfs_uri.find('/user')
        static_path = hdfs_uri[pos:]

        schema, table = hive_table.split('.')

        druid = DruidHook(druid_ingest_conn_id=self.druid_ingest_conn_id)
        logging.info("Inserting rows into Druid")
        logging.info("HDFS path: " + static_path)

        try:
            druid.load_from_hdfs(
                datasource=self.druid_datasource,
                intervals=self.intervals,
                static_path=static_path, ts_dim=self.ts_dim,
                columns=columns, num_shards=self.num_shards, target_partition_size=self.target_partition_size,
                query_granularity=self.query_granularity, segment_granularity=self.segment_granularity,
                metric_spec=self.metric_spec, hadoop_dependency_coordinates=self.hadoop_dependency_coordinates)
            logging.info("Load seems to have succeeded!")
        finally:
            logging.info(
                "Cleaning up by dropping the temp "
                "Hive table {}".format(hive_table))
            hql = "DROP TABLE IF EXISTS {}".format(hive_table)
            hive.run_cli(hql)
