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

from __future__ import print_function
import logging
import json
import time

from pydruid.client import PyDruid
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

LOAD_CHECK_INTERVAL = 5
DEFAULT_TARGET_PARTITION_SIZE = 5000000


class AirflowDruidLoadException(AirflowException):
    pass


class DruidHook(BaseHook):
    '''
    Interact with druid.
    '''

    def __init__(
            self,
            druid_query_conn_id='druid_query_default',
            druid_ingest_conn_id='druid_ingest_default'):
        self.druid_query_conn_id = druid_query_conn_id
        self.druid_ingest_conn_id = druid_ingest_conn_id
        self.header = {'content-type': 'application/json'}

    def get_conn(self):
        """
        Returns a druid connection object for query
        """
        conn = self.get_connection(self.druid_query_conn_id)
        return PyDruid(
            "http://{conn.host}:{conn.port}".format(**locals()),
            conn.extra_dejson.get('endpoint', ''))

    @property
    def ingest_post_url(self):
        conn = self.get_connection(self.druid_ingest_conn_id)
        host = conn.host
        port = conn.port
        endpoint = conn.extra_dejson.get('endpoint', '')
        return "http://{host}:{port}/{endpoint}".format(**locals())

    def get_ingest_status_url(self, task_id):
        post_url = self.ingest_post_url
        return "{post_url}/{task_id}/status".format(**locals())

    def construct_ingest_query(
            self, datasource, static_path, ts_dim, columns, metric_spec,
            intervals, num_shards, target_partition_size, hadoop_dependency_coordinates=None):
        """
        Builds an ingest query for an HDFS TSV load.

        :param datasource: target datasource in druid
        :param columns: list of all columns in the TSV, in the right order
        """

        # backward compatibilty for num_shards, but target_partition_size is the default setting
        # and overwrites the num_shards
        if target_partition_size == -1:
            if num_shards == -1:
                target_partition_size = DEFAULT_TARGET_PARTITION_SIZE
        else:
            num_shards = -1

        metric_names = [
            m['fieldName'] for m in metric_spec if m['type'] != 'count']
        dimensions = [c for c in columns if c not in metric_names and c != ts_dim]
        ingest_query_dict = {
            "type": "index_hadoop",
            "spec": {
                "dataSchema": {
                    "metricsSpec": metric_spec,
                    "granularitySpec": {
                        "queryGranularity": "NONE",
                        "intervals": intervals,
                        "type": "uniform",
                        "segmentGranularity": "DAY",
                    },
                    "parser": {
                        "type": "string",
                        "parseSpec": {
                            "columns": columns,
                            "dimensionsSpec": {
                                "dimensionExclusions": [],
                                "dimensions": dimensions,  # list of names
                                "spatialDimensions": []
                            },
                            "timestampSpec": {
                                "column": ts_dim,
                                "format": "auto"
                            },
                            "format": "tsv"
                        }
                    },
                    "dataSource": datasource
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
                "ioConfig": {
                    "inputSpec": {
                        "paths": static_path,
                        "type": "static"
                    },
                    "type": "hadoop"
                }
            }
        }
        if hadoop_dependency_coordinates:
            ingest_query_dict[
                'hadoopDependencyCoordinates'] = hadoop_dependency_coordinates

        return json.dumps(ingest_query_dict, indent=4)

    def send_ingest_query(
            self, datasource, static_path, ts_dim, columns, metric_spec,
            intervals, num_shards, target_partition_size, hadoop_dependency_coordinates=None):
        query = self.construct_ingest_query(
            datasource, static_path, ts_dim, columns,
            metric_spec, intervals, num_shards, target_partition_size, hadoop_dependency_coordinates)
        r = requests.post(
            self.ingest_post_url, headers=self.header, data=query)
        logging.info(self.ingest_post_url)
        logging.info(query)
        logging.info(r.text)
        d = json.loads(r.text)
        if "task" not in d:
            raise AirflowDruidLoadException(
                "[Error]: Ingesting data to druid failed.")
        return d["task"]

    def load_from_hdfs(
            self, datasource, static_path,  ts_dim, columns,
            intervals, num_shards, target_partition_size, metric_spec=None, hadoop_dependency_coordinates=None):
        """
        load data to druid from hdfs

        :param ts_dim: The column name to use as a timestamp
        :param metric_spec: A list of dictionaries
        """
        task_id = self.send_ingest_query(
            datasource, static_path, ts_dim, columns, metric_spec,
            intervals, num_shards, target_partition_size, hadoop_dependency_coordinates)
        status_url = self.get_ingest_status_url(task_id)
        while True:
            r = requests.get(status_url)
            d = json.loads(r.text)
            if d['status']['status'] == 'FAILED':
                logging.error(d)
                raise AirflowDruidLoadException(
                    "[Error]: Ingesting data to druid failed.")
            elif d['status']['status'] == 'SUCCESS':
                break
            time.sleep(LOAD_CHECK_INTERVAL)
