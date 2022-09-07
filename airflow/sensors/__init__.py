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
# fmt: off
"""Sensors."""
from airflow.utils.deprecation_tools import add_deprecated_classes

__deprecated_classes = {
    'base_sensor_operator': {
        'BaseSensorOperator': 'airflow.sensors.base.BaseSensorOperator',
    },
    'date_time_sensor': {
        'DateTimeSensor': 'airflow.sensors.date_time.DateTimeSensor',
    },
    'external_task_sensor': {
        'ExternalTaskMarker': 'airflow.sensors.external_task.ExternalTaskMarker',
        'ExternalTaskSensor': 'airflow.sensors.external_task.ExternalTaskSensor',
        'ExternalTaskSensorLink': 'airflow.sensors.external_task.ExternalTaskSensorLink',
    },
    'hdfs_sensor': {
        'HdfsSensor': 'airflow.providers.apache.hdfs.sensors.hdfs.HdfsSensor',
    },
    'hive_partition_sensor': {
        'HivePartitionSensor': 'airflow.providers.apache.hive.sensors.hive_partition.HivePartitionSensor',
    },
    'http_sensor': {
        'HttpSensor': 'airflow.providers.http.sensors.http.HttpSensor',
    },
    'metastore_partition_sensor': {
        'MetastorePartitionSensor': (
            'airflow.providers.apache.hive.sensors.metastore_partition.MetastorePartitionSensor'
        ),
    },
    'named_hive_partition_sensor': {
        'NamedHivePartitionSensor': (
            'airflow.providers.apache.hive.sensors.named_hive_partition.NamedHivePartitionSensor'
        ),
    },
    's3_key_sensor': {
        'S3KeySensor': 'airflow.providers.amazon.aws.sensors.s3.S3KeySensor',
    },
    'sql': {
        'SqlSensor': 'airflow.providers.common.sql.sensors.sql.SqlSensor',
    },
    'sql_sensor': {
        'SqlSensor': 'airflow.providers.common.sql.sensors.sql.SqlSensor',
    },
    'time_delta_sensor': {
        'TimeDeltaSensor': 'airflow.sensors.time_delta.TimeDeltaSensor',
    },
    'web_hdfs_sensor': {
        'WebHdfsSensor': 'airflow.providers.apache.hdfs.sensors.web_hdfs.WebHdfsSensor',
    },
}

add_deprecated_classes(__deprecated_classes, __name__)
