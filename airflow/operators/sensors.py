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


# ------------------------------------------------------------------------
#
# #TODO #FIXME Airflow 2.0
#
# In Airflow this will be moved to the airflow.sensors package.
# Until then this class will provide backward compatibility
#
# ------------------------------------------------------------------------


from airflow.sensors.base_sensor_operator import BaseSensorOperator as \
    BaseSensorOperatorImp
from airflow.sensors.external_task_sensor import ExternalTaskSensor as \
    ExternalTaskSensorImp
from airflow.sensors.hdfs_sensor import HdfsSensor as HdfsSensorImp
from airflow.sensors.hive_partition_sensor import HivePartitionSensor as \
    HivePartitionSensorImp
from airflow.sensors.http_sensor import HttpSensor as HttpSensorImp
from airflow.sensors.metastore_partition_sensor import MetastorePartitionSensor as \
    MetastorePartitionSensorImp
from airflow.sensors.s3_key_sensor import S3KeySensor as S3KeySensorImp
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor as S3PrefixSensorImp
from airflow.sensors.sql_sensor import SqlSensor as SqlSensorImp
from airflow.sensors.time_delta_sensor import TimeDeltaSensor as TimeDeltaSensorImp
from airflow.sensors.time_sensor import TimeSensor as TimeSensorImp
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor as WebHdfsSensorImp


class BaseSensorOperator(BaseSensorOperatorImp):
    pass


class ExternalTaskSensor(ExternalTaskSensorImp):
    pass


class HdfsSensor(HdfsSensorImp):
    pass


class HttpSensor(HttpSensorImp):
    pass


class MetastorePartitionSensor(MetastorePartitionSensorImp):
    pass


class HivePartitionSensor(HivePartitionSensorImp):
    pass


class S3KeySensor(S3KeySensorImp):
    pass


class S3PrefixSensor(S3PrefixSensorImp):
    pass


class SqlSensor(SqlSensorImp):
    pass


class TimeDeltaSensor(TimeDeltaSensorImp):
    pass


class TimeSensor(TimeSensorImp):
    pass


class WebHdfsSensor(WebHdfsSensorImp):
    pass
