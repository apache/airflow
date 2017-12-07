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
#
import sys
import os as _os

_sensors = {
    'base_sensor_operator': ['BaseSensorOperator'],
    'external_task_sensor': ['ExternalTaskSensor'],
    'hdfs_sensor': ['HdfsSensor'],
    'hive_partition_sensor': ['HivePartitionSensor'],
    'http_sensor': ['HttpSensor'],
    'metastore_partition_sensor': ['MetastorePartitionSensor'],
    'named_hive_partition_sensor': ['NamedHivePartitionSensor'],
    's3_key_sensor': ['S3KeySensor'],
    's3_prefix_sensor': ['S3PrefixSensor'],
    'sql_sensor': ['SqlSensor'],
    'time_delta_sensor': ['TimeDeltaSensor'],
    'time_sensor': ['TimeSensor'],
    'web_hdfs_sensor': ['WebHdfsSensor']
}

if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
    from airflow.utils.helpers import AirflowImporter
    airflow_importer = AirflowImporter(sys.modules[__name__], _sensors)


def _integrate_plugins():
    """Integrate plugins to the context"""
    from airflow.plugins_manager import sensors_modules
    for sensors_module in sensors_modules:
        sys.modules[sensors_module.__name__] = sensors_module
        globals()[sensors_module._name] = sensors_module

        ##########################################################
        # TODO FIXME Remove in Airflow 2.0

        if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
            from zope.deprecation import deprecated as _deprecated
            for _operator in sensors_module._objects:
                operator_name = _operator.__name__
                globals()[operator_name] = _operator
                _deprecated(
                    operator_name,
                    "Importing plugin operator '{i}' directly from "
                    "'airflow.operators' has been deprecated. Please "
                    "import from 'airflow.operators.[plugin_module]' "
                    "instead. Support for direct imports will be dropped "
                    "entirely in Airflow 2.0.".format(i=operator_name))
