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

import sys
import os
from airflow.models import BaseOperator  # noqa: F401

# ------------------------------------------------------------------------
#
# #TODO #FIXME Airflow 2.0
#
# Old import machinary below.
#
# This is deprecated but should be kept until Airflow 2.0
# for compatibility.
#
# ------------------------------------------------------------------------

# Imports operators dynamically while keeping the package API clean,
# abstracting the underlying modules

_operators = {
    'bash_operator': ['BashOperator'],
    'check_operator': [
        'CheckOperator',
        'ValueCheckOperator',
        'IntervalCheckOperator',
    ],
    'python_operator': [
        'PythonOperator',
        'BranchPythonOperator',
        'ShortCircuitOperator',
    ],
    'hive_operator': ['HiveOperator'],
    'pig_operator': ['PigOperator'],
    'presto_check_operator': [
        'PrestoCheckOperator',
        'PrestoValueCheckOperator',
        'PrestoIntervalCheckOperator',
    ],
    'sensors': [
        'BaseSensorOperator',
        'ExternalTaskSensor',
        'HdfsSensor',
        'HivePartitionSensor',
        'HttpSensor',
        'MetastorePartitionSensor',
        'NamedHivePartitionSensor',
        'S3KeySensor',
        'S3PrefixSensor',
        'SqlSensor',
        'TimeDeltaSensor',
        'TimeSensor',
        'WebHdfsSensor',
    ],
    'dagrun_operator': ['TriggerDagRunOperator'],
    'dummy_operator': ['DummyOperator'],
    'email_operator': ['EmailOperator'],
    'hive_to_samba_operator': ['Hive2SambaOperator'],
    'latest_only_operator': ['LatestOnlyOperator'],
    'mysql_operator': ['MySqlOperator'],
    'sqlite_operator': ['SqliteOperator'],
    'mysql_to_hive': ['MySqlToHiveTransfer'],
    'postgres_operator': ['PostgresOperator'],
    'subdag_operator': ['SubDagOperator'],
    'hive_stats_operator': ['HiveStatsCollectionOperator'],
    's3_to_hive_operator': ['S3ToHiveTransfer'],
    'hive_to_mysql': ['HiveToMySqlTransfer'],
    'presto_to_mysql': ['PrestoToMySqlTransfer'],
    's3_file_transform_operator': ['S3FileTransformOperator'],
    'http_operator': ['SimpleHttpOperator'],
    'hive_to_druid': ['HiveToDruidTransfer'],
    'jdbc_operator': ['JdbcOperator'],
    'mssql_operator': ['MsSqlOperator'],
    'mssql_to_hive': ['MsSqlToHiveTransfer'],
    'slack_operator': ['SlackAPIOperator', 'SlackAPIPostOperator'],
    'generic_transfer': ['GenericTransfer'],
    'oracle_operator': ['OracleOperator']
}

if not os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
    from airflow.utils.helpers import AirflowImporter
    airflow_importer = AirflowImporter(sys.modules[__name__], _operators)


def _integrate_plugins():
    """Integrate plugins to the context"""
    from airflow.plugins_manager import operators_modules, register_inbuilt_operator_links
    for operators_module in operators_modules:
        sys.modules[operators_module.__name__] = operators_module
        globals()[operators_module._name] = operators_module

        ##########################################################
        # TODO FIXME Remove in Airflow 2.0

        if not os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
            from zope.deprecation import deprecated as _deprecated
            for _operator in operators_module._objects:
                operator_name = _operator.__name__
                globals()[operator_name] = _operator
                _deprecated(
                    operator_name,
                    "Importing plugin operator '{i}' directly from "
                    "'airflow.operators' has been deprecated. Please "
                    "import from 'airflow.operators.[plugin_module]' "
                    "instead. Support for direct imports will be dropped "
                    "entirely in Airflow 2.0.".format(i=operator_name))

    register_inbuilt_operator_links()
