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


# Only import Core Airflow Operators that don't have extra requirements.
# All other operators must be imported directly.
from airflow.models import BaseOperator
from .bash_operator import BashOperator
from .python_operator import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator)
from .check_operator import (
    CheckOperator,
    ValueCheckOperator,
    IntervalCheckOperator)
from .dagrun_operator import TriggerDagRunOperator
from .dummy_operator import DummyOperator
from .email_operator import EmailOperator
from .http_operator import SimpleHttpOperator
import airflow.operators.sensors
from .subdag_operator import SubDagOperator




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
from airflow.utils.helpers import import_module_attrs as _import_module_attrs

# These need to be integrated first as other operators depend on them
# _import_module_attrs(globals(), {
#     'check_operator': [
#         'CheckOperator',
#         'ValueCheckOperator',
#         'IntervalCheckOperator',
#     ],
# })

_operators = {
    # 'bash_operator': ['BashOperator'],
    # 'python_operator': [
    #     'PythonOperator',
    #     'BranchPythonOperator',
    #     'ShortCircuitOperator',
    # ],
    'hive_operator': ['HiveOperator'],
    'pig_operator': ['PigOperator'],
    'presto_check_operator': [
        'PrestoCheckOperator',
        'PrestoValueCheckOperator',
        'PrestoIntervalCheckOperator',
    ],
    # 'dagrun_operator': ['TriggerDagRunOperator'],
    # 'dummy_operator': ['DummyOperator'],
    # 'email_operator': ['EmailOperator'],
    'hive_to_samba_operator': ['Hive2SambaOperator'],
    'mysql_operator': ['MySqlOperator'],
    'sqlite_operator': ['SqliteOperator'],
    'mysql_to_hive': ['MySqlToHiveTransfer'],
    'postgres_operator': ['PostgresOperator'],
    'sensors': [
        'BaseSensorOperator',
        'ExternalTaskSensor',
        'HdfsSensor',
        'HivePartitionSensor',
        'HttpSensor',
        'MetastorePartitionSensor',
        'S3KeySensor',
        'S3PrefixSensor',
        'SqlSensor',
        'TimeDeltaSensor',
        'TimeSensor',
        'WebHdfsSensor',
    ],
    # 'subdag_operator': ['SubDagOperator'],
    'hive_stats_operator': ['HiveStatsCollectionOperator'],
    's3_to_hive_operator': ['S3ToHiveTransfer'],
    'hive_to_mysql': ['HiveToMySqlTransfer'],
    'presto_to_mysql': ['PrestoToMySqlTransfer'],
    's3_file_transform_operator': ['S3FileTransformOperator'],
    # 'http_operator': ['SimpleHttpOperator'],
    'hive_to_druid': ['HiveToDruidTransfer'],
    'jdbc_operator': ['JdbcOperator'],
    'mssql_operator': ['MsSqlOperator'],
    'mssql_to_hive': ['MsSqlToHiveTransfer'],
    'slack_operator': ['SlackAPIOperator', 'SlackAPIPostOperator'],
    'generic_transfer': ['GenericTransfer'],
    'oracle_operator': ['OracleOperator']
}

import os as _os
if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
    from zope.deprecation import deprecated as _deprecated
    _imported = _import_module_attrs(globals(), _operators)
    for _i in _imported:
        _deprecated(
            _i,
            "Importing {i} directly from 'airflow.operators' has been "
            "deprecated. Please import from "
            "'airflow.operators.[operator_module]' instead. Support for direct "
            "imports will be dropped entirely in Airflow 2.0.".format(i=_i))


def _integrate_plugins():
    """Integrate plugins to the context"""
    import sys
    from airflow.plugins_manager import operators as _operators
    for _operator_module in _operators:
        sys.modules[_operator_module.__name__] = _operator_module
        globals()[_operator_module._name] = _operator_module


        ##########################################################
        # TODO FIXME Remove in Airflow 2.0

        if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
            from zope.deprecation import deprecated as _deprecated
            for _operator in _operator_module._objects:
                globals()[_operator.__name__] = _deprecated(
                    _operator,
                    "Importing plugin operator '{i}' directly from "
                    "'airflow.operators' has been deprecated. Please "
                    "import from 'airflow.operators.[plugin_module]' "
                    "instead. Support for direct imports will be dropped "
                    "entirely in Airflow 2.0.".format(i=_operator))
