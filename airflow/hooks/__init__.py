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

# Imports the hooks dynamically while keeping the package API clean,
# abstracting the underlying modules


_hooks = {
    'base_hook': ['BaseHook'],
    'hive_hooks': [
        'HiveCliHook',
        'HiveMetastoreHook',
        'HiveServer2Hook',
    ],
    'hdfs_hook': ['HDFSHook'],
    'webhdfs_hook': ['WebHDFSHook'],
    'pig_hook': ['PigCliHook'],
    'mysql_hook': ['MySqlHook'],
    'postgres_hook': ['PostgresHook'],
    'presto_hook': ['PrestoHook'],
    'samba_hook': ['SambaHook'],
    'sqlite_hook': ['SqliteHook'],
    'S3_hook': ['S3Hook'],
    'zendesk_hook': ['ZendeskHook'],
    'http_hook': ['HttpHook'],
    'druid_hook': ['DruidHook'],
    'jdbc_hook': ['JdbcHook'],
    'dbapi_hook': ['DbApiHook'],
    'mssql_hook': ['MsSqlHook'],
    'oracle_hook': ['OracleHook'],
}

import os as _os
if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
    from airflow.utils.helpers import AirflowImporter
    airflow_importer = AirflowImporter(sys.modules[__name__], _hooks)


def _integrate_plugins():
    """Integrate plugins to the context"""
    from airflow.plugins_manager import hooks_modules
    for hooks_module in hooks_modules:
        sys.modules[hooks_module.__name__] = hooks_module
        globals()[hooks_module._name] = hooks_module

        ##########################################################
        # TODO FIXME Remove in Airflow 2.0

        if not _os.environ.get('AIRFLOW_USE_NEW_IMPORTS', False):
            from zope.deprecation import deprecated as _deprecated
            for _hook in hooks_module._objects:
                hook_name = _hook.__name__
                globals()[hook_name] = _hook
                _deprecated(
                    hook_name,
                    "Importing plugin hook '{i}' directly from "
                    "'airflow.hooks' has been deprecated. Please "
                    "import from 'airflow.hooks.[plugin_module]' "
                    "instead. Support for direct imports will be dropped "
                    "entirely in Airflow 2.0.".format(i=hook_name))
