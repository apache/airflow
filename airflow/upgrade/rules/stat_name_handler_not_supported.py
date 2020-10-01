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

from airflow import plugins_manager

from airflow.configuration import conf

from airflow.upgrade.rules.base_rule import BaseRule


class StatNameHandlerNotSupportedRule(BaseRule):

    title = 'stat_name_handler field in AirflowTestPlugin is not supported.'

    description = '''\
stat_name_handler field is no longer supported in AirflowTestPlugin.
Instead there is stat_name_handler option in [scheduler] section of airflow.cfg.
This change is intended to simplify the statsd configuration.
    '''

    def check(self):
        if conf.has_option('scheduler', 'stat_name_handler'):
            return None

        if any(getattr(plugin, 'stat_name_handler', None) for plugin in plugins_manager.plugins):
            return (
                'stat_name_handler field is no longer supported in AirflowTestPlugin.'
                ' stat_name_handler option in [scheduler] section in airflow.cfg should'
                ' be used to achieve the same effect.'
            )

        return None
