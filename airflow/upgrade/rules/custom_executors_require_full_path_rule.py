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

from airflow.configuration import conf
from airflow.upgrade.rules.base_rule import BaseRule


class CustomExecutorsRequireFullPathRule(BaseRule):
    """
    CustomExecutorsRequireFullPathRule class to ease upgrade to Airflow 2.0
    """
    title = "Custom Executors now require full path"
    description = """\
In Airflow-2.0, loading custom executors via plugins is no longer required.
To load a custom executor, you have to provide a full path to the the custom executor module.
                  """

    def check(self):
        executor = conf.get(section="core", key="executor")
        if executor.count(".") <= 1:
            return (
                "Deprecation Warning: you do not need to load your custom executor via a plugin."
                "In Airflow 2.0, you only need to provide a full path to the the custom executor module."
                "see 'Custom executors is loaded using full import path' section at the link below:"
                "https://github.com/apache/airflow/blob/master/UPDATING.md"
            )
