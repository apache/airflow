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

from airflow.upgrade.rules.base_rule import BaseRule
from airflow.configuration import conf


class MesosExecutorRemovedRule(BaseRule):
    """
    MesosExecutorRemovedRule class to ease upgrade to Airflow 2.0
    """
    title = "Removal of Mesos Executor"
    description = "The Mesos Executor has been deprecated as it was not widely used and not maintained."

    def check(self):
        executor_key = conf.get(section="core", key="executor")
        if executor_key == "MesosExecutor":
            return (
                "The Mesos Executor has been deprecated as it was not widely used and not maintained."
                "Please migrate to any of the supported executors."
                "See https://airflow.apache.org/docs/stable/executor/index.html for more details."
            )
