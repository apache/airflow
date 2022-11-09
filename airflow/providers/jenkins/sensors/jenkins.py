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
from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class JenkinsBuildSensor(BaseSensorOperator):
    """
    Monitor a jenkins job and pass when it is finished building. This is regardless of the build outcome.
    This sensor depend on python-jenkins library,

    :param jenkins_connection_id: The jenkins connection to use for this job
    :param job_name: The name of the job to check
    :param build_number: Build number to check - if None, the latest build will be used
    """

    def __init__(
        self,
        *,
        jenkins_connection_id: str,
        job_name: str,
        build_number: int | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.build_number = build_number
        self.jenkins_connection_id = jenkins_connection_id

    def poke(self, context: Context) -> bool:
        self.log.info("Poking jenkins job %s", self.job_name)
        hook = JenkinsHook(self.jenkins_connection_id)
        is_building = hook.get_build_building_state(self.job_name, self.build_number)

        if is_building:
            self.log.info("Build still ongoing!")
            return False
        else:
            self.log.info("Build is finished.")
            return True
