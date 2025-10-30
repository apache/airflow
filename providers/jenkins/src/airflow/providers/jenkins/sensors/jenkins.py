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

from collections.abc import Iterable

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseSensorOperator, Context
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook


class JenkinsBuildSensor(BaseSensorOperator):
    """
    Monitor a Jenkins job and pass when it is finished building.

    This is regardless of the build outcome.

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
        target_states: Iterable[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.build_number = build_number
        self.jenkins_connection_id = jenkins_connection_id
        self.target_states = target_states or ["SUCCESS", "FAILED"]

    def poke(self, context: Context) -> bool:
        self.log.info("Poking jenkins job %s", self.job_name)
        hook = JenkinsHook(self.jenkins_connection_id)
        build_number = self.build_number or hook.get_latest_build_number(self.job_name)
        is_building = hook.get_build_building_state(self.job_name, build_number)

        if is_building:
            self.log.info("Build still ongoing!")
            return False

        build_result = hook.get_build_result(self.job_name, build_number)
        self.log.info("Build is finished, result is %s", "build_result")
        if build_result in self.target_states:
            return True
        message = (
            f"Build {build_number} finished with a result {build_result}, "
            f"which does not meet the target state {self.target_states}."
        )
        raise AirflowException(message)
