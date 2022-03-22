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

from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError

import jenkins
from jenkins import JenkinsException

from airflow.exceptions import AirflowException
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.sensors.base import BaseSensorOperator

class JenkinsSensor(BaseSensorOperator):
    """
    Monitor a jenkins job and pass when it is finished building. This is regardless of the build outcome.

    :param jenkins_connection_id: The jenkins connection to use for this job
    :param job_name: The name of the job to check
    :param build_number: Build number to check - if None, the latest build will be used
    """

    def __init__(
        self,
        *,
        jenkins_connection_id: str,
        job_name: str,
        build_number: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.build_number = build_number
        self.jenkins_connection_id = jenkins_connection_id

    def get_hook(self) -> JenkinsHook:
        """Instantiate jenkins hook"""
        return JenkinsHook(self.jenkins_connection_id)

    def poke(self, context: 'Context') -> bool:    
        try:
            self.log.info(f"Poking jenkins job {self.job_name}")

            jenkins_server = self.get_hook().get_jenkins_server()
            if not self.build_number:
                job_info = jenkins_server.get_job_info(self.job_name)
                build_number_to_check = job_info['lastBuild']['number']
                self.log.info(f"Build number not specified, getting latest build info from Jenkins")
            else:
                build_number_to_check = self.build_number
            
            self.log.info(f"Checking build number: #{build_number_to_check}")

            build_info = jenkins_server.get_build_info(self.job_name, build_number_to_check)
            if build_info['building']:
                self.log.info(f"Build #{build_number_to_check} is still building")
                return False
            else:
                self.log.info(f"Build #{build_number_to_check} is finished")
                return True
        except jenkins.NotFoundException as err:
            raise AirflowException(f'Jenkins job status check failed. Final error was: {err.resp.status}')
        except jenkins.JenkinsException as err:
            raise AirflowException(
                f'Jenkins call failed with error : {err}'
                )


