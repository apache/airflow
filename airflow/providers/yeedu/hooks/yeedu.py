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

"""
Yeedu hook.
This hook enable the submitting and running of jobs to the Yeedu platform. Internally the
operators talk to the ``/spark/job`
"""

import requests
import time
import logging
from typing import Tuple
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

class YeeduHook(BaseHook):
    """
    YeeduHook provides an interface to interact with the Yeedu API.

    :param token: Yeedu API token.
    :type token: str
    :param hostname: Yeedu API hostname.
    :type hostname: str
    :param workspace_id: The ID of the Yeedu workspace.
    :type workspace_id: int
    :param args: Additional positional arguments.
    :param kwargs: Additional keyword arguments.
    """

    def __init__(self, token: str, hostname: str, workspace_id: int, *args, **kwargs) -> None:
        """
        Initializes YeeduHook with the necessary configurations to communicate with the Yeedu API.

        :param token: Yeedu API token.
        :param hostname: Yeedu API hostname.
        :param workspace_id: The ID of the Yeedu workspace.
        """

        super().__init__(*args, **kwargs)
        self.token: str = token
        self.headers: dict = {
            'accept': 'application/json',
            'Authorization': f"Bearer {token}",
            'Content-Type': 'application/json'
        }
        self.base_url: str = f'http://{hostname}/api/v1/workspace/{workspace_id}/'

    def _api_request(self, method: str, url: str, data=None) -> requests.Response:
        """
        Makes an HTTP request to the Yeedu API.

        :param method: The HTTP method (GET, POST, etc.).
        :param url: The URL of the API endpoint.
        :param data: The JSON data for the request.
        :return: The API response.
        """

        try:
            response: requests.Response = requests.request(method, url, headers=self.headers, json=data)
            return response
                        
        except Exception as e:
            raise AirflowException(e)
            


    def submit_job(self, job_conf_id: str) -> int:
        """
        Submits a job to Yeedu.

        :param job_conf_id: The job configuration ID.
        :return: The ID of the submitted job.
        """

        try:
            job_url: str = self.base_url + 'spark/job'
            data: dict = {'job_conf_id': job_conf_id}
            response = self._api_request('POST', job_url, data)
            api_status_code = response.status_code
            response_json = response.json()
            if api_status_code == 200:
                job_id = response.json().get('job_id')
                if job_id:
                    return job_id
                else:
                    raise AirflowException(response_json)
            else:
                raise AirflowException(response_json)
                
        except Exception as e:
            raise AirflowException(e)
            


    def get_job_status(self, job_id: int) -> requests.Response:
        """
        Retrieves the status of a Yeedu job.

        :param job_id: The ID of the job.
        :return: The API response containing job status.
        """

        try:
            job_status_url: str = self.base_url + f'spark/job/{job_id}'
            return self._api_request('GET', job_status_url)
                        
        except Exception as e:
            raise AirflowException(e)

            

    def get_job_logs(self, job_id: int, log_type: str) -> str:
        """
        Retrieves logs for a Yeedu job.

        :param job_id: The ID of the job.
        :param log_type: The type of logs to retrieve ('stdout' or 'stderr').
        :return: The logs for the specified job and log type.
        """

        try:
            logs_url: str = self.base_url + f'spark/job/{job_id}/log/{log_type}'
            time.sleep(10)
            return self._api_request('GET', logs_url).text
        
        except Exception as e:
            raise AirflowException(e)
            

    def wait_for_completion(self, job_id: int) -> str:
        """
        Waits for the completion of a Yeedu job and retrieves its final status.

        :param job_id: The ID of the job.
        :return: The final status of the job.
        :raises AirflowException: If continuous API failures reach the threshold.
        """
        
        try:
            max_attempts: int = 5
            attempts_failure: int = 0

            while True:
                time.sleep(5)
                # Check job status
                response: requests.Response = self.get_job_status(job_id)
                api_status_code: int = response.status_code
                self.log.info("Current API Status Code: %s",api_status_code)
                if api_status_code == 200:
                    # If API status is a success, reset the failure attempts counter
                    attempts_failure = 0
                    job_status: str = response.json().get('job_status')
                    self.log.info("Current Job Status: %s ", job_status)
                    if job_status in ['DONE', 'ERROR', 'TERMINATED', 'KILLED']:
                        break
                # If API status is an error, increment the failure attempts counter
                else:
                    attempts_failure += 1
                    self.log.info("failure attempts : %s", attempts_failure)

                # If continuous failures reach the threshold, throw an error
                if attempts_failure == max_attempts:
                    raise AirflowException("Continuous API failure reached the threshold")

            return job_status
        
        except Exception as e:
            raise AirflowException(e)