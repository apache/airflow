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

import ast
import json
import socket
import time
from typing import Any, Dict, List, Mapping, Optional, Union
from urllib.error import HTTPError, URLError

import jenkins
from jenkins import Jenkins, JenkinsException
from requests import Request

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
from airflow.utils.decorators import apply_defaults

JenkinsRequest = Mapping[str, Any]
ParamType = Optional[Union[str, Dict, List]]


def jenkins_request_with_headers(jenkins_server: Jenkins, req: Request) -> Optional[JenkinsRequest]:
    """
    We need to get the headers in addition to the body answer
    to get the location from them
    This function uses jenkins_request method from python-jenkins library
    with just the return call changed

    :param jenkins_server: The server to query
    :param req: The request to execute
    :return: Dict containing the response body (key body)
        and the headers coming along (headers)
    """
    try:
        response = jenkins_server.jenkins_request(req)
        response_body = response.content
        response_headers = response.headers
        if response_body is None:
            raise jenkins.EmptyResponseException(
                "Error communicating with server[%s]: empty response" % jenkins_server.server
            )
        return {'body': response_body.decode('utf-8'), 'headers': response_headers}
    except HTTPError as e:
        # Jenkins's funky authentication means its nigh impossible to distinguish errors.
        if e.code in [401, 403, 500]:
            raise JenkinsException(f'Error in request. Possibly authentication failed [{e.code}]: {e.reason}')
        elif e.code == 404:
            raise jenkins.NotFoundException('Requested item could not be found')
        else:
            raise
    except socket.timeout as e:
        raise jenkins.TimeoutException('Error in request: %s' % e)
    except URLError as e:
        raise JenkinsException('Error in request: %s' % e.reason)
    return None


class JenkinsJobTriggerOperator(BaseOperator):
    """
    Trigger a Jenkins Job and monitor it's execution.
    This operator depend on python-jenkins library,
    version >= 0.4.15 to communicate with jenkins server.
    You'll also need to configure a Jenkins connection in the connections screen.

    :param jenkins_connection_id: The jenkins connection to use for this job
    :type jenkins_connection_id: str
    :param job_name: The name of the job to trigger
    :type job_name: str
    :param parameters: The parameters block provided to jenkins for use in
        the API call when triggering a build. (templated)
    :type parameters: str, Dict, or List
    :param sleep_time: How long will the operator sleep between each status
        request for the job (min 1, default 10)
    :type sleep_time: int
    :param max_try_before_job_appears: The maximum number of requests to make
        while waiting for the job to appears on jenkins server (default 10)
    :type max_try_before_job_appears: int
    """

    template_fields = ('parameters',)
    template_ext = ('.json',)
    ui_color = '#f9ec86'

    @apply_defaults
    def __init__(
        self,
        *,
        jenkins_connection_id: str,
        job_name: str,
        parameters: ParamType = "",
        sleep_time: int = 10,
        max_try_before_job_appears: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.parameters = parameters
        self.sleep_time = max(sleep_time, 1)
        self.jenkins_connection_id = jenkins_connection_id
        self.max_try_before_job_appears = max_try_before_job_appears

    def build_job(self, jenkins_server: Jenkins, params: ParamType = "") -> Optional[JenkinsRequest]:
        """
        This function makes an API call to Jenkins to trigger a build for 'job_name'
        It returned a dict with 2 keys : body and headers.
        headers contains also a dict-like object which can be queried to get
        the location to poll in the queue.

        :param jenkins_server: The jenkins server where the job should be triggered
        :param params: The parameters block to provide to jenkins API call.
        :return: Dict containing the response body (key body)
            and the headers coming along (headers)
        """
        # Since params can be either JSON string, dictionary, or list,
        # check type and pass to build_job_url
        if params and isinstance(params, str):
            params = ast.literal_eval(params)

        # We need a None to call the non-parametrized jenkins api end point
        if not params:
            params = None

        request = Request(method='POST', url=jenkins_server.build_job_url(self.job_name, params, None))
        return jenkins_request_with_headers(jenkins_server, request)

    def poll_job_in_queue(self, location: str, jenkins_server: Jenkins) -> int:
        """
        This method poll the jenkins queue until the job is executed.
        When we trigger a job through an API call,
        the job is first put in the queue without having a build number assigned.
        Thus we have to wait the job exit the queue to know its build number.
        To do so, we have to add /api/json (or /api/xml) to the location
        returned by the build_job call and poll this file.
        When a 'executable' block appears in the json, it means the job execution started
        and the field 'number' then contains the build number.

        :param location: Location to poll, returned in the header of the build_job call
        :param jenkins_server: The jenkins server to poll
        :return: The build_number corresponding to the triggered job
        """
        try_count = 0
        location += '/api/json'
        # TODO Use get_queue_info instead
        # once it will be available in python-jenkins (v > 0.4.15)
        self.log.info('Polling jenkins queue at the url %s', location)
        while try_count < self.max_try_before_job_appears:
            location_answer = jenkins_request_with_headers(
                jenkins_server, Request(method='POST', url=location)
            )
            if location_answer is not None:
                json_response = json.loads(location_answer['body'])
                if 'executable' in json_response:
                    build_number = json_response['executable']['number']
                    self.log.info('Job executed on Jenkins side with the build number %s', build_number)
                    return build_number
            try_count += 1
            time.sleep(self.sleep_time)
        raise AirflowException(
            "The job hasn't been executed after polling " f"the queue {self.max_try_before_job_appears} times"
        )

    def get_hook(self) -> JenkinsHook:
        """Instantiate jenkins hook"""
        return JenkinsHook(self.jenkins_connection_id)

    def execute(self, context: Mapping[Any, Any]) -> Optional[str]:
        if not self.jenkins_connection_id:
            self.log.error(
                'Please specify the jenkins connection id to use.'
                'You must create a Jenkins connection before'
                ' being able to use this operator'
            )
            raise AirflowException(
                'The jenkins_connection_id parameter is missing, impossible to trigger the job'
            )

        if not self.job_name:
            self.log.error("Please specify the job name to use in the job_name parameter")
            raise AirflowException('The job_name parameter is missing,impossible to trigger the job')

        self.log.info(
            'Triggering the job %s on the jenkins : %s with the parameters : %s',
            self.job_name,
            self.jenkins_connection_id,
            self.parameters,
        )
        jenkins_server = self.get_hook().get_jenkins_server()
        jenkins_response = self.build_job(jenkins_server, self.parameters)
        if jenkins_response:
            build_number = self.poll_job_in_queue(jenkins_response['headers']['Location'], jenkins_server)

        time.sleep(self.sleep_time)
        keep_polling_job = True
        build_info = None
        # pylint: disable=too-many-nested-blocks
        while keep_polling_job:
            try:
                build_info = jenkins_server.get_build_info(name=self.job_name, number=build_number)
                if build_info['result'] is not None:
                    keep_polling_job = False
                    # Check if job had errors.
                    if build_info['result'] != 'SUCCESS':
                        raise AirflowException(
                            'Jenkins job failed, final state : %s.'
                            'Find more information on job url : %s'
                            % (build_info['result'], build_info['url'])
                        )
                else:
                    self.log.info('Waiting for job to complete : %s , build %s', self.job_name, build_number)
                    time.sleep(self.sleep_time)
            except jenkins.NotFoundException as err:
                # pylint: disable=no-member
                raise AirflowException(
                    'Jenkins job status check failed. Final error was: ' f'{err.resp.status}'
                )
            except jenkins.JenkinsException as err:
                raise AirflowException(
                    f'Jenkins call failed with error : {err}, if you have parameters '
                    'double check them, jenkins sends back '
                    'this exception for unknown parameters'
                    'You can also check logs for more details on this exception '
                    '(jenkins_url/log/rss)'
                )
        if build_info:
            # If we can we return the url of the job
            # for later use (like retrieving an artifact)
            return build_info['url']
        return None
