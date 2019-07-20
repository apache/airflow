# -*- coding: utf-8 -*-
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
This module contains a Google Dataflow Hook.
"""

import json
import re
import select
import subprocess
import time
import uuid

from googleapiclient.discovery import build

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

# This is the default location
# https://cloud.google.com/dataflow/pipelines/specifying-exec-params
DEFAULT_DATAFLOW_LOCATION = 'us-central1'


class DataflowJobStatus:
    """
    Helper class with Dataflow job statuses.
    """
    JOB_STATE_DONE = "JOB_STATE_DONE"
    JOB_STATE_RUNNING = "JOB_STATE_RUNNING"
    JOB_TYPE_STREAMING = "JOB_TYPE_STREAMING"
    JOB_STATE_FAILED = "JOB_STATE_FAILED"
    JOB_STATE_CANCELLED = "JOB_STATE_CANCELLED"
    JOB_STATE_PENDING = "JOB_STATE_PENDING"
    FAILED_END_STATES = {JOB_STATE_FAILED, JOB_STATE_CANCELLED}
    SUCCEEDED_END_STATES = {JOB_STATE_DONE}
    END_STATES = SUCCEEDED_END_STATES | FAILED_END_STATES


class _DataflowJob(LoggingMixin):
    def __init__(self, dataflow, project_number, name, location, poll_sleep=10,
                 job_id=None, num_retries=None, multiple_jobs=None):
        self._dataflow = dataflow
        self._project_number = project_number
        self._job_name = name
        self._job_location = location
        self._multiple_jobs = multiple_jobs
        self._job_id = job_id
        self._num_retries = num_retries
        if self._num_retries is None:
            self._num_retries = 0
        self._poll_sleep = poll_sleep
        self._jobs = self._get_jobs()

    def is_job_running(self):
        """
        Helper method to check if jos is still running in dataflow

        :return: True if job is running.
        :rtype: bool
        """
        for job in self._jobs:
            if job['currentState'] not in DataflowJobStatus.END_STATES:
                return True
        return False

    # pylint: disable=too-many-nested-blocks
    def _get_dataflow_jobs(self):
        """
        Helper method to get list of jobs that start with job name or id

        :return: list of jobs including id's
        :rtype: list
        """
        if not self._multiple_jobs and self._job_id:
            return self._dataflow.projects().locations().jobs().get(
                projectId=self._project_number,
                location=self._job_location,
                jobId=self._job_id).execute(num_retries=self._num_retries)
        elif self._job_name:
            jobs = self._dataflow.projects().locations().jobs().list(
                projectId=self._project_number,
                location=self._job_location
            ).execute(num_retries=self._num_retries)
            dataflow_jobs = []
            if jobs:
                for job in jobs['jobs']:
                    if job['name'].startswith(self._job_name.lower()):
                        dataflow_jobs.append(job)
            if len(dataflow_jobs) == 1:
                self._job_id = dataflow_jobs[0]['id']
            return dataflow_jobs
        else:
            raise Exception('Missing both dataflow job ID and name.')

    def _get_jobs(self):
        """
        Helper method to get all jobs by name

        :return: jobs
        :rtype: list
        """
        self._jobs = self._get_dataflow_jobs()

        for job in self._jobs:
            if job and 'currentState' in job:
                self._job_state = job['currentState']
                self.log.info(
                    'Google Cloud DataFlow job %s is %s',
                    job['name'], job['currentState']
                )
            elif job:
                self.log.info(
                    'Google Cloud DataFlow with job_id %s has name %s',
                    self._job_id, job['name']
                )
            else:
                self.log.info(
                    'Google Cloud DataFlow job not available yet..'
                )

        return self._jobs

    # pylint: disable=too-many-nested-blocks
    def check_dataflow_job_state(self, job):
        """
        Helper method to check the state of all jobs in dataflow for this task
        if job failed raise exception
        :return: True if job is done.
        :rtype: bool
        :raise: Exception
        """
        if DataflowJobStatus.JOB_STATE_DONE == job['currentState']:
            # check all jobs are done
            count_not_done = 0
            for inner_jobs in self._jobs:
                if inner_jobs and 'currentState' in job:
                    if not DataflowJobStatus.JOB_STATE_DONE == inner_jobs['currentState']:
                        count_not_done += 1
            if count_not_done == 0:
                return True
        elif DataflowJobStatus.JOB_STATE_FAILED == job['currentState']:
            raise Exception("Google Cloud Dataflow job {} has failed.".format(
                job['name']))
        elif DataflowJobStatus.JOB_STATE_CANCELLED == job['currentState']:
            raise Exception("Google Cloud Dataflow job {} was cancelled.".format(
                job['name']))
        elif DataflowJobStatus.JOB_STATE_RUNNING == job['currentState'] and \
                DataflowJobStatus.JOB_TYPE_STREAMING == job['type']:
            return True
        elif job['currentState'] in {DataflowJobStatus.JOB_STATE_RUNNING,
                                     DataflowJobStatus.JOB_STATE_PENDING}:
            time.sleep(self._poll_sleep)
        else:
            self.log.debug(str(job))
            raise Exception(
                "Google Cloud Dataflow job {} was unknown state: {}".format(
                    job['name'], job['currentState']))
        return False

    def wait_for_done(self):
        """
        Helper method to wait for result of submitted job.

        :return: True if job is done.
        :rtype: bool
        :raise: Exception
        """
        while True:
            for job in self._jobs:
                if job and 'currentState' in job:
                    if self.check_dataflow_job_state(job):
                        return True
                else:
                    time.sleep(self._poll_sleep)
            self._jobs = self._get_jobs()

    def get(self):
        """
        Returns Dataflow job.
        :return: list of jobs
        :rtype: list
        """
        return self._jobs


class _Dataflow(LoggingMixin):
    def __init__(self, cmd):
        self.log.info("Running command: %s", ' '.join(cmd))
        self._proc = subprocess.Popen(
            cmd,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True)

    def _line(self, fd):
        if fd == self._proc.stderr.fileno():
            line = b''.join(self._proc.stderr.readlines())
            if line:
                self.log.warning(line[:-1])
            return line

        if fd == self._proc.stdout.fileno():
            line = b''.join(self._proc.stdout.readlines())
            if line:
                self.log.info(line[:-1])
            return line

        raise Exception("No data in stderr or in stdout.")

    @staticmethod
    def _extract_job(line):
        """
        Extracts job_id.

        :param line: URL from which job_id has to be extracted
        :type line: bytes
        :return: job_id or None if no match
        :rtype: Union[None, str]
        """
        # Job id info: https://goo.gl/SE29y9.
        job_id_pattern = re.compile(
            br'.*console.cloud.google.com/dataflow.*/jobs/([a-z|0-9|A-Z|\-|\_]+).*')
        matched_job = job_id_pattern.search(line or '')
        if matched_job:
            return matched_job.group(1).decode()
        return None

    def wait_for_done(self):
        """
        Waits for Dataflow job to complete.

        :return: Job id
        :rtype: str
        """
        reads = [self._proc.stderr.fileno(), self._proc.stdout.fileno()]
        self.log.info("Start waiting for DataFlow process to complete.")
        job_id = None
        # Make sure logs are processed regardless whether the subprocess is
        # terminated.
        process_ends = False
        while True:
            ret = select.select(reads, [], [], 5)
            if ret is None:
                self.log.info("Waiting for DataFlow process to complete.")
                continue

            for raw_line in ret[0]:
                line = self._line(raw_line)
                if line:
                    job_id = job_id or self._extract_job(line)

            if process_ends:
                break
            if self._proc.poll() is not None:
                # Mark process completion but allows its outputs to be consumed.
                process_ends = True
        if self._proc.returncode != 0:
            raise Exception("DataFlow failed with return code {}".format(
                self._proc.returncode))
        return job_id


class DataFlowHook(GoogleCloudBaseHook):
    """
    Hook for Google Dataflow.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 poll_sleep=10):
        self.poll_sleep = poll_sleep
        self.num_retries = self._get_field('num_retries', 5)
        super().__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud Dataflow service object.
        """
        http_authorized = self._authorize()
        return build(
            'dataflow', 'v1b3', http=http_authorized, cache_discovery=False)

    @GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
    def _start_dataflow(self, variables, name, command_prefix, label_formatter, multiple_jobs=False):
        variables = self._set_variables(variables)
        cmd = command_prefix + self._build_cmd(variables, label_formatter)
        job_id = _Dataflow(cmd).wait_for_done()
        _DataflowJob(self.get_conn(), variables['project'], name,
                     variables['region'], self.poll_sleep, job_id, self.num_retries, multiple_jobs) \
            .wait_for_done()

    @staticmethod
    def _set_variables(variables):
        if variables['project'] is None:
            raise Exception('Project not specified')
        if 'region' not in variables.keys():
            variables['region'] = DEFAULT_DATAFLOW_LOCATION
        return variables

    def start_java_dataflow(self, job_name, variables, jar, job_class=None,
                            append_job_name=True, multiple_jobs=False):
        """
        Starts Dataflow java job.

        :param job_name: The name of the job.
        :type job_name: str
        :param variables: Variables passed to the job.
        :type variables: dict
        :param jar: Name of the jar for the job
        :type job_class: str
        :param job_class: Name of the java class for the job.
        :type job_class: str
        :param append_job_name: True if unique suffix has to be appended to job name.
        :type append_job_name: bool
        :param multiple_jobs: True if to check for multiple job in dataflow
        :type multiple_jobs: bool
        """
        name = self._build_dataflow_job_name(job_name, append_job_name)
        variables['jobName'] = name

        def label_formatter(labels_dict):
            return ['--labels={}'.format(
                json.dumps(labels_dict).replace(' ', ''))]

        command_prefix = (["java", "-cp", jar, job_class] if job_class
                          else ["java", "-jar", jar])
        self._start_dataflow(variables, name, command_prefix, label_formatter, multiple_jobs)

    def start_template_dataflow(self, job_name, variables, parameters, dataflow_template,
                                append_job_name=True):
        """
        Starts Dataflow template job.

        :param job_name: The name of the job.
        :type job_name: str
        :param variables: Variables passed to the job.
        :type variables: dict
        :param parameters: Parameters fot the template
        :type parameters: dict
        :param dataflow_template: GCS path to the template.
        :type dataflow_template: str
        :param append_job_name: True if unique suffix has to be appended to job name.
        :type append_job_name: bool
        """
        variables = self._set_variables(variables)
        name = self._build_dataflow_job_name(job_name, append_job_name)
        self._start_template_dataflow(
            name, variables, parameters, dataflow_template)

    def start_python_dataflow(self, job_name, variables, dataflow, py_options,
                              append_job_name=True):
        """
        Starts Dataflow job.

        :param job_name: The name of the job.
        :type job_name: str
        :param variables: Variables passed to the job.
        :type variables: dict
        :param dataflow: Name of the Dataflow process.
        :type dataflow: str
        :param py_options: Additional options.
        :type py_options: list
        :param append_job_name: True if unique suffix has to be appended to job name.
        :type append_job_name: bool
        """
        name = self._build_dataflow_job_name(job_name, append_job_name)
        variables['job_name'] = name

        def label_formatter(labels_dict):
            return ['--labels={}={}'.format(key, value)
                    for key, value in labels_dict.items()]

        self._start_dataflow(variables, name, ["python2"] + py_options + [dataflow],
                             label_formatter)

    @staticmethod
    def _build_dataflow_job_name(job_name, append_job_name=True):
        base_job_name = str(job_name).replace('_', '-')

        if not re.match(r"^[a-z]([-a-z0-9]*[a-z0-9])?$", base_job_name):
            raise ValueError(
                'Invalid job_name ({}); the name must consist of'
                'only the characters [-a-z0-9], starting with a '
                'letter and ending with a letter or number '.format(base_job_name))

        if append_job_name:
            safe_job_name = base_job_name + "-" + str(uuid.uuid4())[:8]
        else:
            safe_job_name = base_job_name

        return safe_job_name

    @staticmethod
    def _build_cmd(variables, label_formatter):
        command = ["--runner=DataflowRunner"]
        if variables is not None:
            for attr, value in variables.items():
                if attr == 'labels':
                    command += label_formatter(value)
                elif value is None or value.__len__() < 1:
                    command.append("--" + attr)
                else:
                    command.append("--" + attr + "=" + value)
        return command

    def _start_template_dataflow(self, name, variables, parameters,
                                 dataflow_template):
        # Builds RuntimeEnvironment from variables dictionary
        # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
        environment = {}
        for key in ['numWorkers', 'maxWorkers', 'zone', 'serviceAccountEmail',
                    'tempLocation', 'bypassTempDirValidation', 'machineType',
                    'additionalExperiments', 'network', 'subnetwork', 'additionalUserLabels']:
            if key in variables:
                environment.update({key: variables[key]})
        body = {"jobName": name,
                "parameters": parameters,
                "environment": environment}
        service = self.get_conn()
        request = service.projects().locations().templates().launch(  # pylint: disable=no-member
            projectId=variables['project'],
            location=variables['region'],
            gcsPath=dataflow_template,
            body=body
        )
        response = request.execute(num_retries=self.num_retries)
        variables = self._set_variables(variables)
        _DataflowJob(self.get_conn(), variables['project'], name, variables['region'],
                     self.poll_sleep, num_retries=self.num_retries).wait_for_done()
        return response

    def is_job_dataflow_running(self, name, variables):
        """
        Helper method to check if jos is still running in dataflow

        :return: True if job is running.
        :rtype: bool
        """
        variables = self._set_variables(variables)
        job = _DataflowJob(self.get_conn(), variables['project'], name,
                           variables['region'], self.poll_sleep)
        return job.is_job_running()
