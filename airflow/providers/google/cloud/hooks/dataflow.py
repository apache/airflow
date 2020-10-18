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
import functools
import json
import re
import select
import shlex
import subprocess
import textwrap
import time
import uuid
import warnings
from copy import deepcopy
from tempfile import TemporaryDirectory
from typing import Any, Callable, List, Optional, Sequence, TypeVar, Union, cast

from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.python_virtualenv import prepare_virtualenv

# This is the default location
# https://cloud.google.com/dataflow/pipelines/specifying-exec-params
DEFAULT_DATAFLOW_LOCATION = 'us-central1'


JOB_ID_PATTERN = re.compile(
    r'Submitted job: (?P<job_id_java>.*)|Created job with id: \[(?P<job_id_python>.*)\]'
)

T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def _fallback_variable_parameter(parameter_name: str, variable_key_name: str) -> Callable[[T], T]:
    def _wrapper(func: T) -> T:
        """
        Decorator that provides fallback for location from `region` key in `variables` parameters.

        :param func: function to wrap
        :return: result of the function call
        """

        @functools.wraps(func)
        def inner_wrapper(self: "DataflowHook", *args, **kwargs):
            if args:
                raise AirflowException(
                    "You must use keyword arguments in this methods rather than positional"
                )

            parameter_location = kwargs.get(parameter_name)
            variables_location = kwargs.get('variables', {}).get(variable_key_name)

            if parameter_location and variables_location:
                raise AirflowException(
                    f"The mutually exclusive parameter `{parameter_name}` and `{variable_key_name}` key "
                    f"in `variables` parameter are both present. Please remove one."
                )
            if parameter_location or variables_location:
                kwargs[parameter_name] = parameter_location or variables_location
            if variables_location:
                copy_variables = deepcopy(kwargs['variables'])
                del copy_variables[variable_key_name]
                kwargs['variables'] = copy_variables

            return func(self, *args, **kwargs)

        return cast(T, inner_wrapper)

    return _wrapper


_fallback_to_location_from_variables = _fallback_variable_parameter('location', 'region')
_fallback_to_project_id_from_variables = _fallback_variable_parameter('project_id', 'project')


class DataflowJobStatus:
    """
    Helper class with Dataflow job statuses.
    Reference: https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState
    """

    JOB_STATE_DONE = "JOB_STATE_DONE"
    JOB_STATE_UNKNOWN = "JOB_STATE_UNKNOWN"
    JOB_STATE_STOPPED = "JOB_STATE_STOPPED"
    JOB_STATE_RUNNING = "JOB_STATE_RUNNING"
    JOB_STATE_FAILED = "JOB_STATE_FAILED"
    JOB_STATE_CANCELLED = "JOB_STATE_CANCELLED"
    JOB_STATE_UPDATED = "JOB_STATE_UPDATED"
    JOB_STATE_DRAINING = "JOB_STATE_DRAINING"
    JOB_STATE_DRAINED = "JOB_STATE_DRAINED"
    JOB_STATE_PENDING = "JOB_STATE_PENDING"
    JOB_STATE_CANCELLING = "JOB_STATE_CANCELLING"
    JOB_STATE_QUEUED = "JOB_STATE_QUEUED"
    FAILED_END_STATES = {JOB_STATE_FAILED, JOB_STATE_CANCELLED}
    SUCCEEDED_END_STATES = {JOB_STATE_DONE, JOB_STATE_UPDATED, JOB_STATE_DRAINED}
    TERMINAL_STATES = SUCCEEDED_END_STATES | FAILED_END_STATES
    AWAITING_STATES = {
        JOB_STATE_RUNNING,
        JOB_STATE_PENDING,
        JOB_STATE_QUEUED,
        JOB_STATE_CANCELLING,
        JOB_STATE_DRAINING,
        JOB_STATE_STOPPED,
    }


class DataflowJobType:
    """
    Helper class with Dataflow job types.
    """

    JOB_TYPE_UNKNOWN = "JOB_TYPE_UNKNOWN"
    JOB_TYPE_BATCH = "JOB_TYPE_BATCH"
    JOB_TYPE_STREAMING = "JOB_TYPE_STREAMING"


class _DataflowJobsController(LoggingMixin):
    """
    Interface for communication with Google API.

    It's not use Apache Beam, but only Google Dataflow API.

    :param dataflow: Discovery resource
    :param project_number: The Google Cloud Project ID.
    :param location: Job location.
    :param poll_sleep: The status refresh rate for pending operations.
    :param name: The Job ID prefix used when the multiple_jobs option is passed is set to True.
    :param job_id: ID of a single job.
    :param num_retries: Maximum number of retries in case of connection problems.
    :param multiple_jobs: If set to true this task will be searched by name prefix (``name`` parameter),
        not by specific job ID, then actions will be performed on all matching jobs.
    """

    def __init__(
        self,
        dataflow: Any,
        project_number: str,
        location: str,
        poll_sleep: int = 10,
        name: Optional[str] = None,
        job_id: Optional[str] = None,
        num_retries: int = 0,
        multiple_jobs: bool = False,
    ) -> None:

        super().__init__()
        self._dataflow = dataflow
        self._project_number = project_number
        self._job_name = name
        self._job_location = location
        self._multiple_jobs = multiple_jobs
        self._job_id = job_id
        self._num_retries = num_retries
        self._poll_sleep = poll_sleep
        self._jobs: Optional[List[dict]] = None

    def is_job_running(self) -> bool:
        """
        Helper method to check if jos is still running in dataflow

        :return: True if job is running.
        :rtype: bool
        """
        self._refresh_jobs()
        if not self._jobs:
            return False

        for job in self._jobs:
            if job['currentState'] not in DataflowJobStatus.TERMINAL_STATES:
                return True
        return False

    # pylint: disable=too-many-nested-blocks
    def _get_current_jobs(self) -> List[dict]:
        """
        Helper method to get list of jobs that start with job name or id

        :return: list of jobs including id's
        :rtype: list
        """
        if not self._multiple_jobs and self._job_id:
            return [self._fetch_job_by_id(self._job_id)]
        elif self._job_name:
            jobs = self._fetch_jobs_by_prefix_name(self._job_name.lower())
            if len(jobs) == 1:
                self._job_id = jobs[0]['id']
            return jobs
        else:
            raise Exception('Missing both dataflow job ID and name.')

    def _fetch_job_by_id(self, job_id: str) -> dict:
        return (
            self._dataflow.projects()
            .locations()
            .jobs()
            .get(projectId=self._project_number, location=self._job_location, jobId=job_id)
            .execute(num_retries=self._num_retries)
        )

    def _fetch_all_jobs(self) -> List[dict]:
        request = (
            self._dataflow.projects()
            .locations()
            .jobs()
            .list(projectId=self._project_number, location=self._job_location)
        )
        jobs: List[dict] = []
        while request is not None:
            response = request.execute(num_retries=self._num_retries)
            jobs.extend(response["jobs"])

            request = (
                self._dataflow.projects()
                .locations()
                .jobs()
                .list_next(previous_request=request, previous_response=response)
            )
        return jobs

    def _fetch_jobs_by_prefix_name(self, prefix_name: str) -> List[dict]:
        jobs = self._fetch_all_jobs()
        jobs = [job for job in jobs if job['name'].startswith(prefix_name)]
        return jobs

    def _refresh_jobs(self) -> None:
        """
        Helper method to get all jobs by name

        :return: jobs
        :rtype: list
        """
        self._jobs = self._get_current_jobs()

        if self._jobs:
            for job in self._jobs:
                self.log.info('Google Cloud DataFlow job %s is state: %s', job['name'], job['currentState'])
        else:
            self.log.info('Google Cloud DataFlow job not available yet..')

    def _check_dataflow_job_state(self, job) -> bool:
        """
        Helper method to check the state of one job in dataflow for this task
        if job failed raise exception

        :return: True if job is done.
        :rtype: bool
        :raise: Exception
        """
        if DataflowJobStatus.JOB_STATE_DONE == job['currentState']:
            return True
        elif DataflowJobStatus.JOB_STATE_FAILED == job['currentState']:
            raise Exception("Google Cloud Dataflow job {} has failed.".format(job['name']))
        elif DataflowJobStatus.JOB_STATE_CANCELLED == job['currentState']:
            raise Exception("Google Cloud Dataflow job {} was cancelled.".format(job['name']))
        elif (
            DataflowJobStatus.JOB_STATE_RUNNING == job['currentState']
            and DataflowJobType.JOB_TYPE_STREAMING == job['type']
        ):
            return True
        elif job['currentState'] in DataflowJobStatus.AWAITING_STATES:
            return False
        self.log.debug("Current job: %s", str(job))
        raise Exception(
            "Google Cloud Dataflow job {} was unknown state: {}".format(job['name'], job['currentState'])
        )

    def wait_for_done(self) -> None:
        """
        Helper method to wait for result of submitted job.
        """
        self.log.info("Start waiting for done.")
        self._refresh_jobs()
        while self._jobs and not all(self._check_dataflow_job_state(job) for job in self._jobs):
            self.log.info("Waiting for done. Sleep %s s", self._poll_sleep)
            time.sleep(self._poll_sleep)
            self._refresh_jobs()

    def get_jobs(self, refresh=False) -> List[dict]:
        """
        Returns Dataflow jobs.

        :return: list of jobs
        :rtype: list
        """
        if not self._jobs or refresh:
            self._refresh_jobs()
        if not self._jobs:
            raise ValueError("Could not read _jobs")

        return self._jobs

    def cancel(self) -> None:
        """
        Cancels current job
        """
        jobs = self.get_jobs()
        job_ids = [job['id'] for job in jobs if job['currentState'] not in DataflowJobStatus.TERMINAL_STATES]
        if job_ids:
            batch = self._dataflow.new_batch_http_request()
            self.log.info("Canceling jobs: %s", ", ".join(job_ids))
            for job_id in job_ids:
                batch.add(
                    self._dataflow.projects()
                    .locations()
                    .jobs()
                    .update(
                        projectId=self._project_number,
                        location=self._job_location,
                        jobId=job_id,
                        body={"requestedState": DataflowJobStatus.JOB_STATE_CANCELLED},
                    )
                )
            batch.execute()
        else:
            self.log.info("No jobs to cancel")


class _DataflowRunner(LoggingMixin):
    def __init__(
        self, cmd: List[str], on_new_job_id_callback: Optional[Callable[[str], None]] = None
    ) -> None:
        super().__init__()
        self.log.info("Running command: %s", ' '.join(shlex.quote(c) for c in cmd))
        self.on_new_job_id_callback = on_new_job_id_callback
        self.job_id: Optional[str] = None
        self._proc = subprocess.Popen(
            cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
        )

    def _process_fd(self, fd):
        """
        Prints output to logs and lookup for job ID in each line.

        :param fd: File descriptor.
        """
        if fd == self._proc.stderr:
            while True:
                line = self._proc.stderr.readline().decode()
                if not line:
                    return
                self._process_line_and_extract_job_id(line)
                self.log.warning(line.rstrip("\n"))

        if fd == self._proc.stdout:
            while True:
                line = self._proc.stdout.readline().decode()
                if not line:
                    return
                self._process_line_and_extract_job_id(line)
                self.log.info(line.rstrip("\n"))

        raise Exception("No data in stderr or in stdout.")

    def _process_line_and_extract_job_id(self, line: str) -> None:
        """
        Extracts job_id.

        :param line: URL from which job_id has to be extracted
        :type line: str
        """
        # Job id info: https://goo.gl/SE29y9.
        matched_job = JOB_ID_PATTERN.search(line)
        if matched_job:
            job_id = matched_job.group('job_id_java') or matched_job.group('job_id_python')
            self.log.info("Found Job ID: %s", job_id)
            self.job_id = job_id
            if self.on_new_job_id_callback:
                self.on_new_job_id_callback(job_id)

    def wait_for_done(self) -> Optional[str]:
        """
        Waits for Dataflow job to complete.

        :return: Job id
        :rtype: Optional[str]
        """
        self.log.info("Start waiting for DataFlow process to complete.")
        self.job_id = None
        reads = [self._proc.stderr, self._proc.stdout]
        while True:
            # Wait for at least one available fd.
            readable_fds, _, _ = select.select(reads, [], [], 5)
            if readable_fds is None:
                self.log.info("Waiting for DataFlow process to complete.")
                continue

            for readable_fd in readable_fds:
                self._process_fd(readable_fd)

            if self._proc.poll() is not None:
                break

        # Corner case: check if more output was created between the last read and the process termination
        for readable_fd in reads:
            self._process_fd(readable_fd)

        self.log.info("Process exited with return code: %s", self._proc.returncode)

        if self._proc.returncode != 0:
            raise Exception("DataFlow failed with return code {}".format(self._proc.returncode))
        return self.job_id


class DataflowHook(GoogleBaseHook):
    """
    Hook for Google Dataflow.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        poll_sleep: int = 10,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        self.poll_sleep = poll_sleep
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )

    def get_conn(self) -> build:
        """
        Returns a Google Cloud Dataflow service object.
        """
        http_authorized = self._authorize()
        return build('dataflow', 'v1b3', http=http_authorized, cache_discovery=False)

    @GoogleBaseHook.provide_gcp_credential_file
    def _start_dataflow(
        self,
        variables: dict,
        name: str,
        command_prefix: List[str],
        label_formatter: Callable[[dict], List[str]],
        project_id: str,
        multiple_jobs: bool = False,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> None:
        cmd = command_prefix + self._build_cmd(variables, label_formatter, project_id)
        runner = _DataflowRunner(cmd=cmd, on_new_job_id_callback=on_new_job_id_callback)
        job_id = runner.wait_for_done()
        job_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=name,
            location=location,
            poll_sleep=self.poll_sleep,
            job_id=job_id,
            num_retries=self.num_retries,
            multiple_jobs=multiple_jobs,
        )
        job_controller.wait_for_done()

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def start_java_dataflow(
        self,
        job_name: str,
        variables: dict,
        jar: str,
        project_id: str,
        job_class: Optional[str] = None,
        append_job_name: bool = True,
        multiple_jobs: bool = False,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> None:
        """
        Starts Dataflow java job.

        :param job_name: The name of the job.
        :type job_name: str
        :param variables: Variables passed to the job.
        :type variables: dict
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param jar: Name of the jar for the job
        :type job_class: str
        :param job_class: Name of the java class for the job.
        :type job_class: str
        :param append_job_name: True if unique suffix has to be appended to job name.
        :type append_job_name: bool
        :param multiple_jobs: True if to check for multiple job in dataflow
        :type multiple_jobs: bool
        :param on_new_job_id_callback: Callback called when the job ID is known.
        :type on_new_job_id_callback: callable
        :param location: Job location.
        :type location: str
        """
        name = self._build_dataflow_job_name(job_name, append_job_name)
        variables['jobName'] = name
        variables['region'] = location

        def label_formatter(labels_dict):
            return ['--labels={}'.format(json.dumps(labels_dict).replace(' ', ''))]

        command_prefix = ["java", "-cp", jar, job_class] if job_class else ["java", "-jar", jar]
        self._start_dataflow(
            variables=variables,
            name=name,
            command_prefix=command_prefix,
            label_formatter=label_formatter,
            project_id=project_id,
            multiple_jobs=multiple_jobs,
            on_new_job_id_callback=on_new_job_id_callback,
            location=location,
        )

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def start_template_dataflow(
        self,
        job_name: str,
        variables: dict,
        parameters: dict,
        dataflow_template: str,
        project_id: str,
        append_job_name: bool = True,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        environment: Optional[dict] = None,
    ) -> dict:
        """
        Starts Dataflow template job.

        :param job_name: The name of the job.
        :type job_name: str
        :param variables: Map of job runtime environment options.
            It will update environment argument if passed.

            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__

        :type variables: dict
        :param parameters: Parameters fot the template
        :type parameters: dict
        :param dataflow_template: GCS path to the template.
        :type dataflow_template: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param append_job_name: True if unique suffix has to be appended to job name.
        :type append_job_name: bool
        :param on_new_job_id_callback: Callback called when the job ID is known.
        :type on_new_job_id_callback: callable
        :param location: Job location.
        :type location: str
        :type environment: Optional, Map of job runtime environment options.

            .. seealso::
                For more information on possible configurations, look at the API documentation
                `https://cloud.google.com/dataflow/pipelines/specifying-exec-params
                <https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment>`__

        :type environment: Optional[dict]
        """
        name = self._build_dataflow_job_name(job_name, append_job_name)

        environment = environment or {}
        # available keys for runtime environment are listed here:
        # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
        environment_keys = [
            'numWorkers',
            'maxWorkers',
            'zone',
            'serviceAccountEmail',
            'tempLocation',
            'bypassTempDirValidation',
            'machineType',
            'additionalExperiments',
            'network',
            'subnetwork',
            'additionalUserLabels',
            'kmsKeyName',
            'ipConfiguration',
            'workerRegion',
            'workerZone',
        ]

        for key in variables:
            if key in environment_keys:
                if key in environment:
                    self.log.warning(
                        "'%s' parameter in 'variables' will override of "
                        "the same one passed in 'environment'!",
                        key,
                    )
                environment.update({key: variables[key]})

        service = self.get_conn()
        # pylint: disable=no-member
        request = (
            service.projects()
            .locations()
            .templates()
            .launch(
                projectId=project_id,
                location=location,
                gcsPath=dataflow_template,
                body={"jobName": name, "parameters": parameters, "environment": environment},
            )
        )
        response = request.execute(num_retries=self.num_retries)

        job_id = response['job']['id']
        if on_new_job_id_callback:
            on_new_job_id_callback(job_id)

        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=name,
            job_id=job_id,
            location=location,
            poll_sleep=self.poll_sleep,
            num_retries=self.num_retries,
        )
        jobs_controller.wait_for_done()
        return response["job"]

    @GoogleBaseHook.fallback_to_default_project_id
    def start_flex_template(
        self,
        body: dict,
        location: str,
        project_id: str,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
    ):
        """
        Starts flex templates with the Dataflow  pipeline.

        :param body: The request body. See:
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch#request-body
        :param location: The location of the Dataflow job (for example europe-west1)
        :type location: str
        :param project_id: The ID of the GCP project that owns the job.
            If set to ``None`` or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param on_new_job_id_callback: A callback that is called when a Job ID is detected.
        :return: the Job
        """
        service = self.get_conn()
        request = (
            service.projects()  # pylint: disable=no-member
            .locations()
            .flexTemplates()
            .launch(projectId=project_id, body=body, location=location)
        )
        response = request.execute(num_retries=self.num_retries)
        job_id = response['job']['id']

        if on_new_job_id_callback:
            on_new_job_id_callback(job_id)

        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            job_id=job_id,
            location=location,
            poll_sleep=self.poll_sleep,
            num_retries=self.num_retries,
        )
        jobs_controller.wait_for_done()

        return jobs_controller.get_jobs(refresh=True)[0]

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def start_python_dataflow(  # pylint: disable=too-many-arguments
        self,
        job_name: str,
        variables: dict,
        dataflow: str,
        py_options: List[str],
        project_id: str,
        py_interpreter: str = "python3",
        py_requirements: Optional[List[str]] = None,
        py_system_site_packages: bool = False,
        append_job_name: bool = True,
        on_new_job_id_callback: Optional[Callable[[str], None]] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ):
        """
        Starts Dataflow job.

        :param job_name: The name of the job.
        :type job_name: str
        :param variables: Variables passed to the job.
        :type variables: Dict
        :param dataflow: Name of the Dataflow process.
        :type dataflow: str
        :param py_options: Additional options.
        :type py_options: List[str]
        :param project_id: The ID of the GCP project that owns the job.
            If set to ``None`` or missing, the default project_id from the GCP connection is used.
        :type project_id: Optional[str]
        :param py_interpreter: Python version of the beam pipeline.
            If None, this defaults to the python3.
            To track python versions supported by beam and related
            issues check: https://issues.apache.org/jira/browse/BEAM-1251
        :param py_requirements: Additional python package(s) to install.
            If a value is passed to this parameter, a new virtual environment has been created with
            additional packages installed.

            You could also install the apache-beam package if it is not installed on your system or you want
            to use a different version.
        :type py_requirements: List[str]
        :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
            See virtualenv documentation for more information.

            This option is only relevant if the ``py_requirements`` parameter is not None.
        :type py_interpreter: str
        :param append_job_name: True if unique suffix has to be appended to job name.
        :type append_job_name: bool
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :param on_new_job_id_callback: Callback called when the job ID is known.
        :type on_new_job_id_callback: callable
        :param location: Job location.
        :type location: str
        """
        name = self._build_dataflow_job_name(job_name, append_job_name)
        variables['job_name'] = name
        variables['region'] = location

        def label_formatter(labels_dict):
            return ['--labels={}={}'.format(key, value) for key, value in labels_dict.items()]

        if py_requirements is not None:
            if not py_requirements and not py_system_site_packages:
                warning_invalid_environment = textwrap.dedent(
                    """\
                    Invalid method invocation. You have disabled inclusion of system packages and empty list
                    required for installation, so it is not possible to create a valid virtual environment.
                    In the virtual environment, apache-beam package must be installed for your job to be \
                    executed. To fix this problem:
                    * install apache-beam on the system, then set parameter py_system_site_packages to True,
                    * add apache-beam to the list of required packages in parameter py_requirements.
                    """
                )
                raise AirflowException(warning_invalid_environment)

            with TemporaryDirectory(prefix='dataflow-venv') as tmp_dir:
                py_interpreter = prepare_virtualenv(
                    venv_directory=tmp_dir,
                    python_bin=py_interpreter,
                    system_site_packages=py_system_site_packages,
                    requirements=py_requirements,
                )
                command_prefix = [py_interpreter] + py_options + [dataflow]

                self._start_dataflow(
                    variables=variables,
                    name=name,
                    command_prefix=command_prefix,
                    label_formatter=label_formatter,
                    project_id=project_id,
                    on_new_job_id_callback=on_new_job_id_callback,
                    location=location,
                )
        else:
            command_prefix = [py_interpreter] + py_options + [dataflow]

            self._start_dataflow(
                variables=variables,
                name=name,
                command_prefix=command_prefix,
                label_formatter=label_formatter,
                project_id=project_id,
                on_new_job_id_callback=on_new_job_id_callback,
                location=location,
            )

    @staticmethod
    def _build_dataflow_job_name(job_name: str, append_job_name: bool = True) -> str:
        base_job_name = str(job_name).replace('_', '-')

        if not re.match(r"^[a-z]([-a-z0-9]*[a-z0-9])?$", base_job_name):
            raise ValueError(
                'Invalid job_name ({}); the name must consist of'
                'only the characters [-a-z0-9], starting with a '
                'letter and ending with a letter or number '.format(base_job_name)
            )

        if append_job_name:
            safe_job_name = base_job_name + "-" + str(uuid.uuid4())[:8]
        else:
            safe_job_name = base_job_name

        return safe_job_name

    @staticmethod
    def _build_cmd(variables: dict, label_formatter: Callable, project_id: str) -> List[str]:
        command = [
            "--runner=DataflowRunner",
            "--project={}".format(project_id),
        ]
        if variables is None:
            return command

        # The logic of this method should be compatible with Apache Beam:
        # https://github.com/apache/beam/blob/b56740f0e8cd80c2873412847d0b336837429fb9/sdks/python/
        # apache_beam/options/pipeline_options.py#L230-L251
        for attr, value in variables.items():
            if attr == 'labels':
                command += label_formatter(value)
            elif value is None:
                command.append(f"--{attr}")
            elif isinstance(value, bool) and value:
                command.append(f"--{attr}")
            elif isinstance(value, list):
                command.extend([f"--{attr}={v}" for v in value])
            else:
                command.append(f"--{attr}={value}")
        return command

    @_fallback_to_location_from_variables
    @_fallback_to_project_id_from_variables
    @GoogleBaseHook.fallback_to_default_project_id
    def is_job_dataflow_running(
        self,
        name: str,
        project_id: str,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        variables: Optional[dict] = None,
    ) -> bool:
        """
        Helper method to check if jos is still running in dataflow

        :param name: The name of the job.
        :type name: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id: str
        :param location: Job location.
        :type location: str
        :return: True if job is running.
        :rtype: bool
        """
        if variables:
            warnings.warn(
                "The variables parameter has been deprecated. You should pass location using "
                "the location parameter.",
                DeprecationWarning,
                stacklevel=4,
            )
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=name,
            location=location,
            poll_sleep=self.poll_sleep,
        )
        return jobs_controller.is_job_running()

    @GoogleBaseHook.fallback_to_default_project_id
    def cancel_job(
        self,
        project_id: str,
        job_name: Optional[str] = None,
        job_id: Optional[str] = None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
    ) -> None:
        """
        Cancels the job with the specified name prefix or Job ID.

        Parameter ``name`` and ``job_id`` are mutually exclusive.

        :param job_name: Name prefix specifying which jobs are to be canceled.
        :type job_name: str
        :param job_id: Job ID specifying which jobs are to be canceled.
        :type job_id: str
        :param location: Job location.
        :type location: str
        :param project_id: Optional, the Google Cloud project ID in which to start a job.
            If set to None or missing, the default project_id from the Google Cloud connection is used.
        :type project_id:
        """
        jobs_controller = _DataflowJobsController(
            dataflow=self.get_conn(),
            project_number=project_id,
            name=job_name,
            job_id=job_id,
            location=location,
            poll_sleep=self.poll_sleep,
        )
        jobs_controller.cancel()
