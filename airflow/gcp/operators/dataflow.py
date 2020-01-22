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
This module contains Google Dataflow operators.
"""

import copy
import os
import re
import tempfile
import uuid
from enum import Enum
from typing import List, Optional

from airflow.gcp.hooks.dataflow import DataflowHook
from airflow.gcp.hooks.gcs import GCSHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.version import version


class CheckJobRunning(Enum):
    """
    Helper enum for choosing what to do if job is already running
    IgnoreJob - do not check if running
    FinishIfRunning - finish current dag run with no action
    WaitForRun - wait for job to finish and then continue with new job
    """
    IgnoreJob = 1
    FinishIfRunning = 2
    WaitForRun = 3


class DataflowCreateJavaJobOperator(BaseOperator):
    """
    Start a Java Cloud DataFlow batch job. The parameters of the operation
    will be passed to the job.

    **Example**: ::

        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date':
                (2016, 8, 1),
            'email': ['alex@vanboxel.be'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=30),
            'dataflow_default_options': {
                'project': 'my-gcp-project',
                'zone': 'us-central1-f',
                'stagingLocation': 'gs://bucket/tmp/dataflow/staging/',
            }
        }

        dag = DAG('test-dag', default_args=default_args)

        task = DataFlowJavaOperator(
            gcp_conn_id='gcp_default',
            task_id='normalize-cal',
            jar='{{var.value.gcp_dataflow_base}}pipeline-ingress-cal-normalize-1.0.jar',
            options={
                'autoscalingAlgorithm': 'BASIC',
                'maxNumWorkers': '50',
                'start': '{{ds}}',
                'partitionType': 'DAY'

            },
            dag=dag)

    .. seealso::
        For more detail on job submission have a look at the reference:
        https://cloud.google.com/dataflow/pipelines/specifying-exec-params

    :param jar: The reference to a self executing DataFlow jar (templated).
    :type jar: str
    :param job_name: The 'jobName' to use when executing the DataFlow job
        (templated). This ends up being set in the pipeline options, so any entry
        with key ``'jobName'`` in ``options`` will be overwritten.
    :type job_name: str
    :param dataflow_default_options: Map of default job options.
    :type dataflow_default_options: dict
    :param options: Map of job specific options.
    :type options: dict
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param poll_sleep: The time in seconds to sleep between polling Google
        Cloud Platform for the dataflow job status while the job is in the
        JOB_STATE_RUNNING state.
    :type poll_sleep: int
    :param job_class: The name of the dataflow job class to be executed, it
        is often not the main class configured in the dataflow jar file.
    :type job_class: str

    :param multiple_jobs: If pipeline creates multiple jobs then monitor all jobs
    :type multiple_jobs: boolean
    :param check_if_running: before running job, validate that a previous run is not in process
    :type check_if_running: CheckJobRunning(IgnoreJob = do not check if running, FinishIfRunning=
        if job is running finish with nothing, WaitForRun= wait until job finished and the run job)
        ``jar``, ``options``, and ``job_name`` are templated so you can use variables in them.

    Note that both
    ``dataflow_default_options`` and ``options`` will be merged to specify pipeline
    execution parameter, and ``dataflow_default_options`` is expected to save
    high-level options, for instances, project and zone information, which
    apply to all dataflow operators in the DAG.

    It's a good practice to define dataflow_* parameters in the default_args of the dag
    like the project, zone and staging location.

    .. code-block:: python

       default_args = {
           'dataflow_default_options': {
               'project': 'my-gcp-project',
               'zone': 'europe-west1-d',
               'stagingLocation': 'gs://my-staging-bucket/staging/'
           }
       }

    You need to pass the path to your dataflow as a file reference with the ``jar``
    parameter, the jar needs to be a self executing jar (see documentation here:
    https://beam.apache.org/documentation/runners/dataflow/#self-executing-jar).
    Use ``options`` to pass on options to your job.

    .. code-block:: python

       t1 = DataFlowJavaOperator(
           task_id='dataflow_example',
           jar='{{var.value.gcp_dataflow_base}}pipeline/build/libs/pipeline-example-1.0.jar',
           options={
               'autoscalingAlgorithm': 'BASIC',
               'maxNumWorkers': '50',
               'start': '{{ds}}',
               'partitionType': 'DAY',
               'labels': {'foo' : 'bar'}
           },
           gcp_conn_id='airflow-conn-id',
           dag=my-dag)

    """
    template_fields = ['options', 'jar', 'job_name']
    ui_color = '#0273d4'

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
            self,
            jar: str,
            job_name: str = '{{task.task_id}}',
            dataflow_default_options: Optional[dict] = None,
            options: Optional[dict] = None,
            gcp_conn_id: str = 'google_cloud_default',
            delegate_to: Optional[str] = None,
            poll_sleep: int = 10,
            job_class: Optional[str] = None,
            check_if_running: CheckJobRunning = CheckJobRunning.WaitForRun,
            multiple_jobs: Optional[bool] = None,
            *args,
            **kwargs) -> None:
        super().__init__(*args, **kwargs)

        dataflow_default_options = dataflow_default_options or {}
        options = options or {}
        options.setdefault('labels', {}).update(
            {'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')})
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.jar = jar
        self.multiple_jobs = multiple_jobs
        self.job_name = job_name
        self.dataflow_default_options = dataflow_default_options
        self.options = options
        self.poll_sleep = poll_sleep
        self.job_class = job_class
        self.check_if_running = check_if_running

    def execute(self, context):
        hook = DataflowHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to,
                            poll_sleep=self.poll_sleep)
        dataflow_options = copy.copy(self.dataflow_default_options)
        dataflow_options.update(self.options)
        is_running = False
        if self.check_if_running != CheckJobRunning.IgnoreJob:
            is_running = hook.is_job_dataflow_running(
                name=self.job_name,
                variables=dataflow_options
            )
            while is_running and self.check_if_running == CheckJobRunning.WaitForRun:
                is_running = hook.is_job_dataflow_running(name=self.job_name, variables=dataflow_options)

        if not is_running:
            bucket_helper = GoogleCloudBucketHelper(
                self.gcp_conn_id, self.delegate_to)
            self.jar = bucket_helper.google_cloud_to_local(self.jar)
            hook.start_java_dataflow(
                job_name=self.job_name,
                variables=dataflow_options,
                jar=self.jar,
                job_class=self.job_class,
                append_job_name=True,
                multiple_jobs=self.multiple_jobs
            )


class DataflowTemplatedJobStartOperator(BaseOperator):
    """
    Start a Templated Cloud DataFlow batch job. The parameters of the operation
    will be passed to the job.

    :param template: The reference to the DataFlow template.
    :type template: str
    :param job_name: The 'jobName' to use when executing the DataFlow template
        (templated).
    :param dataflow_default_options: Map of default job environment options.
    :type dataflow_default_options: dict
    :param parameters: Map of job specific parameters for the template.
    :type parameters: dict
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param poll_sleep: The time in seconds to sleep between polling Google
        Cloud Platform for the dataflow job status while the job is in the
        JOB_STATE_RUNNING state.
    :type poll_sleep: int

    It's a good practice to define dataflow_* parameters in the default_args of the dag
    like the project, zone and staging location.

    .. seealso::
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/LaunchTemplateParameters
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment

    .. code-block:: python

       default_args = {
           'dataflow_default_options': {
               'project': 'my-gcp-project',
               'region': 'europe-west1',
               'zone': 'europe-west1-d',
               'tempLocation': 'gs://my-staging-bucket/staging/',
               }
           }
       }

    You need to pass the path to your dataflow template as a file reference with the
    ``template`` parameter. Use ``parameters`` to pass on parameters to your job.
    Use ``environment`` to pass on runtime environment variables to your job.

    .. code-block:: python

       t1 = DataflowTemplateOperator(
           task_id='dataflow_example',
           template='{{var.value.gcp_dataflow_base}}',
           parameters={
               'inputFile': "gs://bucket/input/my_input.txt",
               'outputFile': "gs://bucket/output/my_output.txt"
           },
           gcp_conn_id='airflow-conn-id',
           dag=my-dag)

    ``template``, ``dataflow_default_options``, ``parameters``, and ``job_name`` are
    templated so you can use variables in them.

    Note that ``dataflow_default_options`` is expected to save high-level options
    for project information, which apply to all dataflow operators in the DAG.

        .. seealso::
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3
            /LaunchTemplateParameters
            https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
            For more detail on job template execution have a look at the reference:
            https://cloud.google.com/dataflow/docs/templates/executing-templates
    """
    template_fields = ['parameters', 'dataflow_default_options', 'template', 'job_name']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            template: str,
            job_name: str = '{{task.task_id}}',
            dataflow_default_options: Optional[dict] = None,
            parameters: Optional[dict] = None,
            gcp_conn_id: str = 'google_cloud_default',
            delegate_to: Optional[str] = None,
            poll_sleep: int = 10,
            *args,
            **kwargs) -> None:
        super().__init__(*args, **kwargs)

        dataflow_default_options = dataflow_default_options or {}
        parameters = parameters or {}

        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.dataflow_default_options = dataflow_default_options
        self.poll_sleep = poll_sleep
        self.template = template
        self.job_name = job_name
        self.parameters = parameters

    def execute(self, context):
        hook = DataflowHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to,
                            poll_sleep=self.poll_sleep)

        hook.start_template_dataflow(
            job_name=self.job_name,
            variables=self.dataflow_default_options,
            parameters=self.parameters,
            dataflow_template=self.template
        )


class DataflowCreatePythonJobOperator(BaseOperator):
    """
    Launching Cloud Dataflow jobs written in python. Note that both
    dataflow_default_options and options will be merged to specify pipeline
    execution parameter, and dataflow_default_options is expected to save
    high-level options, for instances, project and zone information, which
    apply to all dataflow operators in the DAG.

    .. seealso::
        For more detail on job submission have a look at the reference:
        https://cloud.google.com/dataflow/pipelines/specifying-exec-params

    :param py_file: Reference to the python dataflow pipeline file.py, e.g.,
        /some/local/file/path/to/your/python/pipeline/file. (templated)
    :type py_file: str
    :param job_name: The 'job_name' to use when executing the DataFlow job
        (templated). This ends up being set in the pipeline options, so any entry
        with key ``'jobName'`` or ``'job_name'`` in ``options`` will be overwritten.
    :type job_name: str
    :param py_options: Additional python options, e.g., ["-m", "-v"].
    :type py_options: list[str]
    :param dataflow_default_options: Map of default job options.
    :type dataflow_default_options: dict
    :param options: Map of job specific options.
    :type options: dict
    :param py_interpreter: Python version of the beam pipeline.
        If None, this defaults to the python3.
        To track python versions supported by beam and related
        issues check: https://issues.apache.org/jira/browse/BEAM-1251
    :type py_interpreter: str
    :param py_requirements: Additional python package(s) to install.
        If a value is passed to this parameter, a new virtual environment has been created with
        additional packages installed.

        You could also install the apache_beam package if it is not installed on your system or you want
        to use a different version.
    :type py_requirements: List[str]
    :param py_system_site_packages: Whether to include system_site_packages in your virtualenv.
        See virtualenv documentation for more information.

        This option is only relevant if the ``py_requirements`` parameter is passed.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud
        Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide  delegation enabled.
    :type delegate_to: str
    :param poll_sleep: The time in seconds to sleep between polling Google
        Cloud Platform for the dataflow job status while the job is in the
        JOB_STATE_RUNNING state.
    :type poll_sleep: int
    """
    template_fields = ['options', 'dataflow_default_options', 'job_name', 'py_file']

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
            self,
            py_file: str,
            job_name: str = '{{task.task_id}}',
            dataflow_default_options: Optional[dict] = None,
            options: Optional[dict] = None,
            py_interpreter: str = "python3",
            py_options: Optional[List[str]] = None,
            py_requirements: Optional[List[str]] = None,
            py_system_site_packages: bool = False,
            gcp_conn_id: str = 'google_cloud_default',
            delegate_to: Optional[str] = None,
            poll_sleep: int = 10,
            *args,
            **kwargs) -> None:

        super().__init__(*args, **kwargs)

        self.py_file = py_file
        self.job_name = job_name
        self.py_options = py_options or []
        self.dataflow_default_options = dataflow_default_options or {}
        self.options = options or {}
        self.options.setdefault('labels', {}).update(
            {'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')})
        self.py_interpreter = py_interpreter
        self.py_requirements = py_requirements or []
        self.py_system_site_packages = py_system_site_packages
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.poll_sleep = poll_sleep

    def execute(self, context):
        """Execute the python dataflow job."""
        bucket_helper = GoogleCloudBucketHelper(
            self.gcp_conn_id, self.delegate_to)
        self.py_file = bucket_helper.google_cloud_to_local(self.py_file)
        hook = DataflowHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to,
                            poll_sleep=self.poll_sleep)
        dataflow_options = self.dataflow_default_options.copy()
        dataflow_options.update(self.options)
        # Convert argument names from lowerCamelCase to snake case.
        camel_to_snake = lambda name: re.sub(
            r'[A-Z]', lambda x: '_' + x.group(0).lower(), name)
        formatted_options = {camel_to_snake(key): dataflow_options[key]
                             for key in dataflow_options}
        hook.start_python_dataflow(
            job_name=self.job_name,
            variables=formatted_options,
            dataflow=self.py_file,
            py_options=self.py_options,
            py_interpreter=self.py_interpreter,
            py_requirements=self.py_requirements,
            py_system_site_packages=self.py_system_site_packages,
        )


class GoogleCloudBucketHelper:
    """GoogleCloudStorageHook helper class to download GCS object."""
    GCS_PREFIX_LENGTH = 5

    def __init__(self,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None) -> None:
        self._gcs_hook = GCSHook(gcp_conn_id, delegate_to)

    def google_cloud_to_local(self, file_name: str) -> str:
        """
        Checks whether the file specified by file_name is stored in Google Cloud
        Storage (GCS), if so, downloads the file and saves it locally. The full
        path of the saved file will be returned. Otherwise the local file_name
        will be returned immediately.

        :param file_name: The full path of input file.
        :type file_name: str
        :return: The full path of local file.
        :rtype: str
        """
        if not file_name.startswith('gs://'):
            return file_name

        # Extracts bucket_id and object_id by first removing 'gs://' prefix and
        # then split the remaining by path delimiter '/'.
        path_components = file_name[self.GCS_PREFIX_LENGTH:].split('/')
        if len(path_components) < 2:
            raise Exception(
                'Invalid Google Cloud Storage (GCS) object path: {}'
                .format(file_name))

        bucket_id = path_components[0]
        object_id = '/'.join(path_components[1:])
        local_file = os.path.join(
            tempfile.gettempdir(),
            'dataflow{}-{}'.format(str(uuid.uuid4())[:8], path_components[-1])
        )
        self._gcs_hook.download(bucket_id, object_id, local_file)

        if os.stat(local_file).st_size > 0:
            return local_file
        raise Exception(
            'Failed to download Google Cloud Storage (GCS) object: {}'
            .format(file_name))
