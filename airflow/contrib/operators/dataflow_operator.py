# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import uuid
import copy

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook
from airflow.models import BaseOperator
from airflow.version import version
from airflow.utils.decorators import apply_defaults


class DataFlowJavaOperator(BaseOperator):
    """
    Start a Java Cloud DataFlow batch job. The parameters of the operation
    will be passed to the job.

    It's a good practice to define dataflow_* parameters in the default_args of the dag
    like the project, zone and staging location.

    ```
    default_args = {
        'dataflow_default_options': {
            'project': 'my-gcp-project',
            'zone': 'europe-west1-d',
            'stagingLocation': 'gs://my-staging-bucket/staging/'
        }
    }
    ```

    You need to pass the path to your dataflow as a file reference with the ``jar``
    parameter, the jar needs to be a self executing jar. Use ``options`` to pass on
    options to your job.

    ```
    t1 = DataFlowOperation(
        task_id='datapflow_example',
        jar='{{var.value.gcp_dataflow_base}}pipeline/build/libs/pipeline-example-1.0.jar',
        options={
            'autoscalingAlgorithm': 'BASIC',
            'maxNumWorkers': '50',
            'start': '{{ds}}',
            'partitionType': 'DAY',
            'labels': {'foo' : 'bar'}
        },
        gcp_conn_id='gcp-airflow-service-account',
        dag=my-dag)
    ```

    Both ``jar`` and ``options`` are templated so you can use variables in them.
    """
    template_fields = ['options', 'jar']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            jar,
            dataflow_default_options=None,
            options=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            poll_sleep=10,
            job_class=None,
            *args,
            **kwargs):
        """
        Create a new DataFlowJavaOperator. Note that both
        dataflow_default_options and options will be merged to specify pipeline
        execution parameter, and dataflow_default_options is expected to save
        high-level options, for instances, project and zone information, which
        apply to all dataflow operators in the DAG.

        For more detail on job submission have a look at the reference:

        https://cloud.google.com/dataflow/pipelines/specifying-exec-params

        :param jar: The reference to a self executing DataFlow jar.
        :type jar: string
        :param dataflow_default_options: Map of default job options.
        :type dataflow_default_options: dict
        :param options: Map of job specific options.
        :type options: dict
        :param gcp_conn_id: The connection ID to use connecting to Google Cloud
        Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide delegation enabled.
        :type delegate_to: string
        :param poll_sleep: The time in seconds to sleep between polling Google
            Cloud Platform for the dataflow job status while the job is in the
            JOB_STATE_RUNNING state.
        :type poll_sleep: int
        :param job_class: The name of the dataflow job class to be executued, it
        is often not the main class configured in the dataflow jar file.
        :type job_class: string
        """
        super(DataFlowJavaOperator, self).__init__(*args, **kwargs)

        dataflow_default_options = dataflow_default_options or {}
        options = options or {}
        options.setdefault('labels', {}).update(
            {'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')})
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.jar = jar
        self.dataflow_default_options = dataflow_default_options
        self.options = options
        self.poll_sleep = poll_sleep
        self.job_class = job_class

    def execute(self, context):
        bucket_helper = GoogleCloudBucketHelper(
            self.gcp_conn_id, self.delegate_to)
        self.jar = bucket_helper.google_cloud_to_local(self.jar)
        hook = DataFlowHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to,
                            poll_sleep=self.poll_sleep)

        dataflow_options = copy.copy(self.dataflow_default_options)
        dataflow_options.update(self.options)

        hook.start_java_dataflow(self.task_id, dataflow_options,
                                 self.jar, self.job_class)


class DataflowTemplateOperator(BaseOperator):
    """
    Start a Templated Cloud DataFlow batch job. The parameters of the operation
    will be passed to the job.
    It's a good practice to define dataflow_* parameters in the default_args of the dag
    like the project, zone and staging location.
    https://cloud.google.com/dataflow/docs/reference/rest/v1b3/LaunchTemplateParameters
    https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
    ```
    default_args = {
        'dataflow_default_options': {
            'project': 'my-gcp-project'
            'zone': 'europe-west1-d',
            'tempLocation': 'gs://my-staging-bucket/staging/'
            }
        }
    }
    ```
    You need to pass the path to your dataflow template as a file reference with the
    ``template`` parameter. Use ``parameters`` to pass on parameters to your job.
    Use ``environment`` to pass on runtime environment variables to your job.
    ```
    t1 = DataflowTemplateOperator(
        task_id='datapflow_example',
        template='{{var.value.gcp_dataflow_base}}',
        parameters={
            'inputFile': "gs://bucket/input/my_input.txt",
            'outputFile': "gs://bucket/output/my_output.txt"
        },
        gcp_conn_id='gcp-airflow-service-account',
        dag=my-dag)
    ```
    ``template`` ``dataflow_default_options`` and ``parameters`` are templated so you can
    use variables in them.
    """
    template_fields = ['parameters', 'dataflow_default_options', 'template']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            template,
            dataflow_default_options=None,
            parameters=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            poll_sleep=10,
            *args,
            **kwargs):
        """
        Create a new DataflowTemplateOperator. Note that
        dataflow_default_options is expected to save high-level options
        for project information, which apply to all dataflow operators in the DAG.
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3
        /LaunchTemplateParameters
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
        For more detail on job template execution have a look at the reference:
        https://cloud.google.com/dataflow/docs/templates/executing-templates
        :param template: The reference to the DataFlow template.
        :type template: string
        :param dataflow_default_options: Map of default job environment options.
        :type dataflow_default_options: dict
        :param parameters: Map of job specific parameters for the template.
        :type parameters: dict
        :param gcp_conn_id: The connection ID to use connecting to Google Cloud
        Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide delegation enabled.
        :type delegate_to: string
        :param poll_sleep: The time in seconds to sleep between polling Google
            Cloud Platform for the dataflow job status while the job is in the
            JOB_STATE_RUNNING state.
        :type poll_sleep: int
        """
        super(DataflowTemplateOperator, self).__init__(*args, **kwargs)

        dataflow_default_options = dataflow_default_options or {}
        parameters = parameters or {}

        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.dataflow_default_options = dataflow_default_options
        self.poll_sleep = poll_sleep
        self.template = template
        self.parameters = parameters

    def execute(self, context):
        hook = DataFlowHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to,
                            poll_sleep=self.poll_sleep)

        hook.start_template_dataflow(self.task_id, self.dataflow_default_options,
                                     self.parameters, self.template)


class DataFlowPythonOperator(BaseOperator):

    template_fields = ['options', 'dataflow_default_options']

    @apply_defaults
    def __init__(
            self,
            py_file,
            py_options=None,
            dataflow_default_options=None,
            options=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            poll_sleep=10,
            *args,
            **kwargs):
        """
        Create a new DataFlowPythonOperator. Note that both
        dataflow_default_options and options will be merged to specify pipeline
        execution parameter, and dataflow_default_options is expected to save
        high-level options, for instances, project and zone information, which
        apply to all dataflow operators in the DAG.

        For more detail on job submission have a look at the reference:

        https://cloud.google.com/dataflow/pipelines/specifying-exec-params

        :param py_file: Reference to the python dataflow pipleline file.py, e.g.,
            /some/local/file/path/to/your/python/pipeline/file.
        :type py_file: string
        :param py_options: Additional python options.
        :type pyt_options: list of strings, e.g., ["-m", "-v"].
        :param dataflow_default_options: Map of default job options.
        :type dataflow_default_options: dict
        :param options: Map of job specific options.
        :type options: dict
        :param gcp_conn_id: The connection ID to use connecting to Google Cloud
            Platform.
        :type gcp_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have
            domain-wide  delegation enabled.
        :type delegate_to: string
        :param poll_sleep: The time in seconds to sleep between polling Google
            Cloud Platform for the dataflow job status while the job is in the
            JOB_STATE_RUNNING state.
        :type poll_sleep: int
        """
        super(DataFlowPythonOperator, self).__init__(*args, **kwargs)

        self.py_file = py_file
        self.py_options = py_options or []
        self.dataflow_default_options = dataflow_default_options or {}
        self.options = options or {}
        self.options.setdefault('labels', {}).update(
            {'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')})
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.poll_sleep = poll_sleep

    def execute(self, context):
        """Execute the python dataflow job."""
        bucket_helper = GoogleCloudBucketHelper(
            self.gcp_conn_id, self.delegate_to)
        self.py_file = bucket_helper.google_cloud_to_local(self.py_file)
        hook = DataFlowHook(gcp_conn_id=self.gcp_conn_id,
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
            self.task_id, formatted_options,
            self.py_file, self.py_options)


class GoogleCloudBucketHelper():
    """GoogleCloudStorageHook helper class to download GCS object."""
    GCS_PREFIX_LENGTH = 5

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None):
        self._gcs_hook = GoogleCloudStorageHook(gcp_conn_id, delegate_to)

    def google_cloud_to_local(self, file_name):
        """
        Checks whether the file specified by file_name is stored in Google Cloud
        Storage (GCS), if so, downloads the file and saves it locally. The full
        path of the saved file will be returned. Otherwise the local file_name
        will be returned immediately.

        :param file_name: The full path of input file.
        :type file_name: string
        :return: The full path of local file.
        :type: string
        """
        if not file_name.startswith('gs://'):
            return file_name

        # Extracts bucket_id and object_id by first removing 'gs://' prefix and
        # then split the remaining by path delimiter '/'.
        path_components = file_name[self.GCS_PREFIX_LENGTH:].split('/')
        if path_components < 2:
            raise Exception(
                'Invalid Google Cloud Storage (GCS) object path: {}.'
                .format(file_name))

        bucket_id = path_components[0]
        object_id = '/'.join(path_components[1:])
        local_file = '/tmp/dataflow{}-{}'.format(str(uuid.uuid1())[:8],
                                                 path_components[-1])
        file_size = self._gcs_hook.download(bucket_id, object_id, local_file)

        if file_size > 0:
            return local_file
        raise Exception(
            'Failed to download Google Cloud Storage GCS object: {}'
            .format(file_name))
