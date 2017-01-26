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

import copy
import re

from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook
from airflow.models import BaseOperator
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
            'partitionType': 'DAY'
        },
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
        """
        super(DataFlowJavaOperator, self).__init__(*args, **kwargs)

        dataflow_default_options = dataflow_default_options or {}
        options = options or {}

        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.jar = jar
        self.dataflow_default_options = dataflow_default_options
        self.options = options

    def execute(self, context):
        hook = DataFlowHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)

        dataflow_options = copy.copy(self.dataflow_default_options)
        dataflow_options.update(self.options)

        hook.start_java_dataflow(self.task_id, dataflow_options, self.jar)


class DataFlowPythonOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            py_file,
            py_options=None,
            dataflow_default_options=None,
            options=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
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

        :param py_file: Reference to the python dataflow pipleline file, e.g.,
            /some/local/file/path/to/your/python/pipeline/file.py.
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
        """
        super(DataFlowPythonOperator, self).__init__(*args, **kwargs)

        self.py_file = py_file
        self.py_options = py_options or []
        self.dataflow_default_options = dataflow_default_options or {}
        self.options = options or {}
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        """Execute the python dataflow job."""
        hook = DataFlowHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)
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
