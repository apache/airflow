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

import logging

from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.kubernetes.pod_launcher import PodLauncher
from airflow.contrib.kubernetes.pod import Pod
from airflow.utils.state import State


class PodOperator(PythonOperator):
    """
        Executes a pod and waits for the job to finish.
        :param dag_run_id: The unique run ID that would be attached to the pod as a label
        :type dag_run_id: str
        :param pod_factory: Reference to the function that creates the pod with format:
                            function (OpContext) => Pod
        :type pod_factory: callable
        :param cache_output: If set to true, the output of the pod would be saved in a
                            cache object using md5 hash of all the pod parameters
                            and in case of success, the cached results will be returned
                            on consecutive calls. Only use this
    """
    # template_fields = tuple('dag_run_id')
    ui_color = '#8da7be'

    def blank_func(self, context):
        return None

    @apply_defaults
    def __init__(
        self,
        dag_run_id,
        pod,
        on_pod_success_func = blank_func,
        *args,
        **kwargs
    ):
        # type: (str, Pod) -> PodOperator
        super(PodOperator, self).__init__(
            python_callable=lambda _:1,
            provide_context=True,
            *args,
            **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pod = pod
        self.dag_run_id = dag_run_id
        self.pod_launcher = PodLauncher()
        self.kwargs = kwargs
        self._on_pod_success_func = on_pod_success_func

    def execute(self, context):
        task_instance = context.get('task_instance')
        if task_instance is None:
            raise AirflowException('`task_instance` is empty! This should not happen')

        pod = self.pod

        # Customize the pod
        pod.name = self.task_id
        pod.labels['run_id'] = self.dag_run_id
        pod.namespace = self.dag.default_args.get('namespace', pod.namespace)

        pod_result = self.pod_launcher.run_pod(pod)

        if pod_result == State.FAILED:
            raise AirflowException("Pod returned a failed status")

        # Launch the pod and wait for it to finish
        self.op_context.result = pod.result
        if pod_result == State.FAILED:
            raise AirflowException("Pod failed")

        # Cache the output
        custom_return_value = self.on_pod_success(context)
        if custom_return_value:
            return custom_return_value

    def on_pod_success(self, context):
        """
            Called when pod is executed successfully.
            
            If you want to access return values for XCOM, place values
            in accessible file system or DB and override this function.
            
            :return: Returns a custom return value for pod which will
                     be stored in xcom
                     
        """
        return self._on_pod_success_func(context=context)
