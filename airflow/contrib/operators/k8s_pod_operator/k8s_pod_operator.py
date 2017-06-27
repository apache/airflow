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
from airflow.contrib.kubernetes.pod_launcher import KubernetesLauncher, \
    KubernetesCommunicationService
from airflow.contrib.kubernetes.kubernetes_request_factory import \
    SimplePodRequestFactory, \
    ReturnValuePodRequestFactory
from .op_context import OpContext


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

    @apply_defaults
    def __init__(
        self,
        dag_run_id,
        pod_factory,
        cache_output,
        kube_request_factory=None,
        *args,
        **kwargs
    ):
        super(PodOperator, self).__init__(
            python_callable=lambda _: 1,
            provide_context=True,
            *args,
            **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        if not callable(pod_factory):
            raise AirflowException('`pod_factory` param must be callable')
        self.dag_run_id = dag_run_id
        self.pod_factory = pod_factory
        self._cache_output = cache_output
        self.op_context = OpContext(self.task_id)
        self.kwargs = kwargs
        self._kube_request_factory = kube_request_factory or SimplePodRequestFactory

    def execute(self, context):
        task_instance = context.get('task_instance')
        if task_instance is None:
            raise AirflowException('`task_instance` is empty! This should not happen')
        self.op_context.set_xcom_instance(task_instance)
        pod = self.pod_factory(self.op_context, context)
        # Customize the pod
        pod.name = self.task_id
        pod.labels['run_id'] = self.dag_run_id
        pod.namespace = self.dag.default_args.get('namespace', pod.namespace)

        # Launch the pod and wait for it to finish
        KubernetesLauncher(pod, self._kube_request_factory).launch()
        self.op_context.result = pod.result

        # Cache the output
        custom_return_value = self.on_pod_success(context)
        if custom_return_value:
            self.op_context.custom_return_value = custom_return_value
        return self.op_context.result

    def on_pod_success(self, context):
        """
            Called when pod is executed successfully.
            :return: Returns a custom return value for pod which will
                     be stored in xcom
        """
        pass


class ReturnValuePodOperator(PodOperator):
    """
     This pod operators is a normal pod operator with the addition of
     reading custom return value back from kubernetes.
    """

    def __init__(self,
                 kube_com_service_factory,
                 result_data_file,
                 *args, **kwargs):
        super(ReturnValuePodOperator, self).__init__(*args, **kwargs)
        if not isinstance(kube_com_service_factory(), KubernetesCommunicationService):
            raise AirflowException(
                '`kube_com_service_factory` must be of type '
                'KubernetesCommunicationService')
        self._kube_com_service_factory = kube_com_service_factory
        self._result_data_file = result_data_file
        self._kube_request_factory = self._return_value_kube_request  # Overwrite the
        # default request factory

    def on_pod_success(self, context):
        return_val = self._kube_com_service_factory().pod_return_data(self.task_id)
        self.op_context.result = return_val  # We also overwrite the results
        return return_val

    def _return_value_kube_request(self):
        return ReturnValuePodRequestFactory(self._kube_com_service_factory,
                                            self._result_data_file)
