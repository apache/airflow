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
    KubernetesCommunicationService, incluster_namespace
from airflow.contrib.kubernetes.kubernetes_request_factory import \
    SimplePodRequestFactory, \
    ReturnValuePodRequestFactory


class PodOperator(PythonOperator):
    """
        Executes a pod and waits for the job to finish.

        :param dag_run_id: The unique run ID that would be attached to the pod as a label
        :type dag_run_id: str
        :param pod_factory: Reference to the function that creates the pod with format:
                            function (OpContext) => Pod
        :type pod_factory: callable
    """
    # template_fields = tuple('dag_run_id')
    ui_color = '#8da7be'

    @apply_defaults
    def __init__(
            self,
            dag_run_id,
            pod_factory,
            kube_request_factory=None,
            *args, **kwargs):
        super(PodOperator, self).__init__(python_callable=lambda _: 1, provide_context=True, *args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        if not callable(pod_factory):
            raise AirflowException('`pod_factory` param must be callable')
        self.dag_run_id = dag_run_id
        self.pod_factory = pod_factory
        self.kwargs = kwargs
        self._kube_request_factory = kube_request_factory or SimplePodRequestFactory

    def execute(self, context):
        pod = self.get_pod_object(context)

        # Customize the pod
        pod.name = self.task_id
        pod.labels['run_id'] = self.dag_run_id
        try:
            pod.namespace = self.dag.default_args.get('namespace', pod.namespace) or incluster_namespace()
        except:
            # Used default namespace
            pass

        # Launch the pod and wait for it to finish
        KubernetesLauncher(pod, self._kube_request_factory).launch()
        result = pod.result
        context['ti'].xcom_push(key='result', value=result)

        custom_return_value = self.on_pod_success(context)
        self.set_custom_return_value(context, custom_return_value)
        return result

    def on_pod_success(self, context):
        """
            Called when pod is executed successfully.
            :return: Returns a custom return value for pod which will
                     be stored in xcom
        """
        pass

    def get_pod_object(self, context):
        """
            Returns a pod object. Overwrite this method to define custom objects
        :param context: The task context
        :return: The pod object
        """
        return self.pod_factory(context)

    def set_custom_return_value(self, context, custom_return_value):
        if custom_return_value:
            context['ti'].xcom_push(key='custom_result', value=custom_return_value)


class ReturnValuePodOperator(PodOperator):
    """
     This pod operators is a normal pod operator with the addition of
     reading custom return value back from kubernetes.
    """
    def __init__(self,
                 result_data_file,
                 kube_com_service_factory=None,
                 *args, **kwargs):
        super(ReturnValuePodOperator, self).__init__(*args, **kwargs)
        kube_com_service_factory = kube_com_service_factory or (
                 lambda: KubernetesCommunicationService.from_dag_default_args(self.dag))
        if not isinstance(kube_com_service_factory(), KubernetesCommunicationService):
            raise AirflowException('`kube_com_service_factory` must be of type KubernetesCommunicationService')
        self._kube_com_service_factory = kube_com_service_factory
        self._result_data_file = result_data_file
        self._kube_request_factory = self._return_value_kube_request  # Overwrite the default request factory

    def on_pod_success(self, context):
        return self._kube_com_service_factory().pod_return_data(self.task_id)

    def _return_value_kube_request(self):
        return ReturnValuePodRequestFactory(self._kube_com_service_factory, self._result_data_file)
