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

import hashlib
import logging
import os

from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.kubernetes.pod_launcher import KubernetesLauncher, KubernetesCommunicationService
from airflow.contrib.kubernetes.pod_request import SimplePodRequestFactory, ReturnValuePodRequestFactory


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
            *args, **kwargs):
        super(PodOperator, self).__init__(python_callable=lambda _: 1, provide_context=True, *args, **kwargs)
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
        cache_key = [pod.image] + pod.envs.keys() + pod.envs.values() + pod.cmds
        logging.info('Cache key:' + str(cache_key))
        if self._cache_output:
            cache = Cache.poll_cache(cache_key)
            if cache is not None:
                self.logger.info('Pod %s - using cache output from job: (%s, %s)' %
                                 (self.task_id, cache.job_id, cache.job_output))
                self.op_context.result = cache.job_output
                return cache.job_output
        # Customize the pod
        pod.name = self.task_id
        pod.labels['run_id'] = self.dag_run_id
        pod.namespace = self.dag.default_args.get('namespace', pod.namespace)

        # Launch the pod and wait for it to finish
        KubernetesLauncher(pod, self._kube_request_factory).launch()
        self.op_context.result = pod.result

        # Cache the output
        if self._cache_output:
            Cache.set_cache(self.task_id, self.op_context.result, cache_key)
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
            raise AirflowException('`kube_com_service_factory` must be of type KubernetesCommunicationService')
        self._kube_com_service_factory = kube_com_service_factory
        self._result_data_file = result_data_file
        self._kube_request_factory = self._return_value_kube_request  # Overwrite the default request factory

    def on_pod_success(self, context):
        return_val = self._kube_com_service_factory().pod_return_data(self.task_id)
        self.op_context.result = return_val  # We also overwrite the results
        return return_val

    def _return_value_kube_request(self):
        return ReturnValuePodRequestFactory(self._kube_com_service_factory, self._result_data_file)




class OpContext(object):
    """
        Data model for operation context of a pod operator with hyper parameters. OpContext is
        able to communicate the context between PodOperators by encapsulating XCom communication
        Note: do not directly modify the upstreams
        Also note: xcom_instance MUST be set before any attribute of this class can be read.
        :param: task_id             The task ID
    """
    _supported_attributes = {'hyper_parameters', 'custom_return_value'}

    def __init__(self, task_id):
        self.task_id = task_id
        self._upstream = []
        self._result = '__not_set__'
        self._data = {}
        self._xcom_instance = None
        self._parent = None

    def __str__(self):
        return 'upstream: [' + \
               ','.join([u.task_id for u in self._upstream]) + ']\n' + \
               'params:' + ','.join([k + '=' + str(self._data[k]) for k in self._data.keys()])

    def __setattr__(self, name, value):
        if name in self._data:
            raise AirflowException('`{}` is already set'.format(name))
        if name not in self._supported_attributes:
            logging.warn('`{}` is not in the supported attribute list for OpContext'.format(name))
        self.get_xcom_instance().xcom_push(key=name, value=value)
        self._data[name] = value

    def __getattr__(self, item):
        if item not in self._supported_attributes:
            logging.warn('`{}` is not in the supported attribute list for OpContext'.format(item))
        if item not in self._data:
            self._data[item] = self.get_xcom_instance().xcom_pull(key=item, task_ids=self.task_id)
        return self._data[item]

    @property
    def result(self):
        if self._result == '__not_set__':
            self._result = self.get_xcom_instance().xcom_pull(task_ids=self.task_id)
        return self._result

    @result.setter
    def result(self, value):
        if self._result != '__not_set__':
            raise AirflowException('`result` is already set')
        self._result = value

    @property
    def upstream(self):
        return self._upstream

    def append_upstream(self, upstream_op_contexes):
        """
        Appends a list of op_contexts to the upstream. It will create new instances and set the task_id.
        All the upstream op_contextes will share the same xcom_instance with this op_context
        :param upstream_op_contexes: List of upstream op_contextes
        """
        for up in upstream_op_contexes:
            op_context = OpContext(up.tak_id)
            op_context._parent = self
            self._upstream.append(op_context)

    def set_xcom_instance(self, xcom_instance):
        """
        Sets the xcom_instance for this op_context and upstreams
        :param xcom_instance: The Airflow TaskInstance for communication through XCom
        :type xcom_instance: airflow.models.TaskInstance
        """
        self._xcom_instance = xcom_instance

    def get_xcom_instance(self):
        if self._xcom_instance is None and self._parent is None:
            raise AirflowException('Trying to access attribtues from OpContext before setting the xcom_instance')
        return self._xcom_instance or self._parent.get_xcom_instance()

"""
  Cache class to cache task results to avoid running the same expensive job if params are not changed
"""


class Cache:
    logger = logging.getLogger('Cache')

    def __init__(self, hash_data=None):
        if hash is not None:
            self.set_hash(hash_data)
        self.data_dir = '/tmp/k_job_cache'
        self.job_id = ''
        self.job_output = ''
        self.hash = None
        if not os.path.isdir(self.data_dir):
            os.system('mkdir -p ' + self.data_dir)

    def set_hash(self, params):
        big_str = '|'.join([str(p) for p in params])
        self.hash = hashlib.md5(big_str).hexdigest()

    def hit(self):
        f_name = os.path.join(self.data_dir, self.hash)
        if os.path.exists(f_name):
            with open(f_name, 'rt') as f:
                lines = f.read().splitlines()
                [self.job_id, self.job_output] = lines
                return True
        return False

    def save(self):
        f_name = os.path.join(self.data_dir, self.hash)
        with open(f_name, 'wt') as f:
            f.write(self.job_id)
            f.write('\n')
            f.write(self.job_output)
        self.logger.info('Write cache `{}`'.format(self.hash))

    @staticmethod
    def poll_cache(cache_key_parts):
        # Checks the local cache. If something exists, returns the cache item
        cache = Cache(cache_key_parts)
        if cache.hit():
            Cache.logger.info('Found cache: {} - {} - {}'.format(cache.hash, cache.job_id, cache.job_output))
            return cache
        Cache.logger.info('Not found cache: {}'.format(cache.hash))
        return None

    @staticmethod
    def set_cache(job_id, job_output, cache_key_parts):
        cache = Cache(cache_key_parts)
        cache.job_id = job_id
        cache.job_output = job_output
        cache.save()

    @staticmethod
    def _reset_cache():
        os.system('rm -rf ' + Cache().data_dir)
