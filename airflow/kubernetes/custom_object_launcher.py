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

"""Launches Custom object"""
import time
from copy import deepcopy
from datetime import datetime as dt

import tenacity
import yaml
from kubernetes import client, watch
from kubernetes.client import models as k8s
from kubernetes.client.rest import ApiException

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager


def should_retry_start_spark_job(exception: Exception):
    """Check if an Exception indicates a transient error and warrants retrying"""
    if isinstance(exception, ApiException):
        return exception.status == 409
    return False


class SparkResources:
    """spark resources
    :param request_memory: requested memory
    :param request_cpu: requested CPU number
    :param request_ephemeral_storage: requested ephemeral storage
    :param limit_memory: limit for memory usage
    :param limit_cpu: Limit for CPU used
    :param limit_gpu: Limits for GPU used
    :param limit_ephemeral_storage: Limit for ephemeral storage
    """

    def __init__(
        self,
        **kwargs,
    ):
        self.driver_request_cpu = kwargs.get('driver_request_cpu')
        self.driver_limit_cpu = kwargs.get('driver_limit_cpu')
        self.driver_limit_memory = kwargs.get('driver_limit_memory')
        self.executor_request_cpu = kwargs.get('executor_request_cpu')
        self.executor_limit_cpu = kwargs.get('executor_limit_cpu')
        self.executor_limit_memory = kwargs.get('executor_limit_memory')
        self.driver_gpu_name = kwargs.get('driver_gpu_name')
        self.driver_gpu_quantity = kwargs.get('driver_gpu_quantity')
        self.executor_gpu_name = kwargs.get('executor_gpu_name')
        self.executor_gpu_quantity = kwargs.get('executor_gpu_quantity')
        self.convert_resources()

    @property
    def resources(self):
        """
        return resources
        """
        return {'driver': self.driver_resources, 'executor': self.executor_resources}

    @property
    def driver_resources(self):
        """
        return resources to use
        """
        driver = {}
        if self.driver_request_cpu:
            driver['cores'] = self.driver_request_cpu
        if self.driver_limit_cpu:
            driver['coreLimit'] = self.driver_limit_cpu
        if self.driver_limit_memory:
            driver['memory'] = self.driver_limit_memory
        if self.driver_gpu_name and self.driver_gpu_quantity:
            driver['gpu'] = {'name': self.driver_gpu_name, 'quantity': self.driver_gpu_quantity}
        return driver

    @property
    def executor_resources(self):
        """
        return resources to use
        """
        executor = {}
        if self.executor_request_cpu:
            executor['cores'] = self.executor_request_cpu
        if self.executor_limit_cpu:
            executor['coreLimit'] = self.executor_limit_cpu
        if self.executor_limit_memory:
            executor['memory'] = self.executor_limit_memory
        if self.executor_gpu_name and self.executor_gpu_quantity:
            executor['gpu'] = {'name': self.executor_gpu_name, 'quantity': self.executor_gpu_quantity}
        return executor

    def convert_resources(self):
        if isinstance(self.driver_limit_memory, str):
            if 'G' in self.driver_limit_memory or 'Gi' in self.driver_limit_memory:
                self.driver_limit_memory = float(self.driver_limit_memory.rstrip('Gi G')) * 1024
            elif 'm' in self.driver_limit_memory:
                self.driver_limit_memory = float(self.driver_limit_memory.rstrip('m'))
            # Adjusting the memory value as operator adds 40% to the given value
            self.driver_limit_memory = str(int(self.driver_limit_memory / 1.4)) + 'm'

        if isinstance(self.executor_limit_memory, str):
            if 'G' in self.executor_limit_memory or 'Gi' in self.executor_limit_memory:
                self.executor_limit_memory = float(self.executor_limit_memory.rstrip('Gi G')) * 1024
            elif 'm' in self.executor_limit_memory:
                self.executor_limit_memory = float(self.executor_limit_memory.rstrip('m'))
            # Adjusting the memory value as operator adds 40% to the given value
            self.executor_limit_memory = str(int(self.executor_limit_memory / 1.4)) + 'm'

        if self.driver_request_cpu:
            self.driver_request_cpu = int(float(self.driver_request_cpu))
        if self.driver_limit_cpu:
            self.driver_limit_cpu = str(self.driver_limit_cpu)
        if self.executor_request_cpu:
            self.executor_request_cpu = int(float(self.executor_request_cpu))
        if self.executor_limit_cpu:
            self.executor_limit_cpu = str(self.executor_limit_cpu)

        if self.driver_gpu_quantity:
            self.driver_gpu_quantity = int(float(self.driver_gpu_quantity))
        if self.executor_gpu_quantity:
            self.executor_gpu_quantity = int(float(self.executor_gpu_quantity))


class CustomObjectStatus:
    """Status of the PODs"""

    SUBMITTED = 'SUBMITTED'
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    SUCCEEDED = 'SUCCEEDED'
    INITIAL_SPEC = {
        'metadata': {},
        'spec': {
            'dynamicAllocation': {'enabled': False},
            'driver': {},
            'executor': {},
        },
    }


class CustomObjectLauncher(PodManager):
    """Launches PODS"""

    def __init__(
        self,
        kube_client: client.CoreV1Api,
        custom_obj_api: client.CustomObjectsApi,
        namespace: str = 'default',
        api_group: str = 'sparkoperator.k8s.io',
        api_version: str = 'v1beta2',
        plural: str = 'sparkapplications',
        kind: str = 'SparkApplication',
        extract_xcom: bool = False,
        application_file: str = None,
    ):
        """
        Creates the launcher.

        :param kube_client: kubernetes client
        :param extract_xcom: whether we should extract xcom
        """
        super().__init__()
        self.namespace = namespace
        self.api_group = api_group
        self.api_version = api_version
        self.plural = plural
        self.kind = kind
        self._client = kube_client
        self.custom_obj_api = custom_obj_api
        self._watch = watch.Watch()
        self.extract_xcom = extract_xcom
        self.spark_obj_spec = None
        self.pod_spec = None
        self.body = {}
        self.application_file = application_file

    @staticmethod
    def _load_body(file_path):
        try:
            with open(file_path) as data:
                base_body = yaml.safe_load(data)
        except yaml.YAMLError as e:
            raise AirflowException(f"Exception when loading resource definition: {e}\n")
        return base_body

    def set_body(self, **kwargs):
        if self.application_file:
            self.body = self._load_body(self.application_file)
        else:
            self.body = self.get_body(
                f'{self.api_group}/{self.api_version}', self.kind, CustomObjectStatus.INITIAL_SPEC, **kwargs
            )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_random_exponential(),
        reraise=True,
        retry=tenacity.retry_if_exception(should_retry_start_spark_job),
    )
    def start_spark_job(self, startup_timeout: int = 600):
        """
        Launches the pod synchronously and waits for completion.

        :param startup_timeout: Timeout for startup of the pod (if pod is pending for too long, fails task)
        :return:
        """
        try:
            self.log.debug('Spark Job Creation Request Submitted')
            self.spark_obj_spec = self.custom_obj_api.create_namespaced_custom_object(
                group=self.api_group,
                version=self.api_version,
                namespace=self.namespace,
                plural=self.plural,
                body=self.body,
            )
            self.log.debug('Spark Job Creation Response: %s', self.spark_obj_spec)
        except Exception as e:
            self.log.exception('Exception when attempting to create spark job: %s', self.body)
            raise e

        self.pod_spec = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                labels=self.spark_obj_spec['spec']['driver']['labels'],
                name=self.spark_obj_spec['metadata']['name'] + '-driver',
                namespace=self.namespace,
            )
        )
        curr_time = dt.now()
        while self.spark_job_not_running(self.spark_obj_spec):
            self.log.warning(
                "Spark job submitted but not yet started: %s", self.spark_obj_spec['metadata']['name']
            )
            delta = dt.now() - curr_time
            if delta.total_seconds() >= startup_timeout:
                pod_status = self.read_pod(self.pod_spec).status.container_statuses
                raise AirflowException(f"Job took too long to start. pod status: {pod_status}")
            time.sleep(2)

        return self.pod_spec, self.spark_obj_spec

    def spark_job_not_running(self, spark_obj_spec):
        """Tests if spark_obj_spec has not started"""
        spark_job_info = self.custom_obj_api.get_namespaced_custom_object_status(
            group=self.api_group,
            version=self.api_version,
            namespace=self.namespace,
            name=spark_obj_spec['metadata']['name'],
            plural=self.plural,
        )
        driver_state = spark_job_info.get('status', {}).get('applicationState', {}).get('state', 'SUBMITTED')
        if driver_state == CustomObjectStatus.FAILED:
            err = spark_job_info.get('status', {}).get('applicationState', {}).get('errorMessage')
            raise AirflowException(f"Spark Job Failed. Error stack:\n{err}")
        return driver_state == CustomObjectStatus.SUBMITTED

    def delete_spark_job(self, spark_job_name=None):
        """Deletes spark job"""
        spark_job_name = spark_job_name or self.spark_obj_spec.get('metadata', {}).get('name')
        if not spark_job_name:
            self.log.warning("Spark job not found: %s", spark_job_name)
            return
        try:
            v1 = client.CustomObjectsApi()
            v1.delete_namespaced_custom_object(
                group=self.api_group,
                version=self.api_version,
                namespace=self.namespace,
                plural=self.plural,
                name=spark_job_name,
            )
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    @staticmethod
    def get_body(api_version, kind, initial_template, **kwargs):
        body_template = deepcopy(initial_template)
        body_template['apiVersion'] = api_version
        body_template['kind'] = kind
        body_template['metadata']['name'] = kwargs['name']
        body_template['metadata']['namespace'] = kwargs['namespace']
        body_template['spec']['type'] = kwargs['spark_job_type']
        body_template['spec']['pythonVersion'] = kwargs['spark_job_python_version']
        body_template['spec']['mode'] = kwargs['spark_job_mode']
        body_template['spec']['image'] = kwargs['image']
        body_template['spec']['sparkVersion'] = kwargs['spark_version']
        body_template['spec']['successfulRunHistoryLimit'] = kwargs['success_run_history_limit']
        body_template['spec']['restartPolicy'] = kwargs['restart_policy']
        body_template['spec']['imagePullPolicy'] = kwargs['image_pull_policy']
        body_template['spec']['hadoopConf'] = kwargs['hadoop_config'] or {}
        body_template['spec']['mainApplicationFile'] = f'local://{kwargs["code_path"]}'
        body_template['spec']['driver']['serviceAccount'] = kwargs['service_account_name']
        body_template['spec']['driver']['labels'] = kwargs['labels']
        body_template['spec']['executor']['labels'] = kwargs['labels']
        body_template['spec']['imagePullSecrets'] = kwargs['image_pull_secrets']
        if kwargs['dynamic_allocation']:
            body_template['spec']['dynamicAllocation']['enabled'] = kwargs['dynamic_allocation']
            body_template['spec']['dynamicAllocation']['initialExecutors'] = kwargs[
                'dynamic_alloc_initial_executors'
            ]
            body_template['spec']['dynamicAllocation']['minExecutors'] = kwargs['dynamic_alloc_min_executors']
            body_template['spec']['dynamicAllocation']['maxExecutors'] = kwargs['dynamic_alloc_max_executors']

        body_template['spec']['driver'].update(kwargs['driver_resource'])
        body_template['spec']['executor'].update(kwargs['executor_resource'])
        body_template['spec']['executor']['instances'] = int(kwargs['number_workers'])

        body_template['spec']['volumes'] = kwargs['volumes']
        for item in ['driver', 'executor']:
            # Env List
            body_template['spec'][item]['env'] = kwargs['env']
            body_template['spec'][item]['envFrom'] = kwargs['env_from']

            # Volumes
            body_template['spec'][item]['volumeMounts'] = kwargs['volume_mounts']

            # Add affinity
            body_template['spec'][item]['affinity'] = kwargs['affinity']
            body_template['spec'][item]['tolerations'] = kwargs['tolerations']

            # Labels
            labels = {
                'spark_version': kwargs['spark_version'],
                'kubernetes_spark_operator': 'True',
            }
            kwargs['labels'].update(labels)
            body_template['spec'][item]['labels'] = kwargs['labels']

        return body_template
