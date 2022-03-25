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
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from airflow import AirflowException
from airflow.kubernetes import kube_client, pod_generator
from airflow.kubernetes.pod_generator import PodGenerator, MAX_LABEL_LEN
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_affinity,
    convert_configmap,
    convert_env_vars,
    convert_image_pull_secrets,
    convert_pod_runtime_info_env,
    convert_port,
    convert_resources,
    convert_toleration,
    convert_volume,
    convert_volume_mount, convert_configmap_to_volume, convert_secret,
)
from airflow.utils.state import State
from kubernetes import client
from kubernetes.client import models as k8s


from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils.decorators import apply_defaults
from airflow.kubernetes.custom_object_launcher import CustomObjectLauncher


class SparkKubernetesOperator(BaseOperator):
    """
    Creates sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

    :param application_file: filepath to kubernetes custom_resource_definition of sparkApplication
    :type application_file:  str
    :param namespace: kubernetes namespace to put sparkApplication
    :type namespace: str
    :param kubernetes_conn_id: the connection to Kubernetes cluster
    :type kubernetes_conn_id: str
    :param hadoop_config: hadoop base config
        example: AWS s3 config
        {'fs.s3n.impl': 'org.apache.hadoop.fs.s3native.NativeS3FileSystem',
         'fs.s3a.enable-server-side-encryption': 'true',
         'fs.s3a.server-side-encryption-algorithm': 'AES256'}
    :type hadoop_config: dict

    """

    template_fields = ['application_file', 'namespace']
    template_ext = ('yaml', 'yml', 'json')
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(
        self,
        *,
        image: Optional[str] = None,
        code_path: Optional[str] = None,
        namespace: Optional[str] = None,
        cluster_context: Optional[str] = None,
        config_file: Optional[str] = None,
        resources: dict = None,
        labels: dict = None,
        env_vars: Optional[Union[List[k8s.V1EnvVar], Dict]] = None,
        env_from: Optional[List[k8s.V1EnvFromSource]] = None,
        affinity: Optional[k8s.V1Affinity] = None,
        tolerations: Optional[List[k8s.V1Toleration]] = None,
        volume_mounts: Optional[List[k8s.V1VolumeMount]] = None,
        volumes: Optional[List[k8s.V1Volume]] = None,
        config_map_mounts: Optional[Dict[str, str]] = None,
        from_env_config_map: Optional[List[str]] = None,
        from_env_secret: Optional[List[str]] = None,
        hadoop_config: Optional[dict] = None,
        application_file: Optional[str] = None,
        get_logs: bool = True,
        number_workers: int = 1,
        do_xcom_push: bool = False,
        restart_policy: [dict] = None,
        spark_version: str = '3.0.0',
        success_run_history_limit: int = 1,
        is_delete_operator_pod: bool = False,
        dynamic_allocation: bool = False,
        dynamic_alloc_max_executors: int = 5,
        dynamic_alloc_initial_executors: int = 1,
        dynamic_alloc_min_executors: int = 1,
        api_group: str = 'sparkoperator.k8s.io',
        api_version: str = 'v1beta2',
        api_kind: str = 'SparkApplication',
        api_plural: str = 'sparkapplications',
        image_pull_policy: str = 'Always',
        service_account_name: str = 'default',
        spark_job_mode: str = 'cluster',
        spark_job_python_version: str = '3',
        spark_job_type: str = 'Python',
        startup_timeout_seconds=600,
        log_events_on_failure: bool = False,
        in_cluster: Optional[bool] = None,
        # this will be False/False for nonprod and True/True for prod
        reattach_on_restart: bool = True,
        delete_on_termination: bool = True,
        kubernetes_conn_id: str = 'kubernetes_default',
        **kwargs,
    ) -> None:
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(**kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.labels = labels or {}
        self.env_from = env_from or []
        self.env_vars = convert_env_vars(env_vars) if env_vars else []
        self.affinity = convert_affinity(affinity) if affinity else k8s.V1Affinity()
        self.tolerations = [convert_toleration(toleration) for toleration in tolerations] \
            if tolerations else []
        self.volume_mounts = [convert_volume_mount(v) for v in volume_mounts] if volume_mounts else []
        self.volumes = [convert_volume(volume) for volume in volumes] if volumes else []
        self.startup_timeout_seconds = startup_timeout_seconds
        self.reattach_on_restart = reattach_on_restart
        self.delete_on_termination = delete_on_termination
        self.application_file = application_file
        self.do_xcom_push = do_xcom_push
        self.name = PodGenerator.make_unique_pod_id(self.task_id)[:MAX_LABEL_LEN]
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.namespace = namespace
        self.get_logs = get_logs
        self.api_group = api_group
        self.api_version = api_version
        self.api_kind = api_kind
        self.api_plural = api_plural
        self.code_path = code_path
        self.dynamic_allocation = dynamic_allocation
        self.dynamic_alloc_max_executors = dynamic_alloc_max_executors
        self.dynamic_alloc_min_executors = dynamic_alloc_min_executors
        self.dynamic_alloc_initial_executors = dynamic_alloc_initial_executors
        if config_map_mounts:
            vols, vols_mounts = convert_configmap_to_volume(config_map_mounts)
            self.volumes.extend(vols)
            self.volume_mounts.extend(vols_mounts)
        if from_env_config_map:
            self.env_from.extend([convert_configmap(c) for c in from_env_config_map])
        if from_env_secret:
            self.env_from.extend([convert_secret(c) for c in from_env_secret])
        self.log_events_on_failure = log_events_on_failure
        self.is_delete_operator_pod = is_delete_operator_pod
        self.in_cluster = in_cluster
        self.image_pull_policy = image_pull_policy
        self.service_account_name = service_account_name
        self.image = image
        self.spark_version = spark_version
        self.spark_job_type = spark_job_type
        self.spark_job_python_version = spark_job_python_version
        self.spark_job_mode = spark_job_mode
        self.labels = {'version': self.spark_version}
        self.success_run_history_limit = success_run_history_limit
        self.number_workers = number_workers
        self.spark_obj_spec = None
        self.restart_policy = restart_policy or {'type': 'Never'}
        self.hadoop_config = hadoop_config
        self.resources = {
            'driver_limit_cpu': '1',
            'executor_limit_cpu': '1',
            'driver_limit_memory': '1Gi',
            'executor_limit_memory': '1Gi'
        }
        if not resources or not isinstance(resources, dict):
            resources = {}
        self.resources['driver_limit_cpu'] = resources.get('driver_limit_cpu', '1')
        self.resources['executor_limit_cpu'] = resources.get('executor_limit_cpu', '1')

        self.resources['driver_limit_memory'] = float(resources.get('driver_limit_memory', '1').rstrip('Gi')) * 1024
        self.resources['executor_limit_memory'] = float(resources.get('executor_limit_memory', '1').rstrip('Gi')) * 1024
        # Adjusting the memory value as operator add 40% to the given value
        self.resources['driver_limit_memory'] = str(int(self.resources['driver_limit_memory']/1.4)) + 'm'
        self.resources['executor_limit_memory'] = str(int(self.resources['executor_limit_memory']/1.4)) + 'm'

    def get_kube_client(self):
        if self.in_cluster is not None:
            core_v1_api = kube_client.get_kube_client(
                in_cluster=self.in_cluster,
                cluster_context=self.cluster_context,
                config_file=self.config_file,
            )
        else:
            core_v1_api = kube_client.get_kube_client(
                cluster_context=self.cluster_context, config_file=self.config_file
            )
        custom_obj_api = client.CustomObjectsApi()
        return core_v1_api, custom_obj_api

    @staticmethod
    def _get_pod_identifying_label_string(labels) -> str:
        filtered_labels = {label_id: label for label_id, label in labels.items() if label_id != 'try_number'}
        return ','.join([label_id + '=' + label for label_id, label in sorted(filtered_labels.items())])

    @staticmethod
    def create_labels_for_pod(context) -> dict:
        """
        Generate labels for the pod to track the pod in case of Operator crash
        :param context: task context provided by airflow DAG
        :return: dict
        """
        labels = {
            'dag_id': context['dag'].dag_id,
            'task_id': context['task'].task_id,
            'execution_date': context['ts'],
            'try_number': context['ti'].try_number,
        }
        # In the case of sub dags this is just useful
        if context['dag'].is_subdag:
            labels['parent_dag_id'] = context['dag'].parent_dag.dag_id
        # Ensure that label is valid for Kube,
        # and if not truncate/remove invalid chars and replace with short hash.
        for label_id, label in labels.items():
            safe_label = pod_generator.make_safe_label_value(str(label))
            labels[label_id] = safe_label
        return labels

    @staticmethod
    def _try_numbers_match(context, pod) -> bool:
        return pod.metadata.labels['try_number'] == context['ti'].try_number

    def execute(self, context):
        self.log.info(f'Creating sparkApplication.')
        # If yaml file used to create spark application
        if self.application_file:
            hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
            response = hook.create_custom_object(
                group=self.api_group,
                version=self.api_version,
                plural=self.api_plural,
                body=self.application_file,
                namespace=self.namespace,
            )
            return response
        self.client, custom_obj_api = self.get_kube_client()
        labels = self.create_labels_for_pod(context)
        label_selector = self._get_pod_identifying_label_string(labels) + ',spark-role=driver'
        pod_list = self.client.list_namespaced_pod(self.namespace, label_selector=label_selector)
        if len(pod_list.items) > 1 and self.reattach_on_restart:
            raise AirflowException(
                f'More than one pod running with labels: {label_selector}'
            )
        self.launcher = CustomObjectLauncher(
            namespace=self.namespace,
            kube_client=self.client,
            custom_obj_api=custom_obj_api,
            api_group=self.api_group,
            kind=self.api_kind,
            plural=self.api_plural,
            api_version=self.api_version,
            extract_xcom=self.do_xcom_push,
            application_file=self.application_file
        )
        self.launcher.set_body(
            name=self.name,
            namespace=self.namespace,
            image=self.image,
            code_path=self.code_path,
            image_pull_policy=self.image_pull_policy,
            restart_policy=self.restart_policy,
            spark_version=self.spark_version,
            spark_job_type=self.spark_job_type,
            spark_job_python_version=self.spark_job_python_version,
            spark_job_mode=self.spark_job_mode,
            labels=self.labels,
            success_run_history_limit=self.success_run_history_limit,
            service_account_name=self.service_account_name,
            dynamic_allocation=self.dynamic_allocation,
            dynamic_alloc_initial_executors=self.dynamic_alloc_initial_executors,
            dynamic_alloc_max_executors=self.dynamic_alloc_max_executors,
            dynamic_alloc_min_executors=self.dynamic_alloc_min_executors,
            driver_cpu=self.resources['driver_limit_cpu'],
            driver_memory=self.resources['driver_limit_memory'],
            executor_cpu=self.resources['executor_limit_cpu'],
            executor_memory=self.resources['executor_limit_memory'],
            number_workers=self.number_workers,
            hadoop_config=self.hadoop_config,
            env=self.env_vars,
            env_from=self.env_from,
            affinity=self.affinity,
            tolerations=self.tolerations,
            volumes=self.volumes,
            volume_mounts=self.volume_mounts
        )

        if len(pod_list.items) == 1:
            try_numbers_match = self._try_numbers_match(context, pod_list.items[0])
            final_state, result = self.handle_spark_object_overlap(
                labels, try_numbers_match, self.launcher, pod_list.items[0]
            )
        else:
            self.log.info("creating pod with labels %s and launcher %s", labels, self.launcher)
            final_state, result = self.create_new_custom_obj_for_operator(self.launcher)

        if final_state != State.SUCCESS:
            status = self.client.read_namespaced_pod(self.pod.metadata.name, self.namespace).status
            self.delete_spark_job(self.launcher)
            raise AirflowException(f'Pod {self.pod.metadata.name} returned a failure: {status.container_statuses}')

        self.delete_spark_job(self.launcher)
        return result

    def delete_spark_job(self, launcher):
        if self.delete_on_termination:
            self.log.debug("Deleting spark job for task %s", self.task_id)
            launcher.delete_spark_job()

    def handle_spark_object_overlap(
        self, labels: dict, try_numbers_match: bool, launcher: Any, pod: k8s.V1Pod
    ) -> Tuple[State, Optional[str]]:
        """

        In cases where the Scheduler restarts while a SparkK8sOperator task is running,
        this function will either continue to monitor the existing pod or launch a new pod
        based on the `reattach_on_restart` parameter.

        :param labels: labels used to determine if a pod is repeated
        :type labels: dict
        :param try_numbers_match: do the try numbers match? Only needed for logging purposes
        :type try_numbers_match: bool
        :param launcher: PodLauncher
        :param pod: list of pods found
        """
        if try_numbers_match:
            log_line = f"found a running pod with labels {labels} and the same try_number."
        else:
            log_line = f"found a running pod with labels {labels} but a different try_number."

        if self.reattach_on_restart:
            log_line += " Will attach to this pod and monitor instead of starting new one"
            self.log.info(log_line)
            self.pod = pod
            final_state, result = self.monitor_launched_pod(launcher, pod)
        else:
            log_line += f"creating pod with labels {labels} and launcher {launcher}"
            self.log.info(log_line)
            final_state, result = self.create_new_custom_obj_for_operator(launcher)
        return final_state, result

    def monitor_launched_pod(self, launcher, pod) -> Tuple[State, Optional[str]]:
        """
        Monitors a pod to completion that was created by a previous KubernetesPodOperator

        :param launcher: pod launcher that will manage launching and monitoring pods
        :param pod: podspec used to find pod using k8s API
        :return:
        """
        try:
            (final_state, result) = launcher.monitor_pod(pod, get_logs=self.get_logs)
        finally:
            if self.is_delete_operator_pod:
                launcher.delete_pod(pod)
        if final_state != State.SUCCESS:
            if self.log_events_on_failure:
                for event in launcher.read_pod_events(pod).items:
                    self.log.error("Pod Event: %s - %s", event.reason, event.message)
            self.patch_already_checked(self.pod)
            raise AirflowException(f'Pod returned a failure: {final_state}')
        return final_state, result

    def patch_already_checked(self, pod: k8s.V1Pod):
        """Add an "already tried annotation to ensure we only retry once"""
        pod.metadata.labels["already_checked"] = "True"
        body = PodGenerator.serialize_pod(pod)
        self.client.patch_namespaced_pod(pod.metadata.name, pod.metadata.namespace, body)

    def create_new_custom_obj_for_operator(self, launcher) -> Tuple[State, Optional[str]]:
        """
        Creates a new pod and monitors for duration of task

        :param launcher: pod launcher that will manage launching and monitoring pods
        :return:
        """
        try:
            self.pod, self.spark_obj_spec = launcher.start_spark_job(startup_timeout=self.startup_timeout_seconds)
            final_state, result = launcher.monitor_pod(pod=launcher.pod_spec, get_logs=self.get_logs)
        except AirflowException:
            if self.log_events_on_failure:
                for event in launcher.read_pod_events(self.pod).items:
                    self.log.error("Pod Event: %s - %s", event.reason, event.message)
            raise
        return final_state, result
