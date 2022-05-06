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
import json
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml
from kubernetes import client
from kubernetes.client import models as k8s

from airflow import AirflowException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import _suppress
from airflow.utils.helpers import prune_dict

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property
from airflow.kubernetes import kube_client, pod_generator
from airflow.kubernetes.custom_object_launcher import CustomObjectLauncher, SparkResources
from airflow.kubernetes.pod_generator import MAX_LABEL_LEN, PodGenerator
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_affinity,
    convert_configmap,
    convert_configmap_to_volume,
    convert_env_vars,
    convert_image_pull_secrets,
    convert_secret,
    convert_toleration,
    convert_volume,
    convert_volume_mount,
)
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, PodPhase
from airflow.utils.state import State


class SparkKubernetesOperator(BaseOperator):
    """
    Creates sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.3.3-3.1.1/docs/api-docs.md#sparkapplication

    :param application_file: filepath to kubernetes custom_resource_definition of sparkApplication
    :param kubernetes_conn_id: the connection to Kubernetes cluster
    :param image: Docker image you wish to launch. Defaults to hub.docker.com,
    :param code_path: path to the code in your image,
    :param namespace: kubernetes namespace to put sparkApplication
    :param api_group: CRD api group for spark
            https://github.com/GoogleCloudPlatform/spark-on-k8s-operator#project-status
    :param api_version: CRD api version
    :param api_kind: CRD api kind
    :param api_plural: CRD api plural
    :param cluster_context: context of the cluster
    :param labels: labels to apply to the crd.
    :param config_file: kube configuration file
    :param resources: resources for the launched pod.
    :param number_workers: number spark executors
    :param env_vars: A dictionary of key:value OR list of V1EnvVar items
    :param env_from: A list of V1EnvFromSource items
    :param affinity: Affinity scheduling rules for the launched pod.(V1Affinity)
    :param tolerations: A list of kubernetes tolerations.(V1Toleration)
    :param volume_mounts: A list of V1VolumeMount items
    :param volumes: A list of V1Volume items
    :param config_map_mounts: A dictionary of config_map as key and path as value
    :param from_env_config_map: Read configmap into a env variable(name of the configmap)
    :param from_env_secret: Read secret into a env variable(name of the configmap)
    :param hadoop_config: hadoop base config e.g, AWS s3 config
    :param application_file: yaml file if passed
    :param image_pull_secrets: Any image pull secrets to be given to the pod.
        If more than one secret is required, provide a
        comma separated list: secret_a,secret_b
    :param get_logs: get the stdout of the container as logs of the tasks.
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    :param restart_policy: restart policy of the driver/executor
    :param spark_version: spark version
    :param success_run_history_limit: Number of past successful runs of the application to keep.
    :param delete_on_termination: What to do when the pod reaches its final
        state, or the execution is interrupted. If True (default), delete the
        pod; if False, leave the pod.
    :param dynamic_allocation: Enable spark dynamic allocation
    :param dynamic_alloc_max_executors: Max number of executor if dynamic_allocation is enabled
    :param dynamic_alloc_initial_executors: Initial number of executor if dynamic_allocation is enabled
    :param dynamic_alloc_min_executors: min number of executor if dynamic_allocation is enabled
    :param image_pull_policy: Specify a policy to cache or always pull an image.
    :param service_account_name: Name of the service account
    :param spark_job_mode: spark job type in spark operator(at the time of writing it just supports cluster)
    :param spark_job_python_version: version of spark python
    :param spark_job_type: type of spark job
    :param startup_timeout_seconds: timeout in seconds to startup the pod.
    :param log_events_on_failure: Log the pod's events if a failure occurs
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param reattach_on_restart: if the scheduler dies while the pod is running, reattach and monitor
    """

    template_fields = ['application_file', 'namespace']
    template_ext = ('yaml', 'yml', 'json')
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        image: Optional[str] = None,
        code_path: Optional[str] = None,
        namespace: Optional[str] = None,
        api_group: str = 'sparkoperator.k8s.io',
        api_version: str = 'v1beta2',
        api_kind: str = 'SparkApplication',
        api_plural: str = 'sparkapplications',
        cluster_context: Optional[str] = None,
        config_file: Optional[str] = None,
        labels: Optional[dict] = None,
        resources: Optional[dict] = None,
        number_workers: int = 1,
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
        image_pull_secrets: Optional[Union[List[k8s.V1LocalObjectReference], str]] = None,
        get_logs: bool = True,
        do_xcom_push: bool = False,
        restart_policy: Optional[dict] = None,
        spark_version: str = '3.0.0',
        success_run_history_limit: int = 1,
        dynamic_allocation: bool = False,
        dynamic_alloc_max_executors: Optional[int] = None,
        dynamic_alloc_initial_executors: int = 1,
        dynamic_alloc_min_executors: int = 1,
        image_pull_policy: str = 'Always',
        service_account_name: str = 'default',
        spark_job_mode: str = 'cluster',
        spark_job_python_version: str = '3',
        spark_job_type: str = 'Python',
        startup_timeout_seconds=600,
        log_events_on_failure: bool = False,
        in_cluster: Optional[bool] = None,
        reattach_on_restart: bool = True,
        delete_on_termination: bool = False,
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
        self.tolerations = (
            [convert_toleration(toleration) for toleration in tolerations] if tolerations else []
        )
        self.volume_mounts = [convert_volume_mount(v) for v in volume_mounts] if volume_mounts else []
        self.volumes = [convert_volume(volume) for volume in volumes] if volumes else []
        self.startup_timeout_seconds = startup_timeout_seconds
        self.reattach_on_restart = reattach_on_restart
        self.delete_on_termination = delete_on_termination
        self.application_file = application_file
        self.image_pull_secrets = convert_image_pull_secrets(image_pull_secrets) if image_pull_secrets else []
        self.do_xcom_push = do_xcom_push
        self.name = PodGenerator.make_unique_pod_id(self.task_id)
        if self.name:
            self.name = self.name[:MAX_LABEL_LEN]
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
        if dynamic_allocation:
            if not all(
                [dynamic_alloc_max_executors, dynamic_alloc_min_executors, dynamic_alloc_initial_executors]
            ):
                raise AirflowException("Make sure initial/min/max value for dynamic allocation is passed")
        if config_map_mounts:
            vols, vols_mounts = convert_configmap_to_volume(config_map_mounts)
            self.volumes.extend(vols)
            self.volume_mounts.extend(vols_mounts)
        if from_env_config_map:
            self.env_from.extend([convert_configmap(c) for c in from_env_config_map])
        if from_env_secret:
            self.env_from.extend([convert_secret(c) for c in from_env_secret])
        self.log_events_on_failure = log_events_on_failure
        self.in_cluster = in_cluster
        self.image_pull_policy = image_pull_policy
        self.service_account_name = service_account_name
        self.image = image
        self.spark_version = spark_version
        self.spark_job_type = spark_job_type
        self.spark_job_python_version = spark_job_python_version
        self.spark_job_mode = spark_job_mode
        self.success_run_history_limit = success_run_history_limit
        self.number_workers = number_workers
        self.spark_obj_spec = None
        self.restart_policy = restart_policy or {'type': 'Never'}
        self.hadoop_config = hadoop_config
        self.job_resources = SparkResources(**resources) if resources else SparkResources()

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

    @cached_property
    def pod_manager(self) -> PodManager:
        return PodManager(kube_client=self.client)

    @staticmethod
    def _try_numbers_match(context, pod) -> bool:
        return pod.metadata.labels['try_number'] == context['ti'].try_number

    def build_spark_request_obj(self, kube_client, custom_obj_api):
        launcher = CustomObjectLauncher(
            namespace=self.namespace,
            kube_client=kube_client,
            custom_obj_api=custom_obj_api,
            api_group=self.api_group,
            kind=self.api_kind,
            plural=self.api_plural,
            api_version=self.api_version,
            extract_xcom=self.do_xcom_push,
            application_file=self.application_file,
        )
        launcher.set_body(
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
            driver_resource=self.job_resources.driver_resources,
            executor_resource=self.job_resources.executor_resources,
            number_workers=self.number_workers,
            hadoop_config=self.hadoop_config,
            image_pull_secrets=self.image_pull_secrets,
            env=self.env_vars,
            env_from=self.env_from,
            affinity=self.affinity,
            tolerations=self.tolerations,
            volumes=self.volumes,
            volume_mounts=self.volume_mounts,
        )
        return launcher

    def find_spark_job(self, context):
        labels = self.create_labels_for_pod(context)
        label_selector = self._get_pod_identifying_label_string(labels) + ',spark-role=driver'
        pod_list = self.client.list_namespaced_pod(self.namespace, label_selector=label_selector).items

        pod = None
        if len(pod_list) > 1:  # and self.reattach_on_restart:
            raise AirflowException(f'More than one pod running with labels: {label_selector}')
        elif len(pod_list) == 1:
            pod = pod_list[0]
            self.log.info(
                "Found matching driver pod %s with labels %s", pod.metadata.name, pod.metadata.labels
            )
            self.log.info("`try_number` of task_instance: %s", context['ti'].try_number)
            self.log.info("`try_number` of pod: %s", pod.metadata.labels['try_number'])
        return pod

    def get_or_create_spark_crd(self, launcher: CustomObjectLauncher, context):
        if self.reattach_on_restart:
            driver_pod = self.find_spark_job(context)
            if driver_pod:
                return driver_pod

        driver_pod, spark_obj_spec = launcher.start_spark_job(startup_timeout=self.startup_timeout_seconds)
        # if self.reattach_on_restart:
        #     pod = self.find_pod(self.namespace or pod_request_obj.metadata.namespace, context=context)
        #     if pod:
        #         return pod
        # self.log.debug("Starting pod:\n%s", yaml.safe_dump(pod_request_obj.to_dict()))
        # self.pod_manager.create_pod(pod=pod_request_obj)
        return driver_pod

    def extract_xcom(self, pod):
        """Retrieves xcom value and kills xcom sidecar container"""
        result = self.pod_manager.extract_xcom(pod)
        self.log.info("xcom result: \n%s", result)
        return json.loads(result)

    def cleanup(self, pod: k8s.V1Pod, remote_pod: k8s.V1Pod):
        pod_phase = remote_pod.status.phase if hasattr(remote_pod, 'status') else None
        if not self.delete_on_termination:
            with _suppress(Exception):
                self.patch_already_checked(pod)
        if pod_phase != PodPhase.SUCCEEDED:
            if self.log_events_on_failure:
                with _suppress(Exception):
                    for event in self.pod_manager.read_pod_events(pod).items:
                        self.log.error("Pod Event: %s - %s", event.reason, event.message)
            with _suppress(Exception):
                self.process_spark_job_deletion(pod)
            raise AirflowException(f'Pod {pod and pod.metadata.name} returned a failure\n{remote_pod}')
        else:
            with _suppress(Exception):
                self.process_spark_job_deletion(pod)

    def process_spark_job_deletion(self, pod):
        if self.delete_on_termination:
            self.log.info("Deleting spark job: %s", pod.metadata)
            self.launcher.delete_spark_job(pod.metadata.name.replace('-driver', ''))
        else:
            self.log.info("skipping deleting spark job: %s", pod.metadata.name)

    def execute(self, context):
        self.log.info('Creating sparkApplication.')
        # If yaml file used to create spark application
        remote_pod = None
        driver_pod = None
        try:
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
            self.launcher = self.build_spark_request_obj(self.client, custom_obj_api)
            driver_pod = self.get_or_create_spark_crd(self.launcher, context)

            if self.get_logs:
                self.launcher.fetch_container_logs(
                    pod=driver_pod,
                    container_name='spark-kubernetes-driver',
                    follow=True,
                )
            else:
                self.launcher.await_container_completion(
                    pod=driver_pod, container_name='spark-kubernetes-driver'
                )

            if self.do_xcom_push:
                result = self.extract_xcom(pod=driver_pod)
            remote_pod = self.launcher.await_pod_completion(driver_pod)
        finally:
            self.cleanup(
                pod=driver_pod or self.launcher.pod_spec,
                remote_pod=remote_pod,
            )
        ti = context['ti']
        ti.xcom_push(key='pod_name', value=driver_pod.metadata.name)
        ti.xcom_push(key='pod_namespace', value=driver_pod.metadata.namespace)
        if self.do_xcom_push:
            return result

    def on_kill(self) -> None:
        if self.launcher:
            self.log.debug("Deleting spark job for task %s", self.task_id)
            self.launcher.delete_spark_job()

    def patch_already_checked(self, pod: k8s.V1Pod):
        """Add an "already checked" annotation to ensure we don't reattach on retries"""
        pod.metadata.labels["already_checked"] = "True"
        body = PodGenerator.serialize_pod(pod)
        self.client.patch_namespaced_pod(pod.metadata.name, pod.metadata.namespace, body)

    def dry_run(self) -> None:
        """
        Prints out the spark job that would be created by this operator.
        """
        client, custom_obj_api = self.get_kube_client()
        launcher = self.build_spark_request_obj(client, custom_obj_api)
        print(yaml.dump(prune_dict(launcher.body(), mode='strict')))
