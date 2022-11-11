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
from __future__ import annotations

import json
import sys

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
from airflow.providers.cncf.kubernetes.resource_convert.configmap import (
    convert_configmap,
    convert_configmap_to_volume,
)
from airflow.providers.cncf.kubernetes.resource_convert.env_variable import convert_env_vars
from airflow.providers.cncf.kubernetes.resource_convert.secret import (
    convert_image_pull_secrets,
    convert_secret,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, PodPhase


class SparkKubernetesOperator(BaseOperator):
    """
    Creates sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.3.3-3.1.1/docs/api-docs.md#sparkapplication

    :param image: Docker image you wish to launch. Defaults to hub.docker.com,
    :param code_path: path to the spark code in image,
    :param namespace: kubernetes namespace to put sparkApplication
    :param application_file: filepath to kubernetes custom_resource_definition of sparkApplication.
           (yaml file if passed)
    :param spark_api_group: CRD api group for spark
            https://github.com/GoogleCloudPlatform/spark-on-k8s-operator#project-status
    :param spark_api_version: CRD api version
    :param spark_api_kind: CRD api kind
    :param spark_api_plural: CRD api plural
    :param spark_resources: resources defined for driver and executor pods.
    :param spark_dynamic_allocation: Enable spark dynamic allocation
    :param spark_dynamic_alloc_max_executors: Max number of executor if spark_dynamic_allocation is enabled
    :param spark_dynamic_alloc_initial_executors: Initial number of executor if spark_dynamic_allocation is enabled
    :param spark_dynamic_alloc_min_executors: min number of executor if spark_dynamic_allocation is enabled
    :param spark_version: spark version
    :param spark_job_mode: spark job type in spark operator(at the time of writing it just supports cluster)
    :param spark_job_python_version: version of spark python
    :param spark_job_type: type of spark job
    :param spark_hadoop_config: hadoop base config e.g, AWS s3 config
    :param spark_number_workers: number spark executors
    :param k8s_env_vars: A dictionary of key:value OR list of V1EnvVar items
    :param k8s_env_from: A list of V1EnvFromSource items
    :param k8s_affinity: Affinity scheduling rules for the launched pod.(V1Affinity)
    :param k8s_tolerations: A list of kubernetes tolerations.(V1Toleration)
    :param k8s_volume_mounts: A list of V1VolumeMount items
    :param k8s_volumes: A list of V1Volume items
    :param k8s_config_map_mounts: A dictionary of config_map as key and path as value
    :param k8s_from_env_config_map: Read configmap into a env variable(name of the configmap)
    :param k8s_from_env_secret: Read secret into a env variable(name of the configmap)
    :param k8s_image_pull_policy: Specify a policy to cache or always pull an image.
    :param k8s_service_account_name: Name of the service account
    :param k8s_image_pull_secrets: Any image pull secrets to be given to the pod.
        If more than one secret is required, provide a
        comma separated list: secret_a,secret_b
    :param k8s_labels: labels to apply to the crd.
    :param k8s_restart_policy: restart policy of the driver/executor
    :param k8s_cluster_context: context of the cluster
    :param k8s_config_file: kube configuration file
    :param k8s_in_cluster: run kubernetes client with in_cluster configuration.
    :param get_logs: get the stdout of the container as logs of the tasks.
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    :param success_run_history_limit: Number of past successful runs of the application to keep.
    :param delete_on_termination: What to do when the pod reaches its final
        state, or the execution is interrupted. If True (default), delete the
        pod; if False, leave the pod.
    :param startup_timeout_seconds: timeout in seconds to startup the pod.
    :param log_events_on_failure: Log the pod's events if a failure occurs
    :param reattach_on_restart: if the scheduler dies while the pod is running, reattach and monitor
    """

<<<<<<< HEAD
    template_fields = ['application_file', 'namespace']
    template_ext = ('yaml', 'yml', 'json')
    ui_color = '#f4a460'
=======
    template_fields: Sequence[str] = ("application_file", "namespace")
    template_ext: Sequence[str] = (".yaml", ".yml", ".json")
    ui_color = "#f4a460"
>>>>>>> origin/main

    def __init__(
        self,
        *,
<<<<<<< HEAD
        image: str | None = None,
        code_path: str | None = None,
        namespace: str | None = 'default',
        application_file: str | None = None,
        spark_api_group: str = 'sparkoperator.k8s.io',
        spark_api_version: str = 'v1beta2',
        spark_api_kind: str = 'SparkApplication',
        spark_api_plural: str = 'sparkapplications',
        spark_resources: dict | None = None,
        spark_dynamic_allocation: bool = False,
        spark_dynamic_alloc_max_executors: int | None = None,
        spark_dynamic_alloc_initial_executors: int = 1,
        spark_dynamic_alloc_min_executors: int = 1,
        spark_version: str = '3.0.0',
        spark_job_mode: str = 'cluster',
        spark_job_python_version: str = '3',
        spark_job_type: str = 'Python',
        spark_hadoop_config: dict | None = None,
        spark_number_workers: int = 1,
        k8s_env_vars: list[k8s.V1EnvVar] | dict | None = None,
        k8s_env_from: list[k8s.V1EnvFromSource] | None = None,
        k8s_affinity: k8s.V1Affinity | None = None,
        k8s_tolerations: list[k8s.V1Toleration] | None = None,
        k8s_volume_mounts: list[k8s.V1VolumeMount] | None = None,
        k8s_volumes: list[k8s.V1Volume] | None = None,
        k8s_config_map_mounts: dict[str, str] | None = None,
        k8s_from_env_config_map: list[str] | None = None,
        k8s_from_env_secret: list[str] | None = None,
        k8s_image_pull_policy: str = 'Always',
        k8s_service_account_name: str = 'default',
        k8s_image_pull_secrets: list[k8s.V1LocalObjectReference] | str | None = None,
        k8s_labels: dict | None = None,
        k8s_restart_policy: dict | None = None,
        k8s_cluster_context: str | None = None,
        k8s_config_file: str | None = None,
        k8s_in_cluster: bool | None = None,
        get_logs: bool = True,
        do_xcom_push: bool = False,
        success_run_history_limit: int = 1,
        delete_on_termination: bool = True,
        startup_timeout_seconds=600,
        log_events_on_failure: bool = False,
        reattach_on_restart: bool = True,
=======
        application_file: str,
        namespace: str | None = None,
        kubernetes_conn_id: str = "kubernetes_default",
        api_group: str = "sparkoperator.k8s.io",
        api_version: str = "v1beta2",
>>>>>>> origin/main
        **kwargs,
    ) -> None:
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'do_xcom_push' instead")
        super().__init__(**kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.startup_timeout_seconds = startup_timeout_seconds
        self.reattach_on_restart = reattach_on_restart
        self.delete_on_termination = delete_on_termination
        self.application_file = application_file
        self.do_xcom_push = do_xcom_push
        self.name = PodGenerator.make_unique_pod_id(self.task_id)
        if self.name:
            self.name = self.name[:MAX_LABEL_LEN]
        self.k8s_cluster_context = k8s_cluster_context
        self.k8s_config_file = k8s_config_file
        self.namespace = namespace
        self.get_logs = get_logs
        self.code_path = code_path
        self.log_events_on_failure = log_events_on_failure
        self.k8s_in_cluster = k8s_in_cluster
        self.image = image
        self.success_run_history_limit = success_run_history_limit
        self.k8s_labels = k8s_labels or {}
        self.k8s_env_from = k8s_env_from or []
        self.k8s_env_vars = convert_env_vars(k8s_env_vars) if k8s_env_vars else []
        self.k8s_affinity = k8s_affinity or k8s.V1Affinity()
        self.k8s_tolerations = k8s_tolerations or []
        self.k8s_volume_mounts = k8s_volume_mounts or []
        self.k8s_volumes = k8s_volumes or []
        self.k8s_image_pull_secrets = convert_image_pull_secrets(k8s_image_pull_secrets) if k8s_image_pull_secrets else []
        self.k8s_image_pull_policy = k8s_image_pull_policy
        self.k8s_service_account_name = k8s_service_account_name
        self.k8s_restart_policy = k8s_restart_policy or {'type': 'Never'}
        self.spark_api_group = spark_api_group
        self.spark_api_version = spark_api_version
        self.spark_api_kind = spark_api_kind
        self.spark_api_plural = spark_api_plural
        self.spark_dynamic_allocation = spark_dynamic_allocation
        self.spark_dynamic_alloc_max_executors = spark_dynamic_alloc_max_executors
        self.spark_dynamic_alloc_min_executors = spark_dynamic_alloc_min_executors
        self.spark_dynamic_alloc_initial_executors = spark_dynamic_alloc_initial_executors
        self.spark_version = spark_version
        self.spark_job_type = spark_job_type
        self.spark_job_python_version = spark_job_python_version
        self.spark_job_mode = spark_job_mode
        self.spark_number_workers = spark_number_workers
        self.spark_obj_spec = None
        self.spark_hadoop_config = spark_hadoop_config
        if spark_dynamic_allocation:
            if not all(
                [spark_dynamic_alloc_max_executors, spark_dynamic_alloc_min_executors, spark_dynamic_alloc_initial_executors]
            ):
                raise AirflowException("Make sure initial/min/max value for dynamic allocation is passed")
        if k8s_config_map_mounts:
            vols, vols_mounts = convert_configmap_to_volume(k8s_config_map_mounts)
            self.k8s_volumes.extend(vols)
            self.k8s_volume_mounts.extend(vols_mounts)
        if k8s_from_env_config_map:
            self.k8s_env_from.extend([convert_configmap(c_name) for c_name in k8s_from_env_config_map])
        if k8s_from_env_secret:
            self.k8s_env_from.extend([convert_secret(c) for c in k8s_from_env_secret])
        self.job_resources = SparkResources(**spark_resources) if spark_resources else SparkResources()

    def get_kube_clients(self):
        if self.k8s_in_cluster is not None:
            core_v1_api = kube_client.get_kube_client(
                in_cluster=self.k8s_in_cluster,
                cluster_context=self.k8s_cluster_context,
                config_file=self.k8s_config_file,
            )
        else:
            core_v1_api = kube_client.get_kube_client(
                cluster_context=self.k8s_cluster_context, config_file=self.k8s_config_file
            )
        custom_obj_api = client.CustomObjectsApi()
        return core_v1_api, custom_obj_api

    @staticmethod
    def _get_pod_identifying_label_string(labels) -> str:
        filtered_labels = {label_id: label for label_id, label in labels.items() if label_id != 'try_number'}
        return ','.join([label_id + '=' + label for label_id, label in sorted(filtered_labels.items())])

    @staticmethod
    def create_labels_for_pod(context: dict | None = None, include_try_number: bool = True) -> dict:
        """
        Generate labels for the pod to track the pod in case of Operator crash
        :param include_try_number: add try number to labels
        :param context: task context provided by airflow DAG
        :return: dict
        """
        if not context:
            return {}

        ti = context['ti']
        run_id = context['run_id']

        labels = {
            'dag_id': ti.dag_id,
            'task_id': ti.task_id,
            'run_id': run_id,
            'spark_kubernetes_operator': 'True',
            # 'execution_date': context['ts'],
            # 'try_number': context['ti'].try_number,
        }

        # If running on Airflow 2.3+:
        map_index = getattr(ti, 'map_index', -1)
        if map_index >= 0:
            labels['map_index'] = map_index

        if include_try_number:
            labels.update(try_number=ti.try_number)

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
            api_group=self.spark_api_group,
            kind=self.spark_api_kind,
            plural=self.spark_api_plural,
            api_version=self.spark_api_version,
            extract_xcom=self.do_xcom_push,
            application_file=self.application_file,
        )
        launcher.set_body(
            application_file=self.application_file,
            name=self.name,
            namespace=self.namespace,
            image=self.image,
            code_path=self.code_path,
            image_pull_policy=self.k8s_image_pull_policy,
            restart_policy=self.k8s_restart_policy,
            spark_version=self.spark_version,
            spark_job_type=self.spark_job_type,
            spark_job_python_version=self.spark_job_python_version,
            spark_job_mode=self.spark_job_mode,
            labels=self.k8s_labels,
            success_run_history_limit=self.success_run_history_limit,
            service_account_name=self.k8s_service_account_name,
            dynamic_allocation=self.spark_dynamic_allocation,
            dynamic_alloc_initial_executors=self.spark_dynamic_alloc_initial_executors,
            dynamic_alloc_max_executors=self.spark_dynamic_alloc_max_executors,
            dynamic_alloc_min_executors=self.spark_dynamic_alloc_min_executors,
            driver_resource=self.job_resources.driver_resources,
            executor_resource=self.job_resources.executor_resources,
            number_workers=self.spark_number_workers,
            hadoop_config=self.spark_hadoop_config,
            image_pull_secrets=self.k8s_image_pull_secrets,
            env=self.k8s_env_vars,
            env_from=self.k8s_env_from,
            affinity=self.k8s_affinity,
            tolerations=self.k8s_tolerations,
            volumes=self.k8s_volumes,
            volume_mounts=self.k8s_volume_mounts,
        )
        return launcher

    def find_spark_job(self, context):
        labels = self.create_labels_for_pod(context, include_try_number=False)
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

        # self.log.debug("Starting spark job:\n%s", yaml.safe_dump(launcher.body.to_dict()))
        driver_pod, spark_obj_spec = launcher.start_spark_job(startup_timeout=self.startup_timeout_seconds)
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
            self.client, custom_obj_api = self.get_kube_clients()
            self.launcher = self.build_spark_request_obj(self.client, custom_obj_api)
            driver_pod = self.get_or_create_spark_crd(self.launcher, context)

            if self.get_logs:
                self.pod_manager.fetch_container_logs(
                    pod=driver_pod,
                    container_name='spark-kubernetes-driver',
                    follow=True,
                )
            else:
                self.pod_manager.await_container_completion(
                    pod=driver_pod, container_name='spark-kubernetes-driver'
                )

            if self.do_xcom_push:
                result = self.extract_xcom(pod=driver_pod)
            remote_pod = self.pod_manager.await_pod_completion(driver_pod)
        except Exception as e:
            print(e)
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
        """Prints out the spark job that would be created by this operator."""
        client, custom_obj_api = self.get_kube_clients()
        launcher = self.build_spark_request_obj(client, custom_obj_api)
        print(yaml.dump(prune_dict(launcher.body(), mode='strict')))
