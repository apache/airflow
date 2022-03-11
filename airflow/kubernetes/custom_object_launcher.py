"""Launches Custom object"""
import time
from datetime import datetime as dt

import tenacity
import yaml
from airflow.kubernetes.pod_launcher import PodLauncher
from kubernetes import client, watch
from kubernetes.client import models as k8s
from kubernetes.client.rest import ApiException

from airflow.exceptions import AirflowException


def should_retry_start_spark_job(exception: Exception):
    """Check if an Exception indicates a transient error and warrants retrying"""
    if isinstance(exception, ApiException):
        return exception.status == 409
    return False


class CustomObjectStatus:
    """Status of the PODs"""

    SUBMITTED = 'SUBMITTED'
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    SUCCEEDED = 'SUCCEEDED'
    INITIAL_SPEC = {
        'metadata': {},
        'spec': {
            'driver': {
                'cores': 1,
                'coreLimit': '1',
                'memory': '512m',
            },
            'executor': {
                'cores': 1,
                'coreLimit': '1',
                'memory': '512m',
                'instances': 1,
            }
        }
    }


class CustomObjectLauncher(PodLauncher):
    """Launches PODS"""

    def __init__(
        self,
        kube_client: client.CoreV1Api,
        custom_obj_api: client.CustomObjectsApi,
        namespace: str = 'ds',
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
            self.body = self.get_body(f'{self.api_group}/{self.api_version}', self.kind, CustomObjectStatus.INITIAL_SPEC, **kwargs)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_random_exponential(),
        reraise=True,
        retry=tenacity.retry_if_exception(should_retry_start_spark_job),
    )
    def start_spark_job(self, startup_timeout: int = 600):
        """
        Launches the pod synchronously and waits for completion.

        :param pod:
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

        self.pod_spec = k8s.V1Pod(metadata=k8s.V1ObjectMeta(
            labels=self.spark_obj_spec['spec']['driver']['labels'],
            name=self.spark_obj_spec['metadata']['name'] + '-driver',
            namespace=self.namespace
        ))
        curr_time = dt.now()
        while self.spark_job_not_running(self.spark_obj_spec):
            self.log.warning("Spark job submitted but not yet started: %s", self.spark_obj_spec['metadata']['name'])
            delta = dt.now() - curr_time
            if delta.total_seconds() >= startup_timeout:
                pod_status = self.read_pod(self.pod_spec).status.container_statuses
                raise AirflowException(f"Job took too long to start. pod status: {pod_status}")
            time.sleep(2)

        return self.pod_spec, self.spark_obj_spec

    def spark_job_not_running(self, spark_obj_spec):
        """Tests if spark_obj_spec has not started"""
        spark_job_info = self.custom_obj_api.get_namespaced_custom_object_status(group=self.api_group, version=self.api_version, namespace=self.namespace, name=spark_obj_spec['metadata']['name'], plural=self.plural)
        driver_state = spark_job_info.get('status', {}).get('applicationState', {}).get('state', 'SUBMITTED')
        if driver_state == CustomObjectStatus.FAILED:
            err = spark_job_info.get('status', {}).get('applicationState', {}).get('errorMessage')
            raise AirflowException(f"Spark Job Failed. Error stack:\n{err}")
        return driver_state == CustomObjectStatus.SUBMITTED

    def delete_spark_job(self, spark_obj_spec=None):
        """Deletes spark job"""
        if not spark_obj_spec:
            spark_obj_spec = self.spark_obj_spec
        try:
            v1 = client.CustomObjectsApi()
            v1.delete_namespaced_custom_object(
                group=self.api_group,
                version=self.api_version,
                namespace=self.namespace,
                plural=self.plural,
                name=spark_obj_spec['metadata']['name'],
            )
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    @staticmethod
    def get_body(api_version, kind, body_template, **kwargs):
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
        body_template['spec']['driver']['coreLimit'] = kwargs['driver_cpu']
        body_template['spec']['driver']['cores'] = int(float(kwargs['driver_cpu']))
        body_template['spec']['driver']['memory'] = kwargs['driver_memory']
        body_template['spec']['driver']['labels'] = kwargs['labels']
        body_template['spec']['executor']['coreLimit'] = kwargs['executor_cpu']
        body_template['spec']['executor']['cores'] = int(float(kwargs['executor_cpu']))
        body_template['spec']['executor']['memory'] = kwargs['executor_memory']
        body_template['spec']['executor']['instances'] = int(kwargs['number_workers'])
        body_template['spec']['executor']['labels'] = kwargs['labels']

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
