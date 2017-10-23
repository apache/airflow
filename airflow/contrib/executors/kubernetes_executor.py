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

import base64
import os
import multiprocessing
import six
from queue import Queue
from dateutil import parser
from uuid import uuid4
from kubernetes import watch, client
from kubernetes.client.rest import ApiException
from airflow.contrib.kubernetes.pod_launcher import PodLauncher
from airflow.contrib.kubernetes.kube_client import get_kube_client
from airflow.contrib.kubernetes.worker_configuration import WorkerConfiguration
from airflow.executors.base_executor import BaseExecutor
from airflow.models import TaskInstance, KubeResourceVersion
from airflow.utils.state import State
from airflow import configuration, settings
from airflow.exceptions import AirflowConfigException
from airflow.utils.log.logging_mixin import LoggingMixin

class KubeConfig:
    core_section = "core"
    kubernetes_section = "kubernetes"

    @staticmethod
    def safe_get(section, option, default):
        try:
            return configuration.get(section, option)
        except AirflowConfigException:
            return default

    @staticmethod
    def safe_getboolean(section, option, default):
        try:
            return configuration.getboolean(section, option)
        except AirflowConfigException:
            return default

    def __init__(self):
        configuration_dict = configuration.as_dict(display_sensitive=True)
        self.core_configuration = configuration_dict['core']
        self.kube_secrets = configuration_dict.get('kubernetes_secrets', {})
        self.airflow_home = configuration.get(self.core_section, 'airflow_home')
        self.dags_folder = configuration.get(self.core_section, 'dags_folder')
        self.parallelism = configuration.getint(self.core_section, 'PARALLELISM')
        self.worker_container_repository = configuration.get(
            self.kubernetes_section, 'worker_container_repository')
        self.worker_container_tag = configuration.get(
            self.kubernetes_section, 'worker_container_tag')
        self.kube_image = '{}:{}'.format(
            self.worker_container_repository, self.worker_container_tag)
        self.delete_worker_pods = self.safe_getboolean(self.kubernetes_section, 'delete_worker_pods', True)

        self.worker_service_account_name = self.safe_get(
            self.kubernetes_section, 'worker_service_account_name', 'default')
        self.image_pull_secrets = self.safe_get(
            self.kubernetes_section, 'image_pull_secrets', '')

        # NOTE: `git_repo` and `git_branch` must be specified together as a pair
        # The http URL of the git repository to clone from
        self.git_repo = self.safe_get(self.kubernetes_section, 'git_repo', None)
        # The branch of the repository to be checked out
        self.git_branch = self.safe_get(self.kubernetes_section, 'git_branch', None)
        # Optionally, the directory in the git repository containing the dags
        self.git_subpath = self.safe_get(self.kubernetes_section, 'git_subpath', '')

        # Optionally a user may supply a `git_user` and `git_password` for private repositories
        self.git_user = self.safe_get(self.kubernetes_section, 'git_user', None)
        self.git_password = self.safe_get(self.kubernetes_section, 'git_password', None)

        # NOTE: The user may optionally use a volume claim to mount a PV containing DAGs directly
        self.dags_volume_claim = self.safe_get(self.kubernetes_section, 'dags_volume_claim', None)

        # This prop may optionally be set for PV Claims and is used to locate DAGs on a SubPath
        self.dags_volume_subpath = self.safe_get(self.kubernetes_section, 'dags_volume_subpath', None)

        # The Kubernetes Namespace in which the Scheduler and Webserver reside. Note that if your
        # cluster has RBAC enabled, your scheduler may need service account permissions to
        # create, watch, get, and delete pods in this namespace.
        self.kube_namespace = self.safe_get(self.kubernetes_section, 'namespace', 'default')
        # The Kubernetes Namespace in which pods will be created by the executor. Note that if your
        # cluster has RBAC enabled, your workers may need service account permissions to
        # interact with cluster components.
        self.executor_namespace = self.safe_get(self.kubernetes_section, 'namespace', 'default')
        # Task secrets managed by KubernetesExecutor.
        self.gcp_service_account_keys = self.safe_get(self.kubernetes_section, 'gcp_service_account_keys', None)

        # If the user is using the git-sync container to clone their repository via git,
        # allow them to specify repository, tag, and pod name for the init container.
        self.git_sync_container_repository = self.safe_get(
            self.kubernetes_section, 'git_sync_container_repository',
            'gcr.io/google-containers/git-sync-amd64')

        self.git_sync_container_tag = self.safe_get(
            self.kubernetes_section, 'git_sync_container_tag', 'v2.0.5')
        self.git_sync_container = '{}:{}'.format(
            self.git_sync_container_repository, self.git_sync_container_tag)

        self.git_sync_init_container_name = self.safe_get(
            self.kubernetes_section, 'git_sync_init_container_name', 'git-sync-clone')

        # The worker pod may optionally have a  valid Airflow config loaded via a configmap
        self.airflow_configmap = self.safe_get(self.kubernetes_section, 'airflow_configmap', None)

        self._validate()

    def _validate(self):
        if not self.dags_volume_claim and (not self.git_repo or not self.git_branch):
            raise AirflowConfigException(
                "In kubernetes mode you must set the following configs in the `kubernetes` section: "
                "`dags_volume_claim` or "
                "`git_repo and git_branch` "
            )


class KubernetesJobWatcher(multiprocessing.Process, LoggingMixin, object):
    def __init__(self, namespace, watcher_queue, resource_version):
        multiprocessing.Process.__init__(self)
        self.namespace = namespace
        self.watcher_queue = watcher_queue
        self.resource_version = resource_version

    def run(self):
        kube_client = get_kube_client()
        while True:
            try:
                self.resource_version = self._run(kube_client, self.resource_version)
            except Exception:
                self.log.exception("Unknown error in KubernetesJobWatcher. Failing")
                raise
            else:
                self.log.warn("Watch died gracefully, starting back up with: "
                              "last resource_version: {}".format(self.resource_version))

    def _run(self, kube_client, resource_version):
        self.log.info("Event: and now my watch begins starting at resource_version: {}".format(resource_version))
        watcher = watch.Watch()

        kwargs = {"label_selector": "airflow-slave"}
        if resource_version:
            kwargs["resource_version"] = resource_version

        last_resource_version = None
        for event in watcher.stream(kube_client.list_namespaced_pod, self.namespace, **kwargs):
            task = event['object']
            self.log.info("Event: {} had an event of type {}".format(task.metadata.name, event['type']))
            self.process_status(
                task.metadata.name, task.status.phase, task.metadata.labels, task.metadata.resource_version
            )
            last_resource_version = task.metadata.resource_version

        return last_resource_version

    def process_status(self, pod_id, status, labels, resource_version):
        if status == 'Pending':
            self.log.info("Event: {} Pending".format(pod_id))
        elif status == 'Failed':
            self.log.info("Event: {} Failed".format(pod_id))
            self.watcher_queue.put((pod_id, State.FAILED, labels, resource_version))
        elif status == 'Succeeded':
            self.log.info("Event: {} Succeeded".format(pod_id))
            self.watcher_queue.put((pod_id, None, labels, resource_version))
        elif status == 'Running':
            self.log.info("Event: {} is Running".format(pod_id))
        else:
            self.log.warn("Event: Invalid state: {} on pod: {} with labels: {} "
                             "with resource_version: {}".format(status, pod_id, labels, resource_version))


class AirflowKubernetesScheduler(LoggingMixin, object):
    def __init__(self, kube_config, task_queue, result_queue, session, kube_client):
        self.log.debug("creating kubernetes executor")
        self.kube_config = kube_config
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.namespace = self.kube_config.kube_namespace
        self.log.debug("k8s: using namespace {}".format(self.namespace))
        self.kube_client = kube_client
        self.launcher = PodLauncher(kube_client=self.kube_client)
        self.worker_configuration = WorkerConfiguration(kube_config=self.kube_config)
        self.watcher_queue = multiprocessing.Queue()
        self._session = session
        self.kube_watcher = self._make_kube_watcher()

    def _make_kube_watcher(self):
        resource_version = KubeResourceVersion.get_current_resource_version(self._session)
        watcher = KubernetesJobWatcher(self.namespace, self.watcher_queue, resource_version)
        watcher.start()
        return watcher

    def _health_check_kube_watcher(self):
        if self.kube_watcher.is_alive():
            pass
        else:
            self.log.error("Error while health checking kube watcher process. Process died for unknown reasons")
            self.kube_watcher = self._make_kube_watcher()

    def run_next(self, next_job):
        """

        The run_next command will check the task_queue for any un-run jobs.
        It will then create a unique job-id, launch that job in the cluster,
        and store relevent info in the current_jobs map so we can track the job's
        status

        :return: 

        """
        self.log.debug('k8s: job is {}'.format(str(next_job)))
        key, command = next_job
        dag_id, task_id, execution_date = key
        self.log.debug("k8s: running for command {}".format(command))
        self.log.debug("k8s: launching image {}".format(self.kube_config.kube_image))
        pod = self.worker_configuration.make_pod(
            namespace=self.namespace, pod_id=self._create_pod_id(dag_id, task_id),
            dag_id=dag_id, task_id=task_id, execution_date=self._datetime_to_label_safe_datestring(execution_date),
            airflow_command=command
        )
        # the watcher will monitor pods, so we do not block.
        self.launcher.run_pod_async(pod)
        self.log.debug("k8s: Job created!")

    def delete_pod(self, pod_id):
        if self.kube_config.delete_worker_pods:
            try:
                self.kube_client.delete_namespaced_pod(pod_id, self.namespace, body=client.V1DeleteOptions())
            except ApiException as e:
                if e.status != 404:
                    raise

    def sync(self):
        """

        The sync function checks the status of all currently running kubernetes jobs.
        If a job is completed, it's status is placed in the result queue to 
        be sent back to the scheduler.

        :return:

        """
        self._health_check_kube_watcher()
        while not self.watcher_queue.empty():
            self.process_watcher_task()

    def process_watcher_task(self):
        pod_id, state, labels, resource_version = self.watcher_queue.get()
        self.log.info("Attempting to finish pod; pod_id: {}; state: {}; labels: {}".format(pod_id, state, labels))
        key = self._labels_to_key(labels)
        if key:
            self.log.debug("finishing job {} - {} ({})".format(key, state, pod_id))
            self.result_queue.put((key, state, pod_id, resource_version))

    @staticmethod
    def _strip_unsafe_kubernetes_special_chars(string):
        """
        Kubernetes only supports lowercase alphanumeric characters and "-" and "." in the pod name
        However, there are special rules about how "-" and "." can be used so let's only keep alphanumeric chars
        see here for detail: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
        :param string:
        :return:
        """
        return ''.join(ch.lower() for ind, ch in enumerate(string) if ch.isalnum())

    @staticmethod
    def _make_safe_pod_id(safe_dag_id, safe_task_id, safe_uuid):
        """
        Kubernetes pod names must be <= 253 chars and must pass the following regex for validation
        "^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
        :param safe_dag_id: a dag_id with only alphanumeric characters
        :param safe_task_id: a task_id with only alphanumeric characters
        :param random_uuid: a uuid
        :return:
        """
        MAX_POD_ID_LEN = 253

        safe_key = safe_dag_id + safe_task_id

        safe_pod_id = safe_key[:MAX_POD_ID_LEN-len(safe_uuid)-1] + "-" + safe_uuid

        return safe_pod_id

    @staticmethod
    def _create_pod_id(dag_id, task_id):
        safe_dag_id = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(dag_id)
        safe_task_id = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(task_id)
        safe_uuid = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(uuid4().hex)
        return AirflowKubernetesScheduler._make_safe_pod_id(safe_dag_id, safe_task_id, safe_uuid)

    @staticmethod
    def _label_safe_datestring_to_datetime(string):
        """
        Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but not "_" let's replace ":" with "_"
        :param string: string
        :return: datetime.datetime object
        """
        return parser.parse(string.replace("_", ":"))

    @staticmethod
    def _datetime_to_label_safe_datestring(datetime_obj):
        """
        Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but not "_" let's replace ":" with "_"
        :param datetime_obj: datetime.datetime object
        :return: ISO-like string representing the datetime
        """
        return datetime_obj.isoformat().replace(":", "_")

    def _labels_to_key(self, labels):
        try:
            return labels["dag_id"], labels["task_id"], self._label_safe_datestring_to_datetime(labels["execution_date"])
        except Exception as e:
            self.log.warn("Error while converting labels to key; labels: {}; exception: {}".format(
                labels, e
            ))
            return None


class KubernetesExecutor(BaseExecutor, LoggingMixin):
    def __init__(self):
        super(KubernetesExecutor, self).__init__(parallelism=PARALLELISM)
        self.task_queue = None
        self._session = None
        self.result_queue = None
        self.kube_scheduler = None
        self.kube_client = None
        super(KubernetesExecutor, self).__init__(parallelism=self.kube_config.parallelism)

    def clear_not_launched_queued_tasks(self):
        """
        If the airflow scheduler restarts with pending "Queued" tasks, the tasks may or may not have been launched
        Thus, on starting up the scheduler let's check every "Queued" task to see if it has been launched
            (ie: if there is a corresponding pod on kubernetes)
        If it has been launched then do nothing, otherwise reset the state to "None" so the task will be rescheduled
        This will not be necessary in a future version of airflow in which there is proper support for State.LAUNCHED
        :return: None
        """
        queued_tasks = self._session.query(TaskInstance).filter(TaskInstance.state == State.QUEUED).all()
        self.log.info("When executor started up, found {} queued task instances".format(len(queued_tasks)))

        for t in queued_tasks:
            kwargs = dict(label_selector="dag_id={},task_id={},execution_date={}".format(
                t.dag_id, t.task_id, AirflowKubernetesScheduler._datetime_to_label_safe_datestring(t.execution_date)
            ))
            pod_list = self.kube_client.list_namespaced_pod(self.kube_config.kube_namespace, **kwargs)
            if len(pod_list.items) == 0:
                self.log.info("TaskInstance: {} found in queued state but was not launched, rescheduling".format(t))
                self._session.query(TaskInstance).filter(
                    TaskInstance.dag_id == t.dag_id,
                    TaskInstance.task_id == t.task_id,
                    TaskInstance.execution_date == t.execution_date
                ).update({TaskInstance.state: State.NONE})

        self._session.commit()

    def _inject_secrets(self):
        def _create_or_update_secret(secret_name, secret_path):
            try:
                return self.kube_client.create_namespaced_secret(
                    self.kube_config.executor_namespace, kubernetes.client.V1Secret(
                        data={'key.json' : base64.b64encode(open(secret_path, 'r').read())},
                        metadata=kubernetes.client.V1ObjectMeta(name=secret_name)))
            except ApiException as e:
                if e.status == 409:
                    return self.kube_client.replace_namespaced_secret(
                        secret_name, self.kube_config.executor_namespace,
                        kubernetes.client.V1Secret(
                            data={'key.json' : base64.b64encode(open(secret_path, 'r').read())},
                            metadata=kubernetes.client.V1ObjectMeta(name=secret_name)))
                self.log.exception("Exception while trying to inject secret."
                                      "Secret name: {}, error details: {}.".format(secret_name, e))
                raise

        # For each GCP service account key, inject it as a secret in executor
        # namespace with the specific secret name configured in the airflow.cfg.
        # We let exceptions to pass through to users.
        if self.kube_config.gcp_service_account_keys:
            name_path_pair_list = [
                {'name' : account_spec.strip().split('=')[0],
                 'path' : account_spec.strip().split('=')[1]}
                for account_spec in self.kube_config.gcp_service_account_keys.split(',')]
            for service_account in name_path_pair_list:
                _create_or_update_secret(service_account['name'], service_account['path'])

    def start(self):
        self.log.info('k8s: starting kubernetes executor')
        self._session = settings.Session()
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.kube_client = get_kube_client()
        self.kube_scheduler = AirflowKubernetesScheduler(
            self.kube_config, self.task_queue, self.result_queue, self._session, self.kube_client
        )
        self._inject_secrets()
        self.clear_not_launched_queued_tasks()

    def execute_async(self, key, command, queue=None):
        self.log.info("k8s: adding task {} with command {}".format(key, command))
        self.task_queue.put((key, command))

    def sync(self):
        self.log.info("self.running: {}".format(self.running))
        self.log.info("self.queued: {}".format(self.queued_tasks))
        self.kube_scheduler.sync()

        last_resource_version = None
        while not self.result_queue.empty():
            results = self.result_queue.get()
            key, state, pod_id, resource_version = results
            last_resource_version = resource_version
            self.log.info("Changing state of {}".format(results))
            self._change_state(key, state, pod_id)

        KubeResourceVersion.checkpoint_resource_version(last_resource_version, session=self._session)

        if not self.task_queue.empty():
            key, command = self.task_queue.get()
            self.kube_scheduler.run_next((key, command))

    def _change_state(self, key, state, pod_id):
        if state != State.RUNNING:
            self.kube_scheduler.delete_pod(pod_id)
            try:
                self.running.pop(key)
            except KeyError:
                pass
        self.event_buffer[key] = state
        (dag_id, task_id, ex_time) = key
        item = self._session.query(TaskInstance).filter_by(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=ex_time
        ).one()

        if item.state == State.RUNNING or item.state == State.QUEUED:
            item.state = state
            self._session.add(item)
            self._session.commit()

    def end(self):
        self.log.info('ending kube executor')
        self.task_queue.join()

    def execute_async(self, key, command, queue=None):
        self.logger.info("k8s: adding task {} with command {}".format(key, command))
        self.task_queue.put((key, command))
