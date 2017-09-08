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

import calendar
import logging
import os
import multiprocessing
from queue import Queue
from dateutil import parser
from uuid import uuid4
from kubernetes import watch, client
from kubernetes.client.rest import ApiException
from airflow.contrib.kubernetes.pod_launcher import PodLauncher
from airflow.contrib.kubernetes.kube_client import get_kube_client
from airflow.executors.base_executor import BaseExecutor
from airflow.models import TaskInstance, KubeResourceVersion
from airflow.utils.state import State
from airflow import configuration, settings
from airflow.exceptions import AirflowConfigException
from airflow.contrib.kubernetes.pod import Pod


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
        self.dags_folder = configuration.get(self.core_section, 'dags_folder')
        self.parallelism = configuration.getint(self.core_section, 'PARALLELISM')
        self.kube_image = configuration.get(self.kubernetes_section, 'container_image')
        self.delete_worker_pods = self.safe_getboolean(self.kubernetes_section, 'delete_worker_pods', True)
        self.kube_namespace = os.environ.get('AIRFLOW_KUBE_NAMESPACE', 'default')

        # These two props must be set together
        self.git_repo = self.safe_get(self.kubernetes_section, 'git_repo', None)
        self.git_branch = self.safe_get(self.kubernetes_section, 'git_branch', None)

        # Or this one prop
        self.dags_volume_claim = self.safe_get(self.kubernetes_section, 'dags_volume_claim', None)
        # And optionally this prop
        self.dags_volume_subpath = self.safe_get(self.kubernetes_section, 'dags_volume_subpath', None)

        self._validate()

    def _validate(self):
        if self.dags_volume_claim:
            # do volume things
            pass
        elif self.git_repo and self.git_branch:
            # do git things
            pass
        else:
            raise AirflowConfigException(
                "In kubernetes mode you must set the following configs in the `kubernetes` section: "
                "`dags_volume_claim` or "
                "`git_repo and git_branch`"
            )


class PodMaker:
    def __init__(self, kube_config):
        self.logger = logging.getLogger(__name__)
        self.kube_config = kube_config

    def _get_volumes_and_mounts(self):
        volume_name = "airflow-dags"

        if self.kube_config.dags_volume_claim:
            volumes = [{
                "name": volume_name, "persistentVolumeClaim": {"claimName": self.kube_config.dags_volume_claim}
            }]
            volume_mounts = [{
                "name": volume_name, "mountPath": self.kube_config.dags_folder,
                "readOnly": True
            }]
            if self.kube_config.dags_volume_subpath:
                volume_mounts[0]["subPath"] = self.kube_config.dags_volume_subpath

            return volumes, volume_mounts
        else:
            return [], []

    def _get_args(self, airflow_command):
        if self.kube_config.dags_volume_claim:
            self.logger.info("Using k8s_dags_volume_claim for airflow dags")
            return [airflow_command]
        else:
            self.logger.info("Using git-syncher for airflow dags")
            cmd_args = "mkdir -p {dags_folder} && cd {dags_folder} &&" \
                       "git init && git remote add origin {git_repo} && git pull origin {git_branch} --depth=1 &&" \
                       "{command}".format(dags_folder=self.kube_config.dags_folder, git_repo=self.kube_config.git_repo,
                                          git_branch=self.kube_config.git_branch, command=airflow_command)
            return [cmd_args]

    def make_pod(self, namespace, pod_id, dag_id, task_id, execution_date, airflow_command):
        volumes, volume_mounts = self._get_volumes_and_mounts()

        pod = Pod(
            namespace=namespace,
            name=pod_id,
            image=self.kube_config.kube_image,
            cmds=["bash", "-cx", "--"],
            args=self._get_args(airflow_command),
            labels={
                "airflow-slave": "",
                "dag_id": dag_id,
                "task_id": task_id,
                "execution_date": execution_date
            },
            envs={"AIRFLOW__CORE__EXECUTOR": "LocalExecutor"},
            volumes=volumes,
            volume_mounts=volume_mounts
        )
        return pod


class KubernetesJobWatcher(multiprocessing.Process, object):
    def __init__(self, namespace, watcher_queue, resource_version):
        self.logger = logging.getLogger(__name__)
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
                self.logger.exception("Unknown error in KubernetesJobWatcher. Failing")
                raise
            else:
                self.logger.warn("Watch died gracefully, starting back up with: "
                                 "last resource_version: {}".format(self.resource_version))

    def _run(self, kube_client, resource_version):
        self.logger.info("Event: and now my watch begins starting at resource_version: {}".format(resource_version))
        watcher = watch.Watch()

        kwargs = {"label_selector": "airflow-slave"}
        if resource_version:
            kwargs["resource_version"] = resource_version

        last_resource_version = None
        for event in watcher.stream(kube_client.list_namespaced_pod, self.namespace, **kwargs):
            task = event['object']
            self.logger.info("Event: {} had an event of type {}".format(task.metadata.name, event['type']))
            self.process_status(
                task.metadata.name, task.status.phase, task.metadata.labels, task.metadata.resource_version
            )
            last_resource_version = task.metadata.resource_version

        return last_resource_version

    def process_status(self, pod_id, status, labels, resource_version):
        if status == 'Pending':
            self.logger.info("Event: {} Pending".format(pod_id))
        elif status == 'Failed':
            self.logger.info("Event: {} Failed".format(pod_id))
            self.watcher_queue.put((pod_id, State.FAILED, labels, resource_version))
        elif status == 'Succeeded':
            self.logger.info("Event: {} Succeeded".format(pod_id))
            self.watcher_queue.put((pod_id, None, labels, resource_version))
        elif status == 'Running':
            self.logger.info("Event: {} is Running".format(pod_id))
        else:
            self.logger.warn("Event: Invalid state: {} on pod: {} with labels: {} "
                             "with resource_version: {}".format(status, pod_id, labels, resource_version))


class AirflowKubernetesScheduler(object):
    def __init__(self, kube_config, task_queue, result_queue, session, kube_client):
        self.logger = logging.getLogger(__name__)
        self.logger.info("creating kubernetes executor")
        self.kube_config = KubeConfig()
        self.task_queue = task_queue
        self.pending_jobs = set()
        self.namespace = os.environ['k8s_POD_NAMESPACE']
        self.logger.info("k8s: using namespace {}".format(self.namespace))
        self.kube_client = kube_client
        self.launcher = PodLauncher(kube_client=self.kube_client)
        self.pod_maker = PodMaker(kube_config=self.kube_config)
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
            self.logger.error("Error while health checking kube watcher process. Process died for unknown reasons")
            self.kube_watcher = self._make_kube_watcher()

    def run_next(self, next_job):
        """

        The run_next command will check the task_queue for any un-run jobs.
        It will then create a unique job-id, launch that job in the cluster,
        and store relevent info in the current_jobs map so we can track the job's
        status

        :return: 

        """
        self.logger.info('k8s: job is {}'.format(str(next_job)))
        key, command = next_job
        dag_id, task_id, execution_date = key
        self.logger.info("k8s: running for command {}".format(command))
        self.logger.info("k8s: launching image {}".format(self.kube_config.kube_image))
        pod = self.pod_maker.make_pod(
            namespace=self.namespace, pod_id=self._create_pod_id(dag_id, task_id),
            dag_id=dag_id, task_id=task_id, execution_date=self._datetime_to_label_safe_datestring(execution_date),
            airflow_command=command
        )
        # the watcher will monitor pods, so we do not block.
        self.launcher.run_pod_async(pod)
        self.logger.info("k8s: Job created!")

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
        logging.info("Attempting to finish pod; pod_id: {}; state: {}; labels: {}".format(pod_id, state, labels))
        key = self._labels_to_key(labels)
        if key:
            self.logger.info("finishing job {}".format(key))
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
            self.logger.warn("Error while converting labels to key; labels: {}; exception: {}".format(
                labels, e
            ))
            return None


class KubernetesExecutor(BaseExecutor):
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
        self.logger.info("When executor started up, found {} queued task instances".format(len(queued_tasks)))

        for t in queued_tasks:
            kwargs = dict(label_selector="dag_id={},task_id={},execution_date={}".format(
                t.dag_id, t.task_id, AirflowKubernetesScheduler._datetime_to_label_safe_datestring(t.execution_date)
            ))
            pod_list = self.kube_client.list_namespaced_pod(self.kube_config.kube_namespace, **kwargs)
            if len(pod_list.items) == 0:
                self.logger.info("TaskInstance: {} found in queued state but was not launched, rescheduling".format(t))
                self._session.query(TaskInstance).filter(
                    TaskInstance.dag_id == t.dag_id,
                    TaskInstance.task_id == t.task_id,
                    TaskInstance.execution_date == t.execution_date
                ).update({TaskInstance.state: State.NONE})

        self._session.commit()

    def start(self):
        self.logger.info('k8s: starting kubernetes executor')
        self._session = settings.Session()
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.kube_client = get_kube_client()
        self.kube_scheduler = AirflowKubernetesScheduler(
            self.kube_config, self.task_queue, self.result_queue, self._session, self.kube_client
        )
        self.clear_not_launched_queued_tasks()

    def execute_async(self, key, command, queue=None):
        self.logger.info("k8s: adding task {} with command {}".format(key, command))
        self.task_queue.put((key, command))

    def sync(self):
        self.logger.info("self.running: {}".format(self.running))
        self.logger.info("self.queued: {}".format(self.queued_tasks))
        self.kube_scheduler.sync()

        last_resource_version = None
        while not self.result_queue.empty():
            results = self.result_queue.get()
            key, state, pod_id, resource_version = results
            last_resource_version = resource_version
            self.logger.info("Changing state of {}".format(results))
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
        self.logger.info('ending kube executor')
        self.task_queue.join()

    def execute_async(self, key, command, queue=None):
        self.logger.info("k8s: adding task {} with command {}".format(key, command))
        self.task_queue.put((key, command))
