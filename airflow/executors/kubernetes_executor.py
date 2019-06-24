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

import base64
import hashlib
from queue import Empty

import re
import json
import multiprocessing
from dateutil import parser
from uuid import uuid4
import kubernetes
from kubernetes import watch, client
from kubernetes.client.rest import ApiException
from airflow.configuration import conf
from airflow.kubernetes.pod_launcher import PodLauncher
from airflow.kubernetes.kube_client import get_kube_client
from airflow.kubernetes.worker_configuration import WorkerConfiguration
from airflow.executors.base_executor import BaseExecutor
from airflow.executors import Executors
from airflow.models import KubeResourceVersion, KubeWorkerIdentifier, TaskInstance
from airflow.utils.state import State
from airflow.utils.db import provide_session, create_session
from airflow import configuration, settings
from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin


class KubernetesExecutorConfig:
    def __init__(self, image=None, image_pull_policy=None, request_memory=None,
                 request_cpu=None, limit_memory=None, limit_cpu=None,
                 gcp_service_account_key=None, node_selectors=None, affinity=None,
                 annotations=None, volumes=None, volume_mounts=None, tolerations=None, labels={}):
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.request_memory = request_memory
        self.request_cpu = request_cpu
        self.limit_memory = limit_memory
        self.limit_cpu = limit_cpu
        self.gcp_service_account_key = gcp_service_account_key
        self.node_selectors = node_selectors
        self.affinity = affinity
        self.annotations = annotations
        self.volumes = volumes
        self.volume_mounts = volume_mounts
        self.tolerations = tolerations
        self.labels = labels

    def __repr__(self):
        return "{}(image={}, image_pull_policy={}, request_memory={}, request_cpu={}, " \
               "limit_memory={}, limit_cpu={}, gcp_service_account_key={}, " \
               "node_selectors={}, affinity={}, annotations={}, volumes={}, " \
               "volume_mounts={}, tolerations={}, labels={})" \
            .format(KubernetesExecutorConfig.__name__, self.image, self.image_pull_policy,
                    self.request_memory, self.request_cpu, self.limit_memory,
                    self.limit_cpu, self.gcp_service_account_key, self.node_selectors,
                    self.affinity, self.annotations, self.volumes, self.volume_mounts,
                    self.tolerations, self.labels)

    @staticmethod
    def from_dict(obj):
        if obj is None:
            return KubernetesExecutorConfig()

        if not isinstance(obj, dict):
            raise TypeError(
                'Cannot convert a non-dictionary object into a KubernetesExecutorConfig')

        namespaced = obj.get(Executors.KubernetesExecutor, {})

        return KubernetesExecutorConfig(
            image=namespaced.get('image', None),
            image_pull_policy=namespaced.get('image_pull_policy', None),
            request_memory=namespaced.get('request_memory', None),
            request_cpu=namespaced.get('request_cpu', None),
            limit_memory=namespaced.get('limit_memory', None),
            limit_cpu=namespaced.get('limit_cpu', None),
            gcp_service_account_key=namespaced.get('gcp_service_account_key', None),
            node_selectors=namespaced.get('node_selectors', None),
            affinity=namespaced.get('affinity', None),
            annotations=namespaced.get('annotations', {}),
            volumes=namespaced.get('volumes', []),
            volume_mounts=namespaced.get('volume_mounts', []),
            tolerations=namespaced.get('tolerations', None),
            labels=namespaced.get('labels', {}),
        )

    def as_dict(self):
        return {
            'image': self.image,
            'image_pull_policy': self.image_pull_policy,
            'request_memory': self.request_memory,
            'request_cpu': self.request_cpu,
            'limit_memory': self.limit_memory,
            'limit_cpu': self.limit_cpu,
            'gcp_service_account_key': self.gcp_service_account_key,
            'node_selectors': self.node_selectors,
            'affinity': self.affinity,
            'annotations': self.annotations,
            'volumes': self.volumes,
            'volume_mounts': self.volume_mounts,
            'tolerations': self.tolerations,
            'labels': self.labels,
        }


class KubeConfig:
    core_section = 'core'
    kubernetes_section = 'kubernetes'

    def __init__(self):
        configuration_dict = configuration.as_dict(display_sensitive=True)
        self.core_configuration = configuration_dict['core']
        self.kube_secrets = configuration_dict.get('kubernetes_secrets', {})
        self.kube_env_vars = configuration_dict.get('kubernetes_environment_variables', {})
        self.env_from_configmap_ref = configuration.get(self.kubernetes_section,
                                                        'env_from_configmap_ref')
        self.env_from_secret_ref = configuration.get(self.kubernetes_section,
                                                     'env_from_secret_ref')
        self.airflow_home = settings.AIRFLOW_HOME
        self.dags_folder = configuration.get(self.core_section, 'dags_folder')
        self.parallelism = configuration.getint(self.core_section, 'PARALLELISM')
        self.worker_container_repository = configuration.get(
            self.kubernetes_section, 'worker_container_repository')
        self.worker_container_tag = configuration.get(
            self.kubernetes_section, 'worker_container_tag')
        self.kube_image = '{}:{}'.format(
            self.worker_container_repository, self.worker_container_tag)
        self.kube_image_pull_policy = configuration.get(
            self.kubernetes_section, "worker_container_image_pull_policy"
        )
        self.kube_node_selectors = configuration_dict.get('kubernetes_node_selectors', {})
        self.kube_annotations = configuration_dict.get('kubernetes_annotations', {})
        self.kube_labels = configuration_dict.get('kubernetes_labels', {})
        self.delete_worker_pods = conf.getboolean(
            self.kubernetes_section, 'delete_worker_pods')
        self.worker_pods_creation_batch_size = conf.getint(
            self.kubernetes_section, 'worker_pods_creation_batch_size')
        self.worker_service_account_name = conf.get(
            self.kubernetes_section, 'worker_service_account_name')
        self.image_pull_secrets = conf.get(self.kubernetes_section, 'image_pull_secrets')

        # NOTE: user can build the dags into the docker image directly,
        # this will set to True if so
        self.dags_in_image = conf.getboolean(self.kubernetes_section, 'dags_in_image')

        # Run as user for pod security context
        self.worker_run_as_user = conf.get(self.kubernetes_section, 'run_as_user')
        self.worker_fs_group = conf.get(self.kubernetes_section, 'fs_group')

        # NOTE: `git_repo` and `git_branch` must be specified together as a pair
        # The http URL of the git repository to clone from
        self.git_repo = conf.get(self.kubernetes_section, 'git_repo')
        # The branch of the repository to be checked out
        self.git_branch = conf.get(self.kubernetes_section, 'git_branch')
        # Optionally, the directory in the git repository containing the dags
        self.git_subpath = conf.get(self.kubernetes_section, 'git_subpath')
        # Optionally, the root directory for git operations
        self.git_sync_root = conf.get(self.kubernetes_section, 'git_sync_root')
        # Optionally, the name at which to publish the checked-out files under --root
        self.git_sync_dest = conf.get(self.kubernetes_section, 'git_sync_dest')
        # Optionally, if git_dags_folder_mount_point is set the worker will use
        # {git_dags_folder_mount_point}/{git_sync_dest}/{git_subpath} as dags_folder
        self.git_dags_folder_mount_point = conf.get(self.kubernetes_section,
                                                    'git_dags_folder_mount_point')

        # Optionally a user may supply a (`git_user` AND `git_password`) OR
        # (`git_ssh_key_secret_name` AND `git_ssh_key_secret_key`) for private repositories
        self.git_user = conf.get(self.kubernetes_section, 'git_user')
        self.git_password = conf.get(self.kubernetes_section, 'git_password')
        self.git_ssh_key_secret_name = conf.get(self.kubernetes_section, 'git_ssh_key_secret_name')
        self.git_ssh_known_hosts_configmap_name = conf.get(self.kubernetes_section,
                                                           'git_ssh_known_hosts_configmap_name')

        # NOTE: The user may optionally use a volume claim to mount a PV containing
        # DAGs directly
        self.dags_volume_claim = conf.get(self.kubernetes_section, 'dags_volume_claim')

        # This prop may optionally be set for PV Claims and is used to write logs
        self.logs_volume_claim = conf.get(self.kubernetes_section, 'logs_volume_claim')

        # This prop may optionally be set for PV Claims and is used to locate DAGs
        # on a SubPath
        self.dags_volume_subpath = conf.get(
            self.kubernetes_section, 'dags_volume_subpath')

        # This prop may optionally be set for PV Claims and is used to locate logs
        # on a SubPath
        self.logs_volume_subpath = conf.get(
            self.kubernetes_section, 'logs_volume_subpath')

        # Optionally, hostPath volume containing DAGs
        self.dags_volume_host = conf.get(self.kubernetes_section, 'dags_volume_host')

        # Optionally, write logs to a hostPath Volume
        self.logs_volume_host = conf.get(self.kubernetes_section, 'logs_volume_host')

        # This prop may optionally be set for PV Claims and is used to write logs
        self.base_log_folder = configuration.get(self.core_section, 'base_log_folder')

        # The Kubernetes Namespace in which the Scheduler and Webserver reside. Note
        # that if your
        # cluster has RBAC enabled, your scheduler may need service account permissions to
        # create, watch, get, and delete pods in this namespace.
        self.kube_namespace = conf.get(self.kubernetes_section, 'namespace')
        # The Kubernetes Namespace in which pods will be created by the executor. Note
        # that if your
        # cluster has RBAC enabled, your workers may need service account permissions to
        # interact with cluster components.
        self.executor_namespace = conf.get(self.kubernetes_section, 'namespace')
        # Task secrets managed by KubernetesExecutor.
        self.gcp_service_account_keys = conf.get(self.kubernetes_section,
                                                 'gcp_service_account_keys')

        # If the user is using the git-sync container to clone their repository via git,
        # allow them to specify repository, tag, and pod name for the init container.
        self.git_sync_container_repository = conf.get(
            self.kubernetes_section, 'git_sync_container_repository')

        self.git_sync_container_tag = conf.get(
            self.kubernetes_section, 'git_sync_container_tag')
        self.git_sync_container = '{}:{}'.format(
            self.git_sync_container_repository, self.git_sync_container_tag)

        self.git_sync_init_container_name = conf.get(
            self.kubernetes_section, 'git_sync_init_container_name')

        # The worker pod may optionally have a  valid Airflow config loaded via a
        # configmap
        self.airflow_configmap = conf.get(self.kubernetes_section, 'airflow_configmap')

        affinity_json = conf.get(self.kubernetes_section, 'affinity')
        if affinity_json:
            self.kube_affinity = json.loads(affinity_json)
        else:
            self.kube_affinity = None

        tolerations_json = conf.get(self.kubernetes_section, 'tolerations')
        if tolerations_json:
            self.kube_tolerations = json.loads(tolerations_json)
        else:
            self.kube_tolerations = None

        kube_client_request_args = conf.get(self.kubernetes_section, 'kube_client_request_args')
        if kube_client_request_args:
            self.kube_client_request_args = json.loads(kube_client_request_args)
            if self.kube_client_request_args['_request_timeout'] and \
                    isinstance(self.kube_client_request_args['_request_timeout'], list):
                self.kube_client_request_args['_request_timeout'] = \
                    tuple(self.kube_client_request_args['_request_timeout'])
        else:
            self.kube_client_request_args = {}
        self._validate()

    def _validate(self):
        # TODO: use XOR for dags_volume_claim and git_dags_folder_mount_point
        if not self.dags_volume_claim \
           and not self.dags_volume_host \
           and not self.dags_in_image \
           and (not self.git_repo or not self.git_branch or not self.git_dags_folder_mount_point):
            raise AirflowConfigException(
                'In kubernetes mode the following must be set in the `kubernetes` '
                'config section: `dags_volume_claim` '
                'or `dags_volume_host` '
                'or `dags_in_image` '
                'or `git_repo and git_branch and git_dags_folder_mount_point`')
        if self.git_repo \
           and (self.git_user or self.git_password) \
           and self.git_ssh_key_secret_name:
            raise AirflowConfigException(
                'In kubernetes mode, using `git_repo` to pull the DAGs: '
                'for private repositories, either `git_user` and `git_password` '
                'must be set for authentication through user credentials; '
                'or `git_ssh_key_secret_name` must be set for authentication '
                'through ssh key, but not both')


class KubernetesJobWatcher(multiprocessing.Process, LoggingMixin, object):
    def __init__(self, namespace, watcher_queue, resource_version, worker_uuid, kube_config):
        multiprocessing.Process.__init__(self)
        self.namespace = namespace
        self.worker_uuid = worker_uuid
        self.watcher_queue = watcher_queue
        self.resource_version = resource_version
        self.kube_config = kube_config

    def run(self):
        kube_client = get_kube_client()
        while True:
            try:
                self.resource_version = self._run(kube_client, self.resource_version,
                                                  self.worker_uuid, self.kube_config)
            except Exception:
                self.log.exception('Unknown error in KubernetesJobWatcher. Failing')
                raise
            else:
                self.log.warn('Watch died gracefully, starting back up with: '
                              'last resource_version: %s', self.resource_version)

    def _run(self, kube_client, resource_version, worker_uuid, kube_config):
        self.log.info(
            'Event: and now my watch begins starting at resource_version: %s',
            resource_version
        )
        watcher = watch.Watch()

        kwargs = {'label_selector': 'airflow-worker={}'.format(worker_uuid)}
        if resource_version:
            kwargs['resource_version'] = resource_version
        if kube_config.kube_client_request_args:
            for key, value in kube_config.kube_client_request_args.iteritems():
                kwargs[key] = value

        last_resource_version = None
        for event in watcher.stream(kube_client.list_namespaced_pod, self.namespace,
                                    **kwargs):
            task = event['object']
            self.log.info(
                'Event: %s had an event of type %s',
                task.metadata.name, event['type']
            )
            if event['type'] == 'ERROR':
                return self.process_error(event)
            self.process_status(
                task.metadata.name, task.status.phase, task.metadata.labels,
                task.metadata.resource_version
            )
            last_resource_version = task.metadata.resource_version

        return last_resource_version

    def process_error(self, event):
        self.log.error(
            'Encountered Error response from k8s list namespaced pod stream => %s',
            event
        )
        raw_object = event['raw_object']
        if raw_object['code'] == 410:
            self.log.info(
                'Kubernetes resource version is too old, must reset to 0 => %s',
                raw_object['message']
            )
            # Return resource version 0
            return '0'
        raise AirflowException(
            'Kubernetes failure for %s with code %s and message: %s',
            raw_object['reason'], raw_object['code'], raw_object['message']
        )

    def process_status(self, pod_id, status, labels, resource_version):
        if status == 'Pending':
            self.log.info('Event: %s Pending', pod_id)
        elif status == 'Failed':
            self.log.info('Event: %s Failed', pod_id)
            self.watcher_queue.put((pod_id, State.FAILED, labels, resource_version))
        elif status == 'Succeeded':
            self.log.info('Event: %s Succeeded', pod_id)
            self.watcher_queue.put((pod_id, None, labels, resource_version))
        elif status == 'Running':
            self.log.info('Event: %s is Running', pod_id)
        else:
            self.log.warn(
                'Event: Invalid state: %s on pod: %s with labels: %s with '
                'resource_version: %s', status, pod_id, labels, resource_version
            )


class AirflowKubernetesScheduler(LoggingMixin):
    def __init__(self, kube_config, task_queue, result_queue, kube_client, worker_uuid):
        self.log.debug("Creating Kubernetes executor")
        self.kube_config = kube_config
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.namespace = self.kube_config.kube_namespace
        self.log.debug("Kubernetes using namespace %s", self.namespace)
        self.kube_client = kube_client
        self.launcher = PodLauncher(kube_client=self.kube_client)
        self.worker_configuration = WorkerConfiguration(kube_config=self.kube_config)
        self._manager = multiprocessing.Manager()
        self.watcher_queue = self._manager.Queue()
        self.worker_uuid = worker_uuid
        self.kube_watcher = self._make_kube_watcher()

    def _make_kube_watcher(self):
        resource_version = KubeResourceVersion.get_current_resource_version()
        watcher = KubernetesJobWatcher(self.namespace, self.watcher_queue,
                                       resource_version, self.worker_uuid, self.kube_config)
        watcher.start()
        return watcher

    def _health_check_kube_watcher(self):
        if self.kube_watcher.is_alive():
            pass
        else:
            self.log.error(
                'Error while health checking kube watcher process. '
                'Process died for unknown reasons')
            self.kube_watcher = self._make_kube_watcher()

    def run_next(self, next_job):
        """

        The run_next command will check the task_queue for any un-run jobs.
        It will then create a unique job-id, launch that job in the cluster,
        and store relevant info in the current_jobs map so we can track the job's
        status
        """
        self.log.info('Kubernetes job is %s', str(next_job))
        key, command, kube_executor_config = next_job
        dag_id, task_id, execution_date, try_number = key
        self.log.debug("Kubernetes running for command %s", command)
        self.log.debug("Kubernetes launching image %s", self.kube_config.kube_image)
        pod = self.worker_configuration.make_pod(
            namespace=self.namespace, worker_uuid=self.worker_uuid,
            pod_id=self._create_pod_id(dag_id, task_id),
            dag_id=self._make_safe_label_value(dag_id),
            task_id=self._make_safe_label_value(task_id),
            try_number=try_number,
            execution_date=self._datetime_to_label_safe_datestring(execution_date),
            airflow_command=command, kube_executor_config=kube_executor_config
        )
        # the watcher will monitor pods, so we do not block.
        self.launcher.run_pod_async(pod, **self.kube_config.kube_client_request_args)
        self.log.debug("Kubernetes Job created!")

    def delete_pod(self, pod_id):
        if self.kube_config.delete_worker_pods:
            try:
                self.kube_client.delete_namespaced_pod(
                    pod_id, self.namespace, body=client.V1DeleteOptions(),
                    **self.kube_config.kube_client_request_args)
            except ApiException as e:
                # If the pod is already deleted
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
        while True:
            try:
                task = self.watcher_queue.get_nowait()
                try:
                    self.process_watcher_task(task)
                finally:
                    self.watcher_queue.task_done()
            except Empty:
                break

    def process_watcher_task(self, task):
        pod_id, state, labels, resource_version = task
        self.log.info(
            'Attempting to finish pod; pod_id: %s; state: %s; labels: %s',
            pod_id, state, labels
        )
        key = self._labels_to_key(labels=labels)
        if key:
            self.log.debug('finishing job %s - %s (%s)', key, state, pod_id)
            self.result_queue.put((key, state, pod_id, resource_version))

    @staticmethod
    def _strip_unsafe_kubernetes_special_chars(string):
        """
        Kubernetes only supports lowercase alphanumeric characters and "-" and "." in
        the pod name
        However, there are special rules about how "-" and "." can be used so let's
        only keep
        alphanumeric chars  see here for detail:
        https://kubernetes.io/docs/concepts/overview/working-with-objects/names/

        :param string: The requested Pod name
        :return: ``str`` Pod name stripped of any unsafe characters
        """
        return ''.join(ch.lower() for ind, ch in enumerate(string) if ch.isalnum())

    @staticmethod
    def _make_safe_pod_id(safe_dag_id, safe_task_id, safe_uuid):
        """
        Kubernetes pod names must be <= 253 chars and must pass the following regex for
        validation
        "^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"

        :param safe_dag_id: a dag_id with only alphanumeric characters
        :param safe_task_id: a task_id with only alphanumeric characters
        :param random_uuid: a uuid
        :return: ``str`` valid Pod name of appropriate length
        """
        MAX_POD_ID_LEN = 253

        safe_key = safe_dag_id + safe_task_id

        safe_pod_id = safe_key[:MAX_POD_ID_LEN - len(safe_uuid) - 1] + "-" + safe_uuid

        return safe_pod_id

    @staticmethod
    def _make_safe_label_value(string):
        """
        Valid label values must be 63 characters or less and must be empty or begin and
        end with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),
        dots (.), and alphanumerics between.

        If the label value is then greater than 63 chars once made safe, or differs in any
        way from the original value sent to this function, then we need to truncate to
        53chars, and append it with a unique hash.
        """
        MAX_LABEL_LEN = 63

        safe_label = re.sub(r'^[^a-z0-9A-Z]*|[^a-zA-Z0-9_\-\.]|[^a-z0-9A-Z]*$', '', string)

        if len(safe_label) > MAX_LABEL_LEN or string != safe_label:
            safe_hash = hashlib.md5(string.encode()).hexdigest()[:9]
            safe_label = safe_label[:MAX_LABEL_LEN - len(safe_hash) - 1] + "-" + safe_hash

        return safe_label

    @staticmethod
    def _create_pod_id(dag_id, task_id):
        safe_dag_id = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(
            dag_id)
        safe_task_id = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(
            task_id)
        safe_uuid = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(
            uuid4().hex)
        return AirflowKubernetesScheduler._make_safe_pod_id(safe_dag_id, safe_task_id,
                                                            safe_uuid)

    @staticmethod
    def _label_safe_datestring_to_datetime(string):
        """
        Kubernetes doesn't permit ":" in labels. ISO datetime format uses ":" but not
        "_", let's
        replace ":" with "_"

        :param string: str
        :return: datetime.datetime object
        """
        return parser.parse(string.replace('_plus_', '+').replace("_", ":"))

    @staticmethod
    def _datetime_to_label_safe_datestring(datetime_obj):
        """
        Kubernetes doesn't like ":" in labels, since ISO datetime format uses ":" but
        not "_" let's
        replace ":" with "_"
        :param datetime_obj: datetime.datetime object
        :return: ISO-like string representing the datetime
        """
        return datetime_obj.isoformat().replace(":", "_").replace('+', '_plus_')

    def _labels_to_key(self, labels):
        try_num = 1
        try:
            try_num = int(labels.get('try_number', '1'))
        except ValueError:
            self.log.warn("could not get try_number as an int: %s", labels.get('try_number', '1'))

        try:
            dag_id = labels['dag_id']
            task_id = labels['task_id']
            ex_time = self._label_safe_datestring_to_datetime(labels['execution_date'])
        except Exception as e:
            self.log.warn(
                'Error while retrieving labels; labels: %s; exception: %s',
                labels, e
            )
            return None

        with create_session() as session:
            tasks = (
                session
                .query(TaskInstance)
                .filter_by(execution_date=ex_time).all()
            )
            self.log.info(
                'Checking %s task instances.',
                len(tasks)
            )
            for task in tasks:
                if (
                    self._make_safe_label_value(task.dag_id) == dag_id and
                    self._make_safe_label_value(task.task_id) == task_id and
                    task.execution_date == ex_time
                ):
                    self.log.info(
                        'Found matching task %s-%s (%s) with current state of %s',
                        task.dag_id, task.task_id, task.execution_date, task.state
                    )
                    dag_id = task.dag_id
                    task_id = task.task_id
                    return (dag_id, task_id, ex_time, try_num)
        self.log.warn(
            'Failed to find and match task details to a pod; labels: %s',
            labels
        )
        return None

    def terminate(self):
        self.watcher_queue.join()
        self._manager.shutdown()


class KubernetesExecutor(BaseExecutor, LoggingMixin):
    def __init__(self):
        self.kube_config = KubeConfig()
        self.task_queue = None
        self.result_queue = None
        self.kube_scheduler = None
        self.kube_client = None
        self.worker_uuid = None
        self._manager = multiprocessing.Manager()
        super().__init__(parallelism=self.kube_config.parallelism)

    @provide_session
    def clear_not_launched_queued_tasks(self, session=None):
        """
        If the airflow scheduler restarts with pending "Queued" tasks, the tasks may or
        may not
        have been launched Thus, on starting up the scheduler let's check every
        "Queued" task to
        see if it has been launched (ie: if there is a corresponding pod on kubernetes)

        If it has been launched then do nothing, otherwise reset the state to "None" so
        the task
        will be rescheduled

        This will not be necessary in a future version of airflow in which there is
        proper support
        for State.LAUNCHED
        """
        queued_tasks = session\
            .query(TaskInstance)\
            .filter(TaskInstance.state == State.QUEUED).all()
        self.log.info(
            'When executor started up, found %s queued task instances',
            len(queued_tasks)
        )

        for task in queued_tasks:
            dict_string = (
                "dag_id={},task_id={},execution_date={},airflow-worker={}".format(
                    AirflowKubernetesScheduler._make_safe_label_value(task.dag_id),
                    AirflowKubernetesScheduler._make_safe_label_value(task.task_id),
                    AirflowKubernetesScheduler._datetime_to_label_safe_datestring(
                        task.execution_date
                    ),
                    self.worker_uuid
                )
            )
            kwargs = dict(label_selector=dict_string)
            if self.kube_config.kube_client_request_args:
                for key, value in self.kube_config.kube_client_request_args.iteritems():
                    kwargs[key] = value
            pod_list = self.kube_client.list_namespaced_pod(
                self.kube_config.kube_namespace, **kwargs)
            if len(pod_list.items) == 0:
                self.log.info(
                    'TaskInstance: %s found in queued state but was not launched, '
                    'rescheduling', task
                )
                session.query(TaskInstance).filter(
                    TaskInstance.dag_id == task.dag_id,
                    TaskInstance.task_id == task.task_id,
                    TaskInstance.execution_date == task.execution_date
                ).update({TaskInstance.state: State.NONE})

    def _inject_secrets(self):
        def _create_or_update_secret(secret_name, secret_path):
            try:
                return self.kube_client.create_namespaced_secret(
                    self.kube_config.executor_namespace, kubernetes.client.V1Secret(
                        data={
                            'key.json': base64.b64encode(open(secret_path, 'r').read())},
                        metadata=kubernetes.client.V1ObjectMeta(name=secret_name)),
                    **self.kube_config.kube_client_request_args)
            except ApiException as e:
                if e.status == 409:
                    return self.kube_client.replace_namespaced_secret(
                        secret_name, self.kube_config.executor_namespace,
                        kubernetes.client.V1Secret(
                            data={'key.json': base64.b64encode(
                                open(secret_path, 'r').read())},
                            metadata=kubernetes.client.V1ObjectMeta(name=secret_name)),
                        **self.kube_config.kube_client_request_args)
                self.log.exception(
                    'Exception while trying to inject secret. '
                    'Secret name: %s, error details: %s',
                    secret_name, e
                )
                raise

        # For each GCP service account key, inject it as a secret in executor
        # namespace with the specific secret name configured in the airflow.cfg.
        # We let exceptions to pass through to users.
        if self.kube_config.gcp_service_account_keys:
            name_path_pair_list = [
                {'name': account_spec.strip().split('=')[0],
                 'path': account_spec.strip().split('=')[1]}
                for account_spec in self.kube_config.gcp_service_account_keys.split(',')]
            for service_account in name_path_pair_list:
                _create_or_update_secret(service_account['name'], service_account['path'])

    def start(self):
        self.log.info('Start Kubernetes executor')
        self.worker_uuid = KubeWorkerIdentifier.get_or_create_current_kube_worker_uuid()
        self.log.debug('Start with worker_uuid: %s', self.worker_uuid)
        # always need to reset resource version since we don't know
        # when we last started, note for behavior below
        # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs
        # /CoreV1Api.md#list_namespaced_pod
        KubeResourceVersion.reset_resource_version()
        self.task_queue = self._manager.Queue()
        self.result_queue = self._manager.Queue()
        self.kube_client = get_kube_client()
        self.kube_scheduler = AirflowKubernetesScheduler(
            self.kube_config, self.task_queue, self.result_queue,
            self.kube_client, self.worker_uuid
        )
        self._inject_secrets()
        self.clear_not_launched_queued_tasks()

    def execute_async(self, key, command, queue=None, executor_config=None):
        self.log.info(
            'Add task %s with command %s with executor_config %s',
            key, command, executor_config
        )
        kube_executor_config = KubernetesExecutorConfig.from_dict(executor_config)
        self.task_queue.put((key, command, kube_executor_config))

    def sync(self):
        if self.running:
            self.log.debug('self.running: %s', self.running)
        if self.queued_tasks:
            self.log.debug('self.queued: %s', self.queued_tasks)
        self.kube_scheduler.sync()

        last_resource_version = None
        while True:
            try:
                results = self.result_queue.get_nowait()
                try:
                    key, state, pod_id, resource_version = results
                    last_resource_version = resource_version
                    self.log.info('Changing state of %s to %s', results, state)
                    try:
                        self._change_state(key, state, pod_id)
                    except Exception as e:
                        self.log.exception('Exception: %s when attempting ' +
                                           'to change state of %s to %s, re-queueing.', e, results, state)
                        self.result_queue.put(results)
                finally:
                    self.result_queue.task_done()
            except Empty:
                break

        KubeResourceVersion.checkpoint_resource_version(last_resource_version)

        for _ in range(self.kube_config.worker_pods_creation_batch_size):
            try:
                task = self.task_queue.get_nowait()
                try:
                    self.kube_scheduler.run_next(task)
                except ApiException:
                    self.log.exception('ApiException when attempting to run task, re-queueing.')
                    self.task_queue.put(task)
                finally:
                    self.task_queue.task_done()
            except Empty:
                break

    def _change_state(self, key, state, pod_id):
        if state != State.RUNNING:
            self.kube_scheduler.delete_pod(pod_id)
            try:
                self.log.info('Deleted pod: %s', str(key))
                self.running.pop(key)
            except KeyError:
                self.log.debug('Could not find key: %s', str(key))
        self.event_buffer[key] = state

    def end(self):
        self.log.info('Shutting down Kubernetes executor')
        self.task_queue.join()
        self.result_queue.join()
        if self.kube_scheduler:
            self.kube_scheduler.terminate()
        self._manager.shutdown()
