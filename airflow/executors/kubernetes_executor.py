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
"""
KubernetesExecutor

.. seealso::
    For more information on how the KubernetesExecutor works, take a look at the guide:
    :ref:`executor:KubernetesExecutor`
"""
import base64
import functools
import json
import multiprocessing
import time
from queue import Empty

import kubernetes
from dateutil import parser
from kubernetes import watch, client
from kubernetes.client.rest import ApiException
from urllib3.exceptions import HTTPError, ReadTimeoutError

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.kubernetes import pod_generator
from airflow.kubernetes.kube_client import get_kube_client
from airflow.kubernetes.pod_generator import MAX_POD_ID_LEN
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.kubernetes.pod_launcher import PodLauncher
from airflow.kubernetes.worker_configuration import WorkerConfiguration
from airflow.models import KubeResourceVersion, KubeWorkerIdentifier, TaskInstance
from airflow.utils.db import provide_session, create_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State


class KubeConfig:
    """Configuration for Kubernetes"""
    core_section = 'core'
    kubernetes_section = 'kubernetes'

    def __init__(self):
        configuration_dict = conf.as_dict(display_sensitive=True)
        self.core_configuration = configuration_dict['core']
        self.kube_secrets = configuration_dict.get('kubernetes_secrets', {})
        self.kube_env_vars = configuration_dict.get('kubernetes_environment_variables', {})
        self.env_from_configmap_ref = conf.get(self.kubernetes_section,
                                               'env_from_configmap_ref')
        self.env_from_secret_ref = conf.get(self.kubernetes_section,
                                            'env_from_secret_ref')
        self.airflow_home = settings.AIRFLOW_HOME
        self.dags_folder = conf.get(self.core_section, 'dags_folder')
        self.parallelism = conf.getint(self.core_section, 'parallelism')
        self.worker_container_repository = conf.get(
            self.kubernetes_section, 'worker_container_repository')
        self.worker_container_tag = conf.get(
            self.kubernetes_section, 'worker_container_tag')
        self.kube_image = '{}:{}'.format(
            self.worker_container_repository, self.worker_container_tag)
        self.kube_image_pull_policy = conf.get(
            self.kubernetes_section, "worker_container_image_pull_policy"
        )
        self.kube_node_selectors = configuration_dict.get('kubernetes_node_selectors', {})
        self.kube_annotations = configuration_dict.get('kubernetes_annotations', {}) or None
        self.pod_template_file = conf.get(self.kubernetes_section, 'pod_template_file',
                                          fallback=None)

        self.kube_labels = configuration_dict.get('kubernetes_labels', {})
        self.delete_worker_pods = conf.getboolean(
            self.kubernetes_section, 'delete_worker_pods')
        self.delete_worker_pods_on_failure = conf.getboolean(
            self.kubernetes_section, 'delete_worker_pods_on_failure')
        self.worker_pods_creation_batch_size = conf.getint(
            self.kubernetes_section, 'worker_pods_creation_batch_size')
        self.worker_service_account_name = conf.get(
            self.kubernetes_section, 'worker_service_account_name')
        self.image_pull_secrets = conf.get(self.kubernetes_section, 'image_pull_secrets')

        # NOTE: user can build the dags into the docker image directly,
        # this will set to True if so
        self.dags_in_image = conf.getboolean(self.kubernetes_section, 'dags_in_image')

        # Run as user for pod security context
        self.worker_run_as_user = self._get_security_context_val('run_as_user')
        self.worker_fs_group = self._get_security_context_val('fs_group')

        # NOTE: `git_repo` and `git_branch` must be specified together as a pair
        # The http URL of the git repository to clone from
        self.git_repo = conf.get(self.kubernetes_section, 'git_repo')
        # The branch of the repository to be checked out
        self.git_branch = conf.get(self.kubernetes_section, 'git_branch')
        # Clone depth for git sync
        self.git_sync_depth = conf.get(self.kubernetes_section, 'git_sync_depth')
        # Optionally, the directory in the git repository containing the dags
        self.git_subpath = conf.get(self.kubernetes_section, 'git_subpath')
        # Optionally, the root directory for git operations
        self.git_sync_root = conf.get(self.kubernetes_section, 'git_sync_root')
        # Optionally, the name at which to publish the checked-out files under --root
        self.git_sync_dest = conf.get(self.kubernetes_section, 'git_sync_dest')
        # Optionally, the tag or hash to checkout
        self.git_sync_rev = conf.get(self.kubernetes_section, 'git_sync_rev')
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
        self.git_sync_credentials_secret = conf.get(self.kubernetes_section,
                                                    'git_sync_credentials_secret')

        # NOTE: The user may optionally use a volume claim to mount a PV containing
        # DAGs directly
        self.dags_volume_claim = conf.get(self.kubernetes_section, 'dags_volume_claim')

        self.dags_volume_mount_point = conf.get(self.kubernetes_section, 'dags_volume_mount_point')

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
        self.base_log_folder = conf.get(self.core_section, 'base_log_folder')

        # The Kubernetes Namespace in which the Scheduler and Webserver reside. Note
        # that if your
        # cluster has RBAC enabled, your scheduler may need service account permissions to
        # create, watch, get, and delete pods in this namespace.
        self.kube_namespace = conf.get(self.kubernetes_section, 'namespace')
        self.multi_namespace_mode = conf.getboolean(self.kubernetes_section, 'multi_namespace_mode')
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

        self.git_sync_run_as_user = self._get_security_context_val('git_sync_run_as_user')

        # The worker pod may optionally have a  valid Airflow config loaded via a
        # configmap
        self.airflow_configmap = conf.get(self.kubernetes_section, 'airflow_configmap')

        # The worker pod may optionally have a valid Airflow local settings loaded via a
        # configmap
        self.airflow_local_settings_configmap = conf.get(
            self.kubernetes_section, 'airflow_local_settings_configmap')

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

        delete_option_kwargs = conf.get(self.kubernetes_section, 'delete_option_kwargs')
        if delete_option_kwargs:
            self.delete_option_kwargs = json.loads(delete_option_kwargs)
        else:
            self.delete_option_kwargs = {}

    # pod security context items should return integers
    # and only return a blank string if contexts are not set.
    def _get_security_context_val(self, scontext):
        val = conf.get(self.kubernetes_section, scontext)
        if not val:
            return ""
        else:
            return int(val)

    def _validate(self):
        if self.pod_template_file:
            return
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


class KubernetesJobWatcher(multiprocessing.Process, LoggingMixin):
    """Watches for Kubernetes jobs"""

    def __init__(self,
                 namespace,
                 multi_namespace_mode,
                 watcher_queue,
                 resource_version,
                 worker_uuid,
                 kube_config):
        multiprocessing.Process.__init__(self)
        self.namespace = namespace
        self.multi_namespace_mode = multi_namespace_mode
        self.worker_uuid = worker_uuid
        self.watcher_queue = watcher_queue
        self.resource_version = resource_version
        self.kube_config = kube_config

    def run(self):
        """Performs watching"""
        kube_client = get_kube_client()
        while True:
            try:
                self.resource_version = self._run(kube_client, self.resource_version,
                                                  self.worker_uuid, self.kube_config)
            except ReadTimeoutError:
                self.log.warning("There was a timeout error accessing the Kube API. "
                                 "Retrying request.", exc_info=True)
                time.sleep(1)
            except Exception:
                self.log.exception('Unknown error in KubernetesJobWatcher. Failing')
                raise
            else:
                self.log.warning('Watch died gracefully, starting back up with: '
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
            for key, value in kube_config.kube_client_request_args.items():
                kwargs[key] = value

        last_resource_version = None
        if self.multi_namespace_mode:
            list_worker_pods = functools.partial(watcher.stream,
                                                 kube_client.list_pod_for_all_namespaces,
                                                 **kwargs)
        else:
            list_worker_pods = functools.partial(watcher.stream,
                                                 kube_client.list_namespaced_pod,
                                                 self.namespace,
                                                 **kwargs)
        for event in list_worker_pods():
            task = event['object']
            self.log.info(
                'Event: %s had an event of type %s',
                task.metadata.name, event['type']
            )
            if event['type'] == 'ERROR':
                return self.process_error(event)
            self.process_status(
                pod_id=task.metadata.name,
                namespace=task.metadata.namespace,
                status=task.status.phase,
                labels=task.metadata.labels,
                resource_version=task.metadata.resource_version,
                event=event,
            )
            last_resource_version = task.metadata.resource_version

        return last_resource_version

    def process_error(self, event):
        """Process error response"""
        self.log.error(
            'Encountered Error response from k8s list namespaced pod stream => %s',
            event
        )
        raw_object = event['raw_object']
        if raw_object['code'] == 410:
            self.log.info(
                'Kubernetes resource version is too old, must reset to 0 => %s',
                (raw_object['message'],)
            )
            # Return resource version 0
            return '0'
        raise AirflowException(
            'Kubernetes failure for %s with code %s and message: %s' %
            (raw_object['reason'], raw_object['code'], raw_object['message'])
        )

    def process_status(self, pod_id, namespace, status, labels, resource_version, event):
        """Process status response"""
        if status == 'Pending':
            if event['type'] == 'DELETED':
                self.log.info('Event: Failed to start pod %s, will reschedule', pod_id)
                self.watcher_queue.put((pod_id, namespace, State.UP_FOR_RESCHEDULE, labels, resource_version))
            else:
                self.log.info('Event: %s Pending', pod_id)
        elif status == 'Failed':
            self.log.info('Event: %s Failed', pod_id)
            self.watcher_queue.put((pod_id, namespace, State.FAILED, labels, resource_version))
        elif status == 'Succeeded':
            self.log.info('Event: %s Succeeded', pod_id)
            self.watcher_queue.put((pod_id, namespace, None, labels, resource_version))
        elif status == 'Running':
            self.log.info('Event: %s is Running', pod_id)
        else:
            self.log.warning(
                'Event: Invalid state: %s on pod: %s in namespace %s with labels: %s with '
                'resource_version: %s', status, pod_id, namespace, labels, resource_version
            )


class AirflowKubernetesScheduler(LoggingMixin):
    """Airflow Scheduler for Kubernetes"""
    def __init__(self, kube_config, task_queue, result_queue, kube_client, worker_uuid):
        self.log.debug("Creating Kubernetes executor")
        self.kube_config = kube_config
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.namespace = self.kube_config.kube_namespace
        self.log.debug("Kubernetes using namespace %s", self.namespace)
        self.kube_client = kube_client
        self.launcher = PodLauncher(kube_client=self.kube_client)
        self.worker_configuration_pod = WorkerConfiguration(kube_config=self.kube_config).as_pod()
        self._manager = multiprocessing.Manager()
        self.watcher_queue = self._manager.Queue()
        self.worker_uuid = worker_uuid
        self.kube_watcher = self._make_kube_watcher()

    def _make_kube_watcher(self):
        resource_version = KubeResourceVersion.get_current_resource_version()
        watcher = KubernetesJobWatcher(watcher_queue=self.watcher_queue,
                                       namespace=self.kube_config.kube_namespace,
                                       multi_namespace_mode=self.kube_config.multi_namespace_mode,
                                       resource_version=resource_version,
                                       worker_uuid=self.worker_uuid,
                                       kube_config=self.kube_config)
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

        if command[0:2] != ["airflow", "run"]:
            raise ValueError('The command must start with ["airflow", "run"].')

        pod = PodGenerator.construct_pod(
            namespace=self.namespace,
            worker_uuid=self.worker_uuid,
            pod_id=self._create_pod_id(dag_id, task_id),
            dag_id=pod_generator.make_safe_label_value(dag_id),
            task_id=pod_generator.make_safe_label_value(task_id),
            try_number=try_number,
            kube_image=self.kube_config.kube_image,
            date=execution_date,
            command=command,
            pod_override_object=kube_executor_config,
            base_worker_pod=self.worker_configuration_pod
        )

        sanitized_pod = self.launcher._client.api_client.sanitize_for_serialization(pod)
        json_pod = json.dumps(sanitized_pod, indent=2)

        self.log.debug('Pod Creation Request before mutation: \n%s', json_pod)

        # Reconcile the pod generated by the Operator and the Pod
        # generated by the .cfg file
        self.log.debug("Kubernetes running for command %s", command)
        self.log.debug("Kubernetes launching image %s", pod.spec.containers[0].image)

        # the watcher will monitor pods, so we do not block.
        self.launcher.run_pod_async(pod, **self.kube_config.kube_client_request_args)
        self.log.debug("Kubernetes Job created!")

    def delete_pod(self, pod_id, namespace):
        """Deletes POD"""
        try:
            self.kube_client.delete_namespaced_pod(
                pod_id, namespace, body=client.V1DeleteOptions(**self.kube_config.delete_option_kwargs),
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
        """Process the task by watcher."""
        pod_id, namespace, state, labels, resource_version = task
        self.log.info(
            'Attempting to finish pod; pod_id: %s; state: %s; labels: %s',
            pod_id, state, labels
        )
        key = self._labels_to_key(labels=labels)
        if key:
            self.log.debug('finishing job %s - %s (%s)', key, state, pod_id)
            self.result_queue.put((key, state, pod_id, namespace, resource_version))

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
        ``^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$``

        :param safe_dag_id: a dag_id with only alphanumeric characters
        :param safe_task_id: a task_id with only alphanumeric characters
        :param safe_uuid: a uuid
        :return: ``str`` valid Pod name of appropriate length
        """
        safe_key = safe_dag_id + safe_task_id

        safe_pod_id = safe_key[:MAX_POD_ID_LEN - len(safe_uuid) - 1] + "-" + safe_uuid

        return safe_pod_id

    @staticmethod
    def _create_pod_id(dag_id, task_id):
        safe_dag_id = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(
            dag_id)
        safe_task_id = AirflowKubernetesScheduler._strip_unsafe_kubernetes_special_chars(
            task_id)
        return safe_dag_id + safe_task_id

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
            self.log.warning("could not get try_number as an int: %s", labels.get('try_number', '1'))

        try:
            dag_id = labels['dag_id']
            task_id = labels['task_id']
            ex_time = self._label_safe_datestring_to_datetime(labels['execution_date'])
        except Exception as e:
            self.log.warning(
                'Error while retrieving labels; labels: %s; exception: %s',
                labels, e
            )
            return None

        with create_session() as session:
            task = (
                session
                .query(TaskInstance)
                .filter_by(task_id=task_id, dag_id=dag_id, execution_date=ex_time)
                .one_or_none()
            )
            if task:
                self.log.info(
                    'Found matching task %s-%s (%s) with current state of %s',
                    task.dag_id, task.task_id, task.execution_date, task.state
                )
                return (dag_id, task_id, ex_time, try_num)
            else:
                self.log.warning(
                    'task_id/dag_id are not safe to use as Kubernetes labels. This can cause '
                    'severe performance regressions. Please see '
                    '<https://kubernetes.io/docs/concepts/overview/working-with-objects'
                    '/labels/#syntax-and-character-set>. '
                    'Given dag_id: %s, task_id: %s', task_id, dag_id
                )

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
                    pod_generator.make_safe_label_value(task.dag_id) == dag_id and
                    pod_generator.make_safe_label_value(task.task_id) == task_id and
                    task.execution_date == ex_time
                ):
                    self.log.info(
                        'Found matching task %s-%s (%s) with current state of %s',
                        task.dag_id, task.task_id, task.execution_date, task.state
                    )
                    dag_id = task.dag_id
                    task_id = task.task_id
                    return (dag_id, task_id, ex_time, try_num)
        self.log.warning(
            'Failed to find and match task details to a pod; labels: %s',
            labels
        )
        return None

    def _flush_watcher_queue(self):
        self.log.debug('Executor shutting down, watcher_queue approx. size=%d', self.watcher_queue.qsize())
        while True:
            try:
                task = self.watcher_queue.get_nowait()
                # Ignoring it since it can only have either FAILED or SUCCEEDED pods
                self.log.warning('Executor shutting down, IGNORING watcher task=%s', task)
                self.watcher_queue.task_done()
            except Empty:
                break

    def terminate(self):
        """Termninates the watcher."""
        self.log.debug("Terminating kube_watcher...")
        self.kube_watcher.terminate()
        self.kube_watcher.join()
        self.log.debug("kube_watcher=%s", self.kube_watcher)
        self.log.debug("Flushing watcher_queue...")
        self._flush_watcher_queue()
        # Queue should be empty...
        self.watcher_queue.join()
        self.log.debug("Shutting down manager...")
        self._manager.shutdown()


class KubernetesExecutor(BaseExecutor, LoggingMixin):
    """Executor for Kubernetes"""
    def __init__(self):
        self.kube_config = KubeConfig()
        self.task_queue = None
        self.result_queue = None
        self.kube_scheduler = None
        self.kube_client = None
        self.worker_uuid = None
        self._manager = multiprocessing.Manager()
        super(KubernetesExecutor, self).__init__(parallelism=self.kube_config.parallelism)

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
        queued_tasks = session \
            .query(TaskInstance) \
            .filter(TaskInstance.state == State.QUEUED).all()
        self.log.info(
            'When executor started up, found %s queued task instances',
            len(queued_tasks)
        )

        for task in queued_tasks:
            # noinspection PyProtectedMember
            # pylint: disable=protected-access
            dict_string = (
                "dag_id={},task_id={},execution_date={},airflow-worker={}".format(
                    pod_generator.make_safe_label_value(task.dag_id),
                    pod_generator.make_safe_label_value(task.task_id),
                    AirflowKubernetesScheduler._datetime_to_label_safe_datestring(
                        task.execution_date
                    ),
                    self.worker_uuid
                )
            )
            # pylint: enable=protected-access
            kwargs = dict(label_selector=dict_string)
            if self.kube_config.kube_client_request_args:
                for key, value in self.kube_config.kube_client_request_args.items():
                    kwargs[key] = value
            pod_list = self.kube_client.list_namespaced_pod(
                self.kube_config.kube_namespace, **kwargs)
            if not pod_list.items:
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
        """Starts the executor"""
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
        """Executes task asynchronously"""
        self.log.info(
            'Add task %s with command %s with executor_config %s',
            key, command, executor_config
        )

        kube_executor_config = PodGenerator.from_obj(executor_config)
        self.task_queue.put((key, command, kube_executor_config))

    def sync(self):
        """Synchronize task state."""
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
                    key, state, pod_id, namespace, resource_version = results
                    last_resource_version = resource_version
                    self.log.info('Changing state of %s to %s', results, state)
                    try:
                        self._change_state(key, state, pod_id, namespace)
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
                except ApiException as e:
                    self.log.warning('ApiException when attempting to run task, re-queueing. '
                                     'Message: %s', json.loads(e.body)['message'])
                    self.task_queue.put(task)
                except HTTPError as e:
                    self.log.warning('HTTPError when attempting to run task, re-queueing. '
                                     'Exception: %s', str(e))
                    self.task_queue.put(task)
                finally:
                    self.task_queue.task_done()
            except Empty:
                break

    def _change_state(self, key, state, pod_id, namespace):
        if state != State.RUNNING:
            if self.kube_config.delete_worker_pods:
                if not self.kube_scheduler:
                    raise AirflowException("The executor should be started first!")
                if state is not State.FAILED or self.kube_config.delete_worker_pods_on_failure:
                    self.kube_scheduler.delete_pod(pod_id, namespace)
                    self.log.info('Deleted pod: %s in namespace %s', str(key), str(namespace))
            try:
                self.running.pop(key)
            except KeyError:
                self.log.debug('Could not find key: %s', str(key))
        self.event_buffer[key] = state

    def _flush_task_queue(self):
        self.log.debug('Executor shutting down, task_queue approximate size=%d', self.task_queue.qsize())
        while True:
            try:
                task = self.task_queue.get_nowait()
                # This is a new task to run thus ok to ignore.
                self.log.warning('Executor shutting down, will NOT run task=%s', task)
                self.task_queue.task_done()
            except Empty:
                break

    def _flush_result_queue(self):
        self.log.debug('Executor shutting down, result_queue approximate size=%d', self.result_queue.qsize())
        while True:  # pylint: disable=too-many-nested-blocks
            try:
                results = self.result_queue.get_nowait()
                self.log.warning('Executor shutting down, flushing results=%s', results)
                try:
                    key, state, pod_id, namespace, resource_version = results
                    self.log.info('Changing state of %s to %s : resource_version=%d', results, state,
                                  resource_version)
                    try:
                        self._change_state(key, state, pod_id, namespace)
                    except Exception as e:  # pylint: disable=broad-except
                        self.log.exception('Ignoring exception: %s when attempting to change state of %s '
                                           'to %s.', e, results, state)
                finally:
                    self.result_queue.task_done()
            except Empty:
                break

    def end(self):
        """Called when the executor shuts down"""
        self.log.info('Shutting down Kubernetes executor')
        self.log.debug('Flushing task_queue...')
        self._flush_task_queue()
        self.log.debug('Flushing result_queue...')
        self._flush_result_queue()
        # Both queues should be empty...
        self.task_queue.join()
        self.result_queue.join()
        if self.kube_scheduler:
            self.kube_scheduler.terminate()
        self._manager.shutdown()
