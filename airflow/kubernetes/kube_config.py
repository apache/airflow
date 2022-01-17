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

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.settings import AIRFLOW_HOME


class KubeConfig:
    """Configuration for Kubernetes"""

    core_section = 'core'
    kubernetes_section = 'kubernetes'
    logging_section = 'logging'

    def __init__(self):
        configuration_dict = conf.as_dict(display_sensitive=True)
        self.core_configuration = configuration_dict[self.core_section]
        self.airflow_home = AIRFLOW_HOME
        self.dags_folder = conf.get(self.core_section, 'dags_folder')
        self.parallelism = conf.getint(self.core_section, 'parallelism')
        self.pod_template_file = conf.get(self.kubernetes_section, 'pod_template_file', fallback=None)

        self.delete_worker_pods = conf.getboolean(self.kubernetes_section, 'delete_worker_pods')
        self.delete_worker_pods_on_failure = conf.getboolean(
            self.kubernetes_section, 'delete_worker_pods_on_failure'
        )
        self.worker_pods_creation_batch_size = conf.getint(
            self.kubernetes_section, 'worker_pods_creation_batch_size'
        )

        self.worker_container_repository = conf.get(self.kubernetes_section, 'worker_container_repository')
        self.worker_container_tag = conf.get(self.kubernetes_section, 'worker_container_tag')
        if self.worker_container_repository and self.worker_container_tag:
            self.kube_image = f'{self.worker_container_repository}:{self.worker_container_tag}'
        else:
            self.kube_image = None

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

        self.worker_pods_pending_timeout = conf.getint(self.kubernetes_section, 'worker_pods_pending_timeout')
        self.worker_pods_pending_timeout_check_interval = conf.getint(
            self.kubernetes_section, 'worker_pods_pending_timeout_check_interval'
        )
        self.worker_pods_pending_timeout_batch_size = conf.getint(
            self.kubernetes_section, 'worker_pods_pending_timeout_batch_size'
        )
        self.worker_pods_queued_check_interval = conf.getint(
            self.kubernetes_section, 'worker_pods_queued_check_interval'
        )

        self.kube_client_request_args = conf.getjson(
            self.kubernetes_section, 'kube_client_request_args', fallback={}
        )
        if not isinstance(self.kube_client_request_args, dict):
            raise AirflowConfigException(
                f"[{self.kubernetes_section}] 'kube_client_request_args' expected a JSON dict, got "
                + type(self.kube_client_request_args).__name__
            )
        if self.kube_client_request_args:
            if '_request_timeout' in self.kube_client_request_args and isinstance(
                self.kube_client_request_args['_request_timeout'], list
            ):
                self.kube_client_request_args['_request_timeout'] = tuple(
                    self.kube_client_request_args['_request_timeout']
                )
        self.delete_option_kwargs = conf.getjson(self.kubernetes_section, 'delete_option_kwargs', fallback={})
        if not isinstance(self.delete_option_kwargs, dict):
            raise AirflowConfigException(
                f"[{self.kubernetes_section}] 'delete_option_kwargs' expected a JSON dict, got "
                + type(self.delete_option_kwargs).__name__
            )
