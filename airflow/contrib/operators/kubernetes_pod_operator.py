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

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.kubernetes import kube_client, pod_generator, pod_launcher
from airflow.utils.state import State

template_fields = ('templates_dict',)
template_ext = tuple()
ui_color = '#ffefeb'


class KubernetesPodOperator(BaseOperator):
    """
    Execute a task in a Kubernetes Pod

    :param image: Docker image name
    :type image: str
    :param: namespace: namespace name where run the Pod
    :type: namespace: str
    :param cmds: entrypoint of the container
    :type cmds: list
    :param arguments: arguments of to the entrypoint.
        The docker image's CMD is used if this is not provided.
    :type arguments: list
    :param labels: labels to apply to the Pod
    :type labels: dict
    :param startup_timeout_seconds: timeout in seconds to startup the pod
    :type startup_timeout_seconds: int
    :param name: name for the pod
    :type name: str
    :param env_vars: Environment variables initialized in the container
    :type env_vars: dict
    :param secrets: Secrets to attach to the container
    :type secrets: list
    :param in_cluster: run kubernetes client with in_cluster configuration
    :type in_cluster: bool
    :param get_logs: get the stdout of the container as logs of the tasks
    """

    def execute(self, context):
        try:
            client = kube_client.get_kube_client(in_cluster=self.in_cluster)
            gen = pod_generator.PodGenerator()

            pod = gen.make_pod(
                namespace=self.namespace,
                image=self.image,
                pod_id=self.name,
                cmds=self.cmds,
                arguments=self.arguments,
                labels=self.labels
            )

            pod.secrets = self.secrets
            pod.envs = self.env_vars

            launcher = pod_launcher.PodLauncher(client)
            final_state = launcher.run_pod(
                pod,
                startup_timeout=self.startup_timeout_seconds,
                get_logs=self.get_logs)
            if final_state != State.SUCCESS:
                raise AirflowException('Pod returned a failure')
        except AirflowException as ex:
            raise AirflowException('Pod Launching failed: {error}'.format(error=ex))

    @apply_defaults
    def __init__(self,
                 namespace,
                 image,
                 cmds,
                 arguments,
                 name,
                 env_vars=None,
                 secrets=None,
                 in_cluster=False,
                 labels=None,
                 startup_timeout_seconds=120,
                 get_logs=True,
                 *args,
                 **kwargs):
        super(KubernetesPodOperator, self).__init__(*args, **kwargs)
        self.image = image
        self.namespace = namespace
        self.cmds = cmds
        self.arguments = arguments
        self.labels = labels or {}
        self.startup_timeout_seconds = startup_timeout_seconds
        self.name = name
        self.env_vars = env_vars or {}
        self.secrets = secrets or []
        self.in_cluster = in_cluster
        self.get_logs = get_logs
