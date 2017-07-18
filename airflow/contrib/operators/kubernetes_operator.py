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

import logging
import re

from time import sleep, time
from random import randint

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.kubernetes_hook import KubernetesHook


class KubernetesPodOperator(BaseOperator):
    """
    Deploys docker container to k8s pod and waits for its completion

    :param name: Name of the pod, optional if not given an unique name will
        be created automatically
    :type name: string
    :param namespace: Namespace the pod will be deployed to
    :type namespace: string
    :param image: Fully qualified name of the image in form
        of repo/image:tag
    :type image: string
    :param command: Commands to execute in the image,
        default image command will be executed if none supplied
    :type command: string or list
    :param op_args: Arguments for the command
    :type op_args: list
    :param wait: Wait for the completion. Default True. If set to false
        the operator waits for the pod to start running to ensure
        successful creation.
    :type wait: boolean
    :param unique_name: Whether the operator should ensure the uniqueness
        of the pod's name. Default is true
    :type unique_name: boolean
    :param cleanup: Perform cleanup on completion.
        Allowed values: Always, Never, OnSuccess, OnFailure.
        Default Always. Settign wait == False forces Never,
        as cleanup can be only performed on terminated container.
    :param labels: Labels and presets to apply to the pod.
    :type labels: dict
    :param env: Environment variables defintion as a dictionary
        of a form name:definition, where definition is a string or
        a dictionary with following fields:
        source (configMap|secret), name, and key
    :type env: dict
    :param conn_id: Id of pre-defined k8s connection. Currently not used,
        as only preconfigured environment with kube config or env variables
        is supported.
    :type conn_id: string
    :param poke_interval: Interval between checking the status in seconds
    :type poke_interval: integer
    :param wait_timeout: Time in seconds to wait for the pod to reach running state.
    :type wait_timeout: integer

    """
    template_fields = ('name', 'command', 'op_args', 'namespace', 'env')
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            image,
            name=None,
            namespace="default",
            command=None,
            op_args=None,
            wait=True,
            unique_name=True,
            cleanup="Always",
            labels=None,
            env=None,
            env_from=None,
            volumes=None,
            conn_id="k8s_default",
            poke_interval=3,
            wait_timeout=60,
            *args, **kwargs):
        super(KubernetesPodOperator, self).__init__(*args, **kwargs)
        self.image = image
        self.name = name
        self.namespace = namespace
        self.command = command
        self.op_args = op_args
        self.wait = wait
        self.unique_name = unique_name
        self.cleanup = cleanup if self.wait else "Never"
        self.labels = labels
        self.env = env
        self.env_from = env_from
        self.volumes = volumes
        self.poke_interval = poke_interval
        self.wait_timeout = wait_timeout
        self.conn_id = conn_id

    def _create_hook(self):
        return KubernetesHook(self.conn_id)

    def _base_name(self, context):
        if self.name is not None:
            return self.name

        base_name = "%s-%s" % (context['ti'].dag_id, context['ti'].task_id)
        r = re.compile('[^a-z0-9-]+')
        base_name = r.sub('-', base_name)
        self.name = base_name
        return base_name

    def _unique_name(self, context):
        name = self._base_name(context)

        if not self.unique_name:
            return name

        job_id = context['ti'].job_id
        if job_id is None:
            # job_id is None when running "airflow test"
            job_id = int(time()*1000)

        return "%s-%s" % (name, job_id)

    def should_do_cleanup(self, status):
        return ((self.cleanup == "Always") or
                ((self.cleanup == "OnFailure") and (status == "Failed")) or
                ((self.cleanup == "OnSuccess") and (status == "Succeeded")))

    def execute(self, context):
        exit_statuses = ["Succeeded"] if self.wait else ["Running", "Succeeded"]

        hook = self._create_hook()

        pod_name = self._unique_name(context)
        pod = hook.get_pod_definition(
            image=self.image,
            name=pod_name,
            namespace=self.namespace,
            restart_policy="Never",
            command=self.command,
            args=self.op_args,
            env=self.env,
            env_from=self.env_from,
            volumes=self.volumes,
            labels=self.labels)

        logging.info("Creating pod %s in namespace %s",
                pod_name, self.namespace)

        logging.debug("Pod definition: %s", pod.spec)

        hook.create_pod(pod)
        max_wait = time() + self.wait_timeout

        try:
            hook.relay_pod_events(pod, timeout=self.wait_timeout)

            status = None
            while hook.get_pod_state(pod) == 'Pending':
                if time() > max_wait:
                    raise AirflowException("Timeout while waiting for \
                            pod to reach Running state.")

                sleep(self.poke_interval)

            if self.wait:
                hook.relay_pod_logs(pod)

            while not (status in exit_statuses):
                status = hook.get_pod_state(pod)
                logging.info("Checking pod status => %s", status)

                if (status == "Failed"):
                    raise AirflowException("Pod failed!")

                sleep(self.poke_interval)
        finally:
            if (self.wait and self.should_do_cleanup(status)):
                hook.delete_pod(pod)
