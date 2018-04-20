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
    def execute(self, context):
        try:

            client = kube_client.get_kube_client(in_cluster=self.in_cluster)
            gen = pod_generator.PodGenerator()

            pod = gen.make_pod(namespace=self.namespace,
                               image=self.image,
                               pod_id=self.name,
                               cmds=self.cmds,
                               arguments=self.arguments,
                               labels=self.labels,
                               kube_executor_config=self.kube_executor_config
                               )

            launcher = pod_launcher.PodLauncher(client)
            final_state = launcher.run_pod(
                pod,
                startup_timeout=self.startup_timeout_seconds,
                get_logs=self.get_logs)
            if final_state != State.SUCCESS:
                raise AirflowException("Pod returned a failure")
        except AirflowException as ex:
            raise AirflowException("Pod Launching failed: {error}".format(error=ex))


    @apply_defaults
    def __init__(self,
                 namespace,
                 image,
                 cmds,
                 arguments,
                 name,
                 in_cluster=False,
                 labels=None,
                 startup_timeout_seconds=120,
                 kube_executor_config=None,
                 get_logs=True,
                 *args,
                 **kwargs):
        super(KubernetesPodOperator, self).__init__(*args, **kwargs)
        self.kube_executor_config = kube_executor_config or {}
        self.image = image
        self.namespace = namespace
        self.cmds = cmds
        self.arguments = arguments
        self.labels = labels or {}
        self.startup_timeout_seconds = startup_timeout_seconds
        self.name = name
        self.in_cluster = in_cluster
        self.get_logs = get_logs
