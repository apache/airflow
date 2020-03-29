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
import inspect

import yaml
from kubernetes.client import ApiClient

from airflow.kubernetes import pod_generator
from airflow.kubernetes.k8s_model import append_to_pod
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.cli import get_dag


def is_a_kubernetes_pod_operator_instance(task):
    """Return true if the object is instance of the kubernetes pod generator"""
    return isinstance(task, KubernetesPodOperator)


def get_kubernetes_pod_attributes(kubernetes_task_kwargs):
    """Return a dictionoary for all the attributes of a
       kubernetes pod operator task"""
    all_instances = inspect.getmro(KubernetesPodOperator)
    attrs = []
    for instance in all_instances:
        attrs.extend(list(inspect.signature(instance.__init__).parameters.keys()))

    return {arg: value for arg, value in kubernetes_task_kwargs.items() if arg in attrs}


def generate_pod_preview(kubernetes_tasks_args):
    """Returns a dictionary with a kubernetes pod template for a given kubernetes
       pod operator task"""
    pod_generator_attrs = inspect.getfullargspec(PodGenerator).args
    pod_args = {attr: value for attr, value in kubernetes_tasks_args.items()
                if attr in pod_generator_attrs}

    # TODO: This step should not be done there is naming conversion in all
    # kubernetes classes args to env_vars to envs.
    if 'args' not in pod_args:
        pod_args['args'] = kubernetes_tasks_args.get('arguments', None)
    if 'envs' not in pod_args:
        pod_args['envs'] = kubernetes_tasks_args.get('env_vars', None)
    if 'extract_xcom' not in pod_args:
        pod_args['extract_xcom'] = kubernetes_tasks_args.get('do_xcom_push', None)

    pod_instance = pod_generator.PodGenerator(**(pod_args)).gen_pod()
    append_to_pod(pod_instance,
                  kubernetes_tasks_args.get('pod_runtime_info_envs', None) +
                  kubernetes_tasks_args.get('ports', None) +
                  kubernetes_tasks_args.get('resources', None) +
                  kubernetes_tasks_args.get('secrets', None) +
                  kubernetes_tasks_args.get('volumes', None) +
                  kubernetes_tasks_args.get('volume_mounts', None)
                  )

    res = ApiClient().sanitize_for_serialization(pod_instance)

    res['metadata']['name'] = pod_args['name']

    return res


def kubernetes_preview(args):
    """Cli function prints a yaml template in the console for a given dag for all
       the kubernetespodoperator tasks"""
    dag_tasks = get_dag(args.subdir, args.dag_id).tasks
    kubernetes_tasks_args = [task.__dict__ for task in dag_tasks
                             if is_a_kubernetes_pod_operator_instance(task)]
    if kubernetes_tasks_args:
        for task_args in kubernetes_tasks_args:
            kubernetes_pod_args = get_kubernetes_pod_attributes(task_args)
            pod_request = generate_pod_preview(kubernetes_pod_args)
            yaml_object = yaml.dump(pod_request)
            print(yaml_object)
