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
"""
Example of the PodOperator and ReturnValuePodOperator which would execute
pods on a Kubernetes cluster. PodOperator would only work if airflow is
deployed within kubernetes.
"""
import os

import airflow
import random
from airflow.contrib.kubernetes.pod import Pod, Config
from airflow.contrib.operators.k8s_pod_operator import ReturnValuePodOperator, PodOperator
from airflow.models import DAG
from airflow.utils.trigger_rule import TriggerRule

# TODO: Replace the etcd endpoint with your own etcd endpoint
args = {
    'owner': 'airflow',
    'etcd_endpoint': os.environ.get('AIRFLOWSVC_SERVICE_HOST') + ':' +
                     os.environ.get('AIRFLOWSVC_SERVICE_PORT_ETCDSVC_PORT'),
    'start_date': airflow.utils.dates.days_ago(2)
}

docker_image = 'artprod.dev.bloomberg.com/ds/molecula-python:1.0.0.0-SNAPSHOT'  # Replace with 'ubuntu:latest'
dag = DAG(
    dag_id='example_pod_operator', default_args=args,
    schedule_interval=None)


def pod_that_returns_hello(context):
    """
    Returns a Pod object given the airflow context.
    """
    image = docker_image
    cmds = ['/bin/bash', '-c', 'echo "Hello $RANDOM" > /tmp/result.txt']
    return Pod(image=image, cmds=cmds)


hello_kube_step1 = ReturnValuePodOperator(dag=dag,
                                          task_id='hello-kube-step1',
                                          dag_run_id='run-1',
                                          pod_factory=pod_that_returns_hello,
                                          result_data_file='/tmp/result.txt')


def pod_that_reads_upstream_result(context):
    up_task_id = 'hello-kube-step1'
    # The message including a random number generated inside the upstream pod will be read here
    return_val = context['ti'].xcom_pull(key='custom_result', task_ids=up_task_id)
    image = docker_image
    cmds = ['/bin/bash', '-c', 'echo ' + return_val]
    return Pod(image=image, cmds=cmds)


hello_kube_step2 = PodOperator(dag=dag,
                               task_id='hello-kube-step2',
                               dag_run_id='run_1',
                               pod_factory=pod_that_reads_upstream_result)
hello_kube_step2.set_upstream(hello_kube_step1)

def pod_that_injects_configs(context):
    """
    The returning pod object has a configs map which tells the operator to inject some JSON objects as
    config files
    """
    image = docker_image  # Replace with 'ubuntu:latest'
    configs = [ Config('/configs/c1.json', { 'random_val': str(random.random()) }), Config('/configs/c2.json', { 'my_db': 'conn_str' }) ]
    cmds = ['/bin/bash', '-c', 'sleep 3; cat /configs/c2.json']
    return Pod(image=image, cmds=cmds, configs=configs)

hello_kube_step3 = ReturnValuePodOperator(dag=dag,
                               task_id='hello-kube-step3',
                               dag_run_id='run_1',
                               pod_factory=pod_that_injects_configs,
                               result_data_file='/configs/c1.json')
hello_kube_step3.set_upstream(hello_kube_step1)



