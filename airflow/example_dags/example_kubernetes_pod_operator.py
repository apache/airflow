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
import datetime
from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

with models.DAG(
        dag_id='example_kubernetes_pod_operator',
        schedule_interval=datetime.timedelta(days=1),
        start_date=YESTERDAY) as dag:

    kubernetes_min_pod = KubernetesPodOperator(
        task_id='pod-ex-minimum',
        name='pod-ex-minimum',
        cmds=['echo'],
        namespace='beta',
        image='gcr.io/gcp-runtimes/ubuntu_18_0_4')
    kubenetes_template_ex = KubernetesPodOperator(
        task_id='ex-kube-templates',
        name='ex-kube-templates',
        namespace='prod',
        image='bash',
        cmds=['echo'],
        arguments=['{{ ds }}'],
        env_vars={'MY_VALUE': '{{ var.value.my_value }}'},
        config_file="{{ conf.get('core', 'kube_config') }}")
    kubernetes_affinity_ex = KubernetesPodOperator(
        task_id='ex-pod-affinity',
        name='ex-pod-affinity',
        namespace='master',
        image='perl',
        cmds=['perl'],
        arguments=['-Mbignum=bpi', '-wle', 'print bpi(2000)'],
        affinity={
            'nodeAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            'key': 'cloud.google.com/gke-nodepool',
                            'operator': 'In',
                            'values': [
                                'pool-0',
                                'pool-1',
                            ]
                        }]
                    }]
                }
            }
        })
    kubernetes_full_pod = KubernetesPodOperator(
        task_id='ex-all-configs',
        name='pi',
        namespace='default',
        image='perl',
        cmds=['perl'],
        arguments=['-Mbignum=bpi', '-wle', 'print bpi(2000)'],
        labels={'pod-label': 'label-name'},
        startup_timeout_seconds=120,
        env_vars={'EXAMPLE_VAR': '/example/value'},
        get_logs=True,
        image_pull_policy='Always',
        annotations={'key1': 'value1'},
        config_file='/home/airflow/composer_kube_config',
        affinity={})
