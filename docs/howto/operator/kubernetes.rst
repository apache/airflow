 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



.. _howto/operator:KubernetesPodOperator:

KubernetesPodOperator
=====================

The :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator`:

* Launches a Docker image as a Kubernetes Pod to execute an individual Airflow
  task via a Kubernetes API request, using the
  `Kubernetes Python Client <https://github.com/kubernetes-client/python>`_
* Terminate the pod when the task is completed
* Works with any Airflow Executor
* Allows Airflow to act a job orchestrator for a Docker container,
  no matter the language the job was written in
* Enables task-level resource configuration
* Allow you to pass Kubernetes specific parameters into the task

.. code:: python

    import kubernetes.client.models as k8s

    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
    from airflow.contrib.kubernetes.secret import Secret
    from airflow.contrib.kubernetes.volume import Volume
    from airflow.contrib.kubernetes.volume_mount import VolumeMount
    from airflow.contrib.kubernetes.pod import Port


    secret_file = Secret('volume', '/etc/sql_conn', 'airflow-secrets', 'sql_alchemy_conn')
    secret_env  = Secret('env', 'SQL_CONN', 'airflow-secrets', 'sql_alchemy_conn')
    secret_all_keys  = Secret('env', None, 'airflow-secrets-2')
    volume_mount = VolumeMount('test-volume',
                                mount_path='/root/mount_file',
                                sub_path=None,
                                read_only=True)
    port = Port('http', 80)
    configmaps = ['test-configmap-1', 'test-configmap-2']

    volume_config= {
        'persistentVolumeClaim':
          {
            'claimName': 'test-volume'
          }
        }
    volume = Volume(name='test-volume', configs=volume_config)

    init_container_volume_mounts = [k8s.V1VolumeMount(
      mount_path='/etc/foo',
      name='test-volume',
      sub_path=None,
      read_only=True
    )]

    init_environments = [k8s.V1EnvVar(
      name='key1',
      value='value1'
    ), k8s.V1EnvVar(
      name='key2',
      value='value2'
    )]

    init_container = k8s.V1Container(
      name="init-container",
      image="ubuntu:16.04",
      env=init_environments,
      volume_mounts=init_container_volume_mounts,
      command=["bash", "-cx"],
      args=["echo 10"]
    )

    affinity = {
        'nodeAffinity': {
          'preferredDuringSchedulingIgnoredDuringExecution': [
            {
              "weight": 1,
              "preference": {
                "matchExpressions": {
                  "key": "disktype",
                  "operator": "In",
                  "values": ["ssd"]
                }
              }
            }
          ]
        },
        "podAffinity": {
          "requiredDuringSchedulingIgnoredDuringExecution": [
            {
              "labelSelector": {
                "matchExpressions": [
                  {
                    "key": "security",
                    "operator": "In",
                    "values": ["S1"]
                  }
                ]
              },
              "topologyKey": "failure-domain.beta.kubernetes.io/zone"
            }
          ]
        },
        "podAntiAffinity": {
          "requiredDuringSchedulingIgnoredDuringExecution": [
            {
              "labelSelector": {
                "matchExpressions": [
                  {
                    "key": "security",
                    "operator": "In",
                    "values": ["S2"]
                  }
                ]
              },
              "topologyKey": "kubernetes.io/hostname"
            }
          ]
        }
    }

    tolerations = [
        {
            'key': "key",
            'operator': 'Equal',
            'value': 'value'
         }
    ]

    k = KubernetesPodOperator(namespace='default',
                              image="ubuntu:16.04",
                              cmds=["bash", "-cx"],
                              arguments=["echo", "10"],
                              labels={"foo": "bar"},
                              secrets=[secret_file, secret_env, secret_all_keys],
                              ports=[port]
                              volumes=[volume],
                              volume_mounts=[volume_mount],
                              name="test",
                              task_id="task",
                              affinity=affinity,
                              is_delete_operator_pod=True,
                              hostnetwork=False,
                              tolerations=tolerations,
                              configmaps=configmaps,
                              init_containers=[init_container],
                              priority_class_name="medium",
                              )
