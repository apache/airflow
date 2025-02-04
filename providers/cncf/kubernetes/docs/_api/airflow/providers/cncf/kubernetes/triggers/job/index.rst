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

airflow.providers.cncf.kubernetes.triggers.job
==============================================

.. py:module:: airflow.providers.cncf.kubernetes.triggers.job


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.triggers.job.KubernetesJobTrigger


Module Contents
---------------

.. py:class:: KubernetesJobTrigger(job_name, job_namespace, pod_name, pod_namespace, base_container_name, kubernetes_conn_id = None, poll_interval = 10.0, cluster_context = None, config_file = None, in_cluster = None, get_logs = True, do_xcom_push = False)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`


   KubernetesJobTrigger run on the trigger worker to check the state of Job.

   :param job_name: The name of the job.
   :param job_namespace: The namespace of the job.
   :param pod_name: The name of the Pod.
   :param pod_namespace: The namespace of the Pod.
   :param base_container_name: The name of the base container in the pod.
   :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
       for the Kubernetes cluster.
   :param cluster_context: Context that points to kubernetes cluster.
   :param config_file: Path to kubeconfig file.
   :param poll_interval: Polling period in seconds to check for the status.
   :param in_cluster: run kubernetes client with in_cluster configuration.
   :param get_logs: get the stdout of the base container as logs of the tasks.
   :param do_xcom_push: If True, the content of the file
       /airflow/xcom/return.json in the container will also be pushed to an
       XCom when the container completes.


   .. py:attribute:: job_name


   .. py:attribute:: job_namespace


   .. py:attribute:: pod_name


   .. py:attribute:: pod_namespace


   .. py:attribute:: base_container_name


   .. py:attribute:: kubernetes_conn_id
      :value: None



   .. py:attribute:: poll_interval
      :value: 10.0



   .. py:attribute:: cluster_context
      :value: None



   .. py:attribute:: config_file
      :value: None



   .. py:attribute:: in_cluster
      :value: None



   .. py:attribute:: get_logs
      :value: True



   .. py:attribute:: do_xcom_push
      :value: False



   .. py:method:: serialize()

      Serialize KubernetesCreateJobTrigger arguments and classpath.



   .. py:method:: run()
      :async:


      Get current job status and yield a TriggerEvent.



   .. py:property:: hook
      :type: airflow.providers.cncf.kubernetes.hooks.kubernetes.AsyncKubernetesHook



   .. py:property:: pod_manager
      :type: airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager
