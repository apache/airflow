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

airflow.providers.cncf.kubernetes.operators.spark_kubernetes
============================================================

.. py:module:: airflow.providers.cncf.kubernetes.operators.spark_kubernetes


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator


Module Contents
---------------

.. py:class:: SparkKubernetesOperator(*, image = None, code_path = None, namespace = 'default', name = None, application_file = None, template_spec=None, get_logs = True, do_xcom_push = False, success_run_history_limit = 1, startup_timeout_seconds=600, log_events_on_failure = False, reattach_on_restart = True, delete_on_termination = True, kubernetes_conn_id = 'kubernetes_default', random_name_suffix = True, **kwargs)

   Bases: :py:obj:`airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`


   Creates sparkApplication object in kubernetes cluster.

   .. seealso::
       For more detail about Spark Application Object have a look at the reference:
       https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.3.3-3.1.1/docs/api-docs.md#sparkapplication

   :param image: Docker image you wish to launch. Defaults to hub.docker.com,
   :param code_path: path to the spark code in image,
   :param namespace: kubernetes namespace to put sparkApplication
   :param name: name of the pod in which the task will run, will be used (plus a random
       suffix if random_name_suffix is True) to generate a pod id (DNS-1123 subdomain,
       containing only [a-z0-9.-]).
   :param application_file: filepath to kubernetes custom_resource_definition of sparkApplication
   :param template_spec: kubernetes sparkApplication specification
   :param get_logs: get the stdout of the container as logs of the tasks.
   :param do_xcom_push: If True, the content of the file
       /airflow/xcom/return.json in the container will also be pushed to an
       XCom when the container completes.
   :param success_run_history_limit: Number of past successful runs of the application to keep.
   :param startup_timeout_seconds: timeout in seconds to startup the pod.
   :param log_events_on_failure: Log the pod's events if a failure occurs
   :param reattach_on_restart: if the scheduler dies while the pod is running, reattach and monitor
   :param delete_on_termination: What to do when the pod reaches its final
       state, or the execution is interrupted. If True (default), delete the
       pod; if False, leave the pod.
   :param kubernetes_conn_id: the connection to Kubernetes cluster
   :param random_name_suffix: If True, adds a random suffix to the pod name


   .. py:attribute:: template_fields
      :value: ['application_file', 'namespace', 'template_spec', 'kubernetes_conn_id']



   .. py:attribute:: template_fields_renderers


   .. py:attribute:: template_ext
      :value: ('yaml', 'yml', 'json')



   .. py:attribute:: ui_color
      :value: '#f4a460'



   .. py:attribute:: BASE_CONTAINER_NAME
      :value: 'spark-kubernetes-driver'



   .. py:attribute:: image
      :value: None



   .. py:attribute:: code_path
      :value: None



   .. py:attribute:: application_file
      :value: None



   .. py:attribute:: template_spec
      :value: None



   .. py:attribute:: kubernetes_conn_id
      :value: 'kubernetes_default'



   .. py:attribute:: startup_timeout_seconds
      :value: 600



   .. py:attribute:: reattach_on_restart
      :value: True



   .. py:attribute:: delete_on_termination
      :value: True



   .. py:attribute:: do_xcom_push
      :value: False



   .. py:attribute:: namespace
      :value: 'default'



   .. py:attribute:: get_logs
      :value: True



   .. py:attribute:: log_events_on_failure
      :value: False



   .. py:attribute:: success_run_history_limit
      :value: 1



   .. py:attribute:: random_name_suffix
      :value: True



   .. py:method:: manage_template_specs()


   .. py:method:: create_job_name()


   .. py:method:: create_labels_for_pod(context = None, include_try_number = True)
      :staticmethod:


      Generate labels for the pod to track the pod in case of Operator crash.

      :param include_try_number: add try number to labels
      :param context: task context provided by airflow DAG
      :return: dict.



   .. py:property:: pod_manager
      :type: airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager



   .. py:property:: template_body

      Templated body for CustomObjectLauncher.



   .. py:method:: find_spark_job(context)


   .. py:method:: get_or_create_spark_crd(launcher, context)


   .. py:method:: process_pod_deletion(pod, *, reraise=True)


   .. py:property:: hook
      :type: airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook



   .. py:property:: client
      :type: kubernetes.client.CoreV1Api



   .. py:property:: custom_obj_api
      :type: kubernetes.client.CustomObjectsApi



   .. py:method:: execute(context)

      Based on the deferrable parameter runs the pod asynchronously or synchronously.



   .. py:method:: on_kill()

      Override this method to clean up subprocesses when a task instance gets killed.

      Any use of the threading, subprocess or multiprocessing module within an
      operator needs to be cleaned up, or it will leave ghost processes behind.



   .. py:method:: patch_already_checked(pod, *, reraise=True)

      Add an "already checked" annotation to ensure we don't reattach on retries.



   .. py:method:: dry_run()

      Print out the spark job that would be created by this operator.
