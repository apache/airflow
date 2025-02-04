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

airflow.providers.cncf.kubernetes.hooks.kubernetes
==================================================

.. py:module:: airflow.providers.cncf.kubernetes.hooks.kubernetes


Attributes
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.hooks.kubernetes.LOADING_KUBE_CONFIG_FILE_RESOURCE
   airflow.providers.cncf.kubernetes.hooks.kubernetes.JOB_FINAL_STATUS_CONDITION_TYPES
   airflow.providers.cncf.kubernetes.hooks.kubernetes.JOB_STATUS_CONDITION_TYPES


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook
   airflow.providers.cncf.kubernetes.hooks.kubernetes.AsyncKubernetesHook


Module Contents
---------------

.. py:data:: LOADING_KUBE_CONFIG_FILE_RESOURCE
   :value: 'Loading Kubernetes configuration file kube_config from {}...'


.. py:data:: JOB_FINAL_STATUS_CONDITION_TYPES

.. py:data:: JOB_STATUS_CONDITION_TYPES

.. py:class:: KubernetesHook(conn_id = default_conn_name, client_configuration = None, cluster_context = None, config_file = None, in_cluster = None, disable_verify_ssl = None, disable_tcp_keepalive = None)

   Bases: :py:obj:`airflow.hooks.base.BaseHook`, :py:obj:`airflow.providers.cncf.kubernetes.utils.pod_manager.PodOperatorHookProtocol`


   Creates Kubernetes API connection.

   - use in cluster configuration by using extra field ``in_cluster`` in connection
   - use custom config by providing path to the file using extra field ``kube_config_path`` in connection
   - use custom configuration by providing content of kubeconfig file via
       extra field ``kube_config`` in connection
   - use default config by providing no extras

   This hook check for configuration option in the above order. Once an option is present it will
   use this configuration.

   .. seealso::
       For more information about Kubernetes connection:
       :doc:`/connections/kubernetes`

   :param conn_id: The :ref:`kubernetes connection <howto/connection:kubernetes>`
       to Kubernetes cluster.
   :param client_configuration: Optional dictionary of client configuration params.
       Passed on to kubernetes client.
   :param cluster_context: Optionally specify a context to use (e.g. if you have multiple
       in your kubeconfig.
   :param config_file: Path to kubeconfig file.
   :param in_cluster: Set to ``True`` if running from within a kubernetes cluster.
   :param disable_verify_ssl: Set to ``True`` if SSL verification should be disabled.
   :param disable_tcp_keepalive: Set to ``True`` if you want to disable keepalive logic.


   .. py:attribute:: conn_name_attr
      :value: 'kubernetes_conn_id'



   .. py:attribute:: default_conn_name
      :value: 'kubernetes_default'



   .. py:attribute:: conn_type
      :value: 'kubernetes'



   .. py:attribute:: hook_name
      :value: 'Kubernetes Cluster Connection'



   .. py:attribute:: DEFAULT_NAMESPACE
      :value: 'default'



   .. py:method:: get_connection_form_widgets()
      :classmethod:


      Return connection widgets to add to connection form.



   .. py:method:: get_ui_field_behaviour()
      :classmethod:


      Return custom field behaviour.



   .. py:attribute:: conn_id
      :value: 'kubernetes_default'



   .. py:attribute:: client_configuration
      :value: None



   .. py:attribute:: cluster_context
      :value: None



   .. py:attribute:: config_file
      :value: None



   .. py:attribute:: in_cluster
      :value: None



   .. py:attribute:: disable_verify_ssl
      :value: None



   .. py:attribute:: disable_tcp_keepalive
      :value: None



   .. py:method:: get_connection(conn_id)
      :classmethod:


      Return requested connection.

      If missing and conn_id is "kubernetes_default", will return empty connection so that hook will
      default to cluster-derived credentials.



   .. py:property:: conn_extras


   .. py:method:: get_conn()

      Return kubernetes api session for use with requests.



   .. py:property:: is_in_cluster
      :type: bool


      Expose whether the hook is configured with ``load_incluster_config`` or not.



   .. py:property:: api_client
      :type: kubernetes.client.ApiClient


      Cached Kubernetes API client.



   .. py:property:: core_v1_client
      :type: kubernetes.client.CoreV1Api


      Get authenticated client object.



   .. py:property:: apps_v1_client
      :type: kubernetes.client.AppsV1Api



   .. py:property:: custom_object_client
      :type: kubernetes.client.CustomObjectsApi



   .. py:property:: batch_v1_client
      :type: kubernetes.client.BatchV1Api



   .. py:method:: create_custom_object(group, version, plural, body, namespace = None)

      Create custom resource definition object in Kubernetes.

      :param group: api group
      :param version: api version
      :param plural: api plural
      :param body: crd object definition
      :param namespace: kubernetes namespace



   .. py:method:: get_custom_object(group, version, plural, name, namespace = None)

      Get custom resource definition object from Kubernetes.

      :param group: api group
      :param version: api version
      :param plural: api plural
      :param name: crd object name
      :param namespace: kubernetes namespace



   .. py:method:: delete_custom_object(group, version, plural, name, namespace = None, **kwargs)

      Delete custom resource definition object from Kubernetes.

      :param group: api group
      :param version: api version
      :param plural: api plural
      :param name: crd object name
      :param namespace: kubernetes namespace



   .. py:method:: get_namespace()

      Return the namespace that defined in the connection.



   .. py:method:: get_xcom_sidecar_container_image()

      Return the xcom sidecar image that defined in the connection.



   .. py:method:: get_xcom_sidecar_container_resources()

      Return the xcom sidecar resources that defined in the connection.



   .. py:method:: get_pod_log_stream(pod_name, container = '', namespace = None)

      Retrieve a log stream for a container in a kubernetes pod.

      :param pod_name: pod name
      :param container: container name
      :param namespace: kubernetes namespace



   .. py:method:: get_pod_logs(pod_name, container = '', namespace = None)

      Retrieve a container's log from the specified pod.

      :param pod_name: pod name
      :param container: container name
      :param namespace: kubernetes namespace



   .. py:method:: get_pod(name, namespace)

      Read pod object from kubernetes API.



   .. py:method:: get_namespaced_pod_list(label_selector = '', namespace = None, watch = False, **kwargs)

      Retrieve a list of Kind pod which belong default kubernetes namespace.

      :param label_selector: A selector to restrict the list of returned objects by their labels
      :param namespace: kubernetes namespace
      :param watch: Watch for changes to the described resources and return them as a stream



   .. py:method:: get_deployment_status(name, namespace = 'default', **kwargs)

      Get status of existing Deployment.

      :param name: Name of Deployment to retrieve
      :param namespace: Deployment namespace



   .. py:method:: create_job(job, **kwargs)

      Run Job.

      :param job: A kubernetes Job object



   .. py:method:: get_job(job_name, namespace)

      Get Job of specified name and namespace.

      :param job_name: Name of Job to fetch.
      :param namespace: Namespace of the Job.
      :return: Job object



   .. py:method:: get_job_status(job_name, namespace)

      Get job with status of specified name and namespace.

      :param job_name: Name of Job to fetch.
      :param namespace: Namespace of the Job.
      :return: Job object



   .. py:method:: wait_until_job_complete(job_name, namespace, job_poll_interval = 10)

      Block job of specified name and namespace until it is complete or failed.

      :param job_name: Name of Job to fetch.
      :param namespace: Namespace of the Job.
      :param job_poll_interval: Interval in seconds between polling the job status
      :return: Job object



   .. py:method:: list_jobs_all_namespaces()

      Get list of Jobs from all namespaces.

      :return: V1JobList object



   .. py:method:: list_jobs_from_namespace(namespace)

      Get list of Jobs from dedicated namespace.

      :param namespace: Namespace of the Job.
      :return: V1JobList object



   .. py:method:: is_job_complete(job)

      Check whether the given job is complete (with success or fail).

      :return: Boolean indicating that the given job is complete.



   .. py:method:: is_job_failed(job)
      :staticmethod:


      Check whether the given job is failed.

      :return: Error message if the job is failed, and False otherwise.



   .. py:method:: is_job_successful(job)
      :staticmethod:


      Check whether the given job is completed successfully..

      :return: Error message if the job is failed, and False otherwise.



   .. py:method:: patch_namespaced_job(job_name, namespace, body)

      Update the specified Job.

      :param job_name: name of the Job
      :param namespace: the namespace to run within kubernetes
      :param body: json object with parameters for update



   .. py:method:: apply_from_yaml_file(api_client = None, yaml_file = None, yaml_objects = None, verbose = False, namespace = 'default')

      Perform an action from a yaml file.

      :param api_client: A Kubernetes client application.
      :param yaml_file: Contains the path to yaml file.
      :param yaml_objects: List of YAML objects; used instead of reading the yaml_file.
      :param verbose: If True, print confirmation from create action. Default is False.
      :param namespace: Contains the namespace to create all resources inside. The namespace must
          preexist otherwise the resource creation will fail.



   .. py:method:: check_kueue_deployment_running(name, namespace, timeout = 300.0, polling_period_seconds = 2.0)


   .. py:method:: get_yaml_content_from_file(kueue_yaml_url)
      :staticmethod:


      Download content of YAML file and separate it into several dictionaries.



.. py:class:: AsyncKubernetesHook(config_dict = None, *args, **kwargs)

   Bases: :py:obj:`KubernetesHook`


   Hook to use Kubernetes SDK asynchronously.


   .. py:attribute:: config_dict
      :value: None



   .. py:method:: get_conn_extras()
      :async:



   .. py:method:: get_conn()
      :async:


      Return kubernetes api session for use with requests.



   .. py:method:: get_pod(name, namespace)
      :async:


      Get pod's object.

      :param name: Name of the pod.
      :param namespace: Name of the pod's namespace.



   .. py:method:: delete_pod(name, namespace)
      :async:


      Delete pod's object.

      :param name: Name of the pod.
      :param namespace: Name of the pod's namespace.



   .. py:method:: read_logs(name, namespace)
      :async:


      Read logs inside the pod while starting containers inside.

      All the logs will be outputted with its timestamp to track
      the logs after the execution of the pod is completed. The
      method is used for async output of the logs only in the pod
      failed it execution or the task was cancelled by the user.

      :param name: Name of the pod.
      :param namespace: Name of the pod's namespace.



   .. py:method:: get_job_status(name, namespace)
      :async:


      Get job's status object.

      :param name: Name of the pod.
      :param namespace: Name of the pod's namespace.



   .. py:method:: wait_until_job_complete(name, namespace, poll_interval = 10)
      :async:


      Block job of specified name and namespace until it is complete or failed.

      :param name: Name of Job to fetch.
      :param namespace: Namespace of the Job.
      :param poll_interval: Interval in seconds between polling the job status
      :return: Job object



   .. py:method:: wait_until_container_complete(name, namespace, container_name, poll_interval = 10)
      :async:


      Wait for the given container in the given pod to be completed.

      :param name: Name of Pod to fetch.
      :param namespace: Namespace of the Pod.
      :param container_name: name of the container within the pod to monitor
      :param poll_interval: Interval in seconds between polling the container status



   .. py:method:: wait_until_container_started(name, namespace, container_name, poll_interval = 10)
      :async:


      Wait for the given container in the given pod to be started.

      :param name: Name of Pod to fetch.
      :param namespace: Namespace of the Pod.
      :param container_name: name of the container within the pod to monitor
      :param poll_interval: Interval in seconds between polling the container status
