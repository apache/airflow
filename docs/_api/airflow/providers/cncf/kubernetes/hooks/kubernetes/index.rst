:mod:`airflow.providers.cncf.kubernetes.hooks.kubernetes`
=========================================================

.. py:module:: airflow.providers.cncf.kubernetes.hooks.kubernetes


Module Contents
---------------

.. function:: _load_body_to_dict(body)

.. py:class:: KubernetesHook(conn_id: str = 'kubernetes_default', client_configuration: Optional[client.Configuration] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Creates Kubernetes API connection.

   - use in cluster configuration by using ``extra__kubernetes__in_cluster`` in connection
   - use custom config by providing path to the file using ``extra__kubernetes__kube_config_path``
   - use custom configuration by providing content of kubeconfig file via
       ``extra__kubernetes__kube_config`` in connection
   - use default config by providing no extras

   This hook check for configuration option in the above order. Once an option is present it will
   use this configuration.

   .. seealso::
       For more information about Kubernetes connection:
       :ref:`howto/connection:kubernetes`

   :param conn_id: the connection to Kubernetes cluster
   :type conn_id: str

   
   .. method:: get_conn(self)

      Returns kubernetes api session for use with requests



   
   .. method:: api_client(self)

      Cached Kubernetes API client



   
   .. method:: create_custom_object(self, group: str, version: str, plural: str, body: Union[str, dict], namespace: Optional[str] = None)

      Creates custom resource definition object in Kubernetes

      :param group: api group
      :type group: str
      :param version: api version
      :type version: str
      :param plural: api plural
      :type plural: str
      :param body: crd object definition
      :type body: Union[str, dict]
      :param namespace: kubernetes namespace
      :type namespace: str



   
   .. method:: get_custom_object(self, group: str, version: str, plural: str, name: str, namespace: Optional[str] = None)

      Get custom resource definition object from Kubernetes

      :param group: api group
      :type group: str
      :param version: api version
      :type version: str
      :param plural: api plural
      :type plural: str
      :param name: crd object name
      :type name: str
      :param namespace: kubernetes namespace
      :type namespace: str



   
   .. method:: get_namespace(self)

      Returns the namespace that defined in the connection



   
   .. method:: get_pod_log_stream(self, pod_name: str, container: Optional[str] = '', namespace: Optional[str] = None)

      Retrieves a log stream for a container in a kubernetes pod.

      :param pod_name: pod name
      :type pod_name: str
      :param container: container name
      :param namespace: kubernetes namespace
      :type namespace: str



   
   .. method:: get_pod_logs(self, pod_name: str, container: Optional[str] = '', namespace: Optional[str] = None)

      Retrieves a container's log from the specified pod.

      :param pod_name: pod name
      :type pod_name: str
      :param container: container name
      :param namespace: kubernetes namespace
      :type namespace: str




