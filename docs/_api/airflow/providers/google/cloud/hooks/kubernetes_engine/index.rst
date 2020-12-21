:mod:`airflow.providers.google.cloud.hooks.kubernetes_engine`
=============================================================

.. py:module:: airflow.providers.google.cloud.hooks.kubernetes_engine

.. autoapi-nested-parse::

   This module contains a Google Kubernetes Engine Hook.



Module Contents
---------------

.. data:: OPERATIONAL_POLL_INTERVAL
   :annotation: = 15

   

.. py:class:: GKEHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, location: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Kubernetes Engine APIs.

   All the methods in the hook where project_id is used must be called with
   keyword arguments rather than positional.

   
   .. method:: get_conn(self)

      Returns ClusterManagerCLinet object.

      :rtype: google.cloud.container_v1.ClusterManagerClient



   
   .. method:: get_client(self)



   
   .. method:: wait_for_operation(self, operation: Operation, project_id: Optional[str] = None)

      Given an operation, continuously fetches the status from Google Cloud until either
      completion or an error occurring

      :param operation: The Operation to wait for
      :type operation: google.cloud.container_V1.gapic.enums.Operation
      :param project_id: Google Cloud project ID
      :type project_id: str
      :return: A new, updated operation fetched from Google Cloud



   
   .. method:: get_operation(self, operation_name: str, project_id: Optional[str] = None)

      Fetches the operation from Google Cloud

      :param operation_name: Name of operation to fetch
      :type operation_name: str
      :param project_id: Google Cloud project ID
      :type project_id: str
      :return: The new, updated operation from Google Cloud



   
   .. staticmethod:: _append_label(cluster_proto: Cluster, key: str, val: str)

      Append labels to provided Cluster Protobuf

      Labels must fit the regex ``[a-z]([-a-z0-9]*[a-z0-9])?`` (current
       airflow version string follows semantic versioning spec: x.y.z).

      :param cluster_proto: The proto to append resource_label airflow
          version to
      :type cluster_proto: google.cloud.container_v1.types.Cluster
      :param key: The key label
      :type key: str
      :param val:
      :type val: str
      :return: The cluster proto updated with new label



   
   .. method:: delete_cluster(self, name: str, project_id: str, retry: Retry = DEFAULT, timeout: float = DEFAULT)

      Deletes the cluster, including the Kubernetes endpoint and all
      worker nodes. Firewalls and routes that were configured during
      cluster creation are also deleted. Other Google Compute Engine
      resources that might be in use by the cluster (e.g. load balancer
      resources) will not be deleted if they were not present at the
      initial create time.

      :param name: The name of the cluster to delete
      :type name: str
      :param project_id: Google Cloud project ID
      :type project_id: str
      :param retry: Retry object used to determine when/if to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to
          complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :return: The full url to the delete operation if successful, else None



   
   .. method:: create_cluster(self, cluster: Union[Dict, Cluster], project_id: str, retry: Retry = DEFAULT, timeout: float = DEFAULT)

      Creates a cluster, consisting of the specified number and type of Google Compute
      Engine instances.

      :param cluster: A Cluster protobuf or dict. If dict is provided, it must
          be of the same form as the protobuf message
          :class:`google.cloud.container_v1.types.Cluster`
      :type cluster: dict or google.cloud.container_v1.types.Cluster
      :param project_id: Google Cloud project ID
      :type project_id: str
      :param retry: A retry object (``google.api_core.retry.Retry``) used to
          retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to
          complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :return: The full url to the new, or existing, cluster
      :raises:
          ParseError: On JSON parsing problems when trying to convert dict
          AirflowException: cluster is not dict type nor Cluster proto type



   
   .. method:: get_cluster(self, name: str, project_id: str, retry: Retry = DEFAULT, timeout: float = DEFAULT)

      Gets details of specified cluster

      :param name: The name of the cluster to retrieve
      :type name: str
      :param project_id: Google Cloud project ID
      :type project_id: str
      :param retry: A retry object used to retry requests. If None is specified,
          requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: The amount of time, in seconds, to wait for the request to
          complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :return: google.cloud.container_v1.types.Cluster




