:mod:`airflow.kubernetes.kube_client`
=====================================

.. py:module:: airflow.kubernetes.kube_client

.. autoapi-nested-parse::

   Client for kubernetes communication



Module Contents
---------------

.. data:: has_kubernetes
   :annotation: = True

   

.. function:: _enable_tcp_keepalive() -> None
   This function enables TCP keepalive mechanism. This prevents urllib3 connection
   to hang indefinitely when idle connection is time-outed on services like cloud
   load balancers or firewalls.

   See https://github.com/apache/airflow/pull/11406 for detailed explanation.
   Please ping @michalmisiewicz or @dimberman in the PR if you want to modify this function.


.. function:: get_kube_client(in_cluster: bool = conf.getboolean('kubernetes', 'in_cluster'), cluster_context: Optional[str] = None, config_file: Optional[str] = None) -> client.CoreV1Api
   Retrieves Kubernetes client

   :param in_cluster: whether we are in cluster
   :type in_cluster: bool
   :param cluster_context: context of the cluster
   :type cluster_context: str
   :param config_file: configuration file
   :type config_file: str
   :return kubernetes client
   :rtype client.CoreV1Api


