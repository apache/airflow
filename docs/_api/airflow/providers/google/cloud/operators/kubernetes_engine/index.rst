:mod:`airflow.providers.google.cloud.operators.kubernetes_engine`
=================================================================

.. py:module:: airflow.providers.google.cloud.operators.kubernetes_engine

.. autoapi-nested-parse::

   This module contains Google Kubernetes Engine operators.



Module Contents
---------------

.. py:class:: GKEDeleteClusterOperator(*, name: str, location: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v2', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes the cluster, including the Kubernetes endpoint and all worker nodes.

   To delete a certain cluster, you must specify the ``project_id``, the ``name``
   of the cluster, the ``location`` that the cluster is in, and the ``task_id``.

   **Operator Creation**: ::

       operator = GKEClusterDeleteOperator(
                   task_id='cluster_delete',
                   project_id='my-project',
                   location='cluster-location'
                   name='cluster-name')

   .. seealso::
       For more detail about deleting clusters have a look at the reference:
       https://google-cloud-python.readthedocs.io/en/latest/container/gapic/v1/api.html#google.cloud.container_v1.ClusterManagerClient.delete_cluster

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GKEDeleteClusterOperator`

   :param project_id: The Google Developers Console [project ID or project number]
   :type project_id: str
   :param name: The name of the resource to delete, in this case cluster name
   :type name: str
   :param location: The name of the Google Compute Engine zone in which the cluster
       resides.
   :type location: str
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: The api version to use
   :type api_version: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'gcp_conn_id', 'name', 'location', 'api_version', 'impersonation_chain']

      

   
   .. method:: _check_input(self)



   
   .. method:: execute(self, context)




.. py:class:: GKECreateClusterOperator(*, location: str, body: Optional[Union[Dict, Cluster]], project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v2', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Create a Google Kubernetes Engine Cluster of specified dimensions
   The operator will wait until the cluster is created.

   The **minimum** required to define a cluster to create is:

   ``dict()`` ::
       cluster_def = {'name': 'my-cluster-name',
                      'initial_node_count': 1}

   or

   ``Cluster`` proto ::
       from google.cloud.container_v1.types import Cluster

       cluster_def = Cluster(name='my-cluster-name', initial_node_count=1)

   **Operator Creation**: ::

       operator = GKEClusterCreateOperator(
                   task_id='cluster_create',
                   project_id='my-project',
                   location='my-location'
                   body=cluster_def)

   .. seealso::
       For more detail on about creating clusters have a look at the reference:
       :class:`google.cloud.container_v1.types.Cluster`

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GKECreateClusterOperator`

   :param project_id: The Google Developers Console [project ID or project number]
   :type project_id: str
   :param location: The name of the Google Compute Engine zone in which the cluster
       resides.
   :type location: str
   :param body: The Cluster definition to create, can be protobuf or python dict, if
       dict it must match protobuf message Cluster
   :type body: dict or google.cloud.container_v1.types.Cluster
   :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: The api version to use
   :type api_version: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'gcp_conn_id', 'location', 'api_version', 'body', 'impersonation_chain']

      

   
   .. method:: _check_input(self)



   
   .. method:: execute(self, context)




.. data:: KUBE_CONFIG_ENV_VAR
   :annotation: = KUBECONFIG

   

.. py:class:: GKEStartPodOperator(*, location: str, cluster_name: str, use_internal_ip: bool = False, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', **kwargs)

   Bases: :class:`airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator`

   Executes a task in a Kubernetes pod in the specified Google Kubernetes
   Engine cluster

   This Operator assumes that the system has gcloud installed and has configured a
   connection id with a service account.

   The **minimum** required to define a cluster to create are the variables
   ``task_id``, ``project_id``, ``location``, ``cluster_name``, ``name``,
   ``namespace``, and ``image``

   .. seealso::
       For more detail about Kubernetes Engine authentication have a look at the reference:
       https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#internal_ip

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GKEStartPodOperator`

   :param location: The name of the Google Kubernetes Engine zone in which the
       cluster resides, e.g. 'us-central1-a'
   :type location: str
   :param cluster_name: The name of the Google Kubernetes Engine cluster the pod
       should be spawned in
   :type cluster_name: str
   :param use_internal_ip: Use the internal IP address as the endpoint.
   :param project_id: The Google Developers Console project id
   :type project_id: str
   :param gcp_conn_id: The google cloud connection id to use. This allows for
       users to specify a service account.
   :type gcp_conn_id: str

   .. attribute:: template_fields
      

      

   
   .. method:: execute(self, context)




