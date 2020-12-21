:mod:`airflow.providers.google.cloud.hooks.gdm`
===============================================

.. py:module:: airflow.providers.google.cloud.hooks.gdm


Module Contents
---------------

.. py:class:: GoogleDeploymentManagerHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Interact with Google Cloud Deployment Manager using the Google Cloud connection.
   This allows for scheduled and programmatic inspection and deletion fo resources managed by GDM.

   
   .. method:: get_conn(self)

      Returns a Google Deployment Manager service object.

      :rtype: googleapiclient.discovery.Resource



   
   .. method:: list_deployments(self, project_id: Optional[str] = None, deployment_filter: Optional[str] = None, order_by: Optional[str] = None)

      Lists deployments in a google cloud project.

      :param project_id: The project ID for this request.
      :type project_id: str
      :param deployment_filter: A filter expression which limits resources returned in the response.
      :type deployment_filter: str
      :param order_by: A field name to order by, ex: "creationTimestamp desc"
      :type order_by: Optional[str]
      :rtype: list



   
   .. method:: delete_deployment(self, project_id: Optional[str], deployment: Optional[str] = None, delete_policy: Optional[str] = None)

      Deletes a deployment and all associated resources in a google cloud project.

      :param project_id: The project ID for this request.
      :type project_id: str
      :param deployment: The name of the deployment for this request.
      :type deployment: str
      :param delete_policy: Sets the policy to use for deleting resources. (ABANDON | DELETE)
      :type delete_policy: string

      :rtype: None




