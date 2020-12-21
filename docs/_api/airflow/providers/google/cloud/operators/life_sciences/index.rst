:mod:`airflow.providers.google.cloud.operators.life_sciences`
=============================================================

.. py:module:: airflow.providers.google.cloud.operators.life_sciences

.. autoapi-nested-parse::

   Operators that interact with Google Cloud Life Sciences service.



Module Contents
---------------

.. py:class:: LifeSciencesRunPipelineOperator(*, body: dict, location: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v2beta', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Runs a Life Sciences Pipeline

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:LifeSciencesRunPipelineOperator`

   :param body: The request body
   :type body: dict
   :param location: The location of the project
   :type location: str
   :param project_id: ID of the Google Cloud project if None then
       default project_id is used.
   :param project_id: str
   :param gcp_conn_id: The connection ID to use to connect to Google Cloud.
   :type gcp_conn_id: str
   :param api_version: API version used (for example v2beta).
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
      :annotation: = ['body', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




