:mod:`airflow.providers.google.cloud.operators.compute`
=======================================================

.. py:module:: airflow.providers.google.cloud.operators.compute

.. autoapi-nested-parse::

   This module contains Google Compute Engine operators.



Module Contents
---------------

.. py:class:: ComputeEngineBaseOperator(*, zone: str, resource_id: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Abstract base operator for Google Compute Engine operators to inherit from.

   
   .. method:: _validate_inputs(self)



   
   .. method:: execute(self, context)




.. py:class:: ComputeEngineStartInstanceOperator(*, zone: str, resource_id: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator`

   Starts an instance in Google Compute Engine.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:ComputeEngineStartInstanceOperator`

   :param zone: Google Cloud zone where the instance exists.
   :type zone: str
   :param resource_id: Name of the Compute Engine instance resource.
   :type resource_id: str
   :param project_id: Optional, Google Cloud Project ID where the Compute
       Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
       connection is used.
   :type project_id: str
   :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
       Defaults to 'google_cloud_default'.
   :type gcp_conn_id: str
   :param api_version: Optional, API version used (for example v1 - or beta). Defaults
       to v1.
   :type api_version: str
   :param validate_body: Optional, If set to False, body validation is not performed.
       Defaults to False.
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
      :annotation: = ['project_id', 'zone', 'resource_id', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: ComputeEngineStopInstanceOperator(*, zone: str, resource_id: str, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator`

   Stops an instance in Google Compute Engine.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:ComputeEngineStopInstanceOperator`

   :param zone: Google Cloud zone where the instance exists.
   :type zone: str
   :param resource_id: Name of the Compute Engine instance resource.
   :type resource_id: str
   :param project_id: Optional, Google Cloud Project ID where the Compute
       Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
       connection is used.
   :type project_id: str
   :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
       Defaults to 'google_cloud_default'.
   :type gcp_conn_id: str
   :param api_version: Optional, API version used (for example v1 - or beta). Defaults
       to v1.
   :type api_version: str
   :param validate_body: Optional, If set to False, body validation is not performed.
       Defaults to False.
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
      :annotation: = ['project_id', 'zone', 'resource_id', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. data:: SET_MACHINE_TYPE_VALIDATION_SPECIFICATION
   

   

.. py:class:: ComputeEngineSetMachineTypeOperator(*, zone: str, resource_id: str, body: dict, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', validate_body: bool = True, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator`

   Changes the machine type for a stopped instance to the machine type specified in
       the request.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:ComputeEngineSetMachineTypeOperator`

   :param zone: Google Cloud zone where the instance exists.
   :type zone: str
   :param resource_id: Name of the Compute Engine instance resource.
   :type resource_id: str
   :param body: Body required by the Compute Engine setMachineType API, as described in
       https://cloud.google.com/compute/docs/reference/rest/v1/instances/setMachineType#request-body
   :type body: dict
   :param project_id: Optional, Google Cloud Project ID where the Compute
       Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
       connection is used.
   :type project_id: str
   :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
       Defaults to 'google_cloud_default'.
   :type gcp_conn_id: str
   :param api_version: Optional, API version used (for example v1 - or beta). Defaults
       to v1.
   :type api_version: str
   :param validate_body: Optional, If set to False, body validation is not performed.
       Defaults to False.
   :type validate_body: bool
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
      :annotation: = ['project_id', 'zone', 'resource_id', 'body', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_all_body_fields(self)



   
   .. method:: execute(self, context)




.. data:: GCE_INSTANCE_TEMPLATE_VALIDATION_PATCH_SPECIFICATION
   :annotation: :List[Dict[str, Any]]

   

.. data:: GCE_INSTANCE_TEMPLATE_FIELDS_TO_SANITIZE
   :annotation: = ['kind', 'id', 'name', 'creationTimestamp', 'properties.disks.sha256', 'properties.disks.kind', 'properties.disks.sourceImageEncryptionKey.sha256', 'properties.disks.index', 'properties.disks.licenses', 'properties.networkInterfaces.kind', 'properties.networkInterfaces.accessConfigs.kind', 'properties.networkInterfaces.name', 'properties.metadata.kind', 'selfLink']

   

.. py:class:: ComputeEngineCopyInstanceTemplateOperator(*, resource_id: str, body_patch: dict, project_id: Optional[str] = None, request_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v1', validate_body: bool = True, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator`

   Copies the instance template, applying specified changes.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:ComputeEngineCopyInstanceTemplateOperator`

   :param resource_id: Name of the Instance Template
   :type resource_id: str
   :param body_patch: Patch to the body of instanceTemplates object following rfc7386
       PATCH semantics. The body_patch content follows
       https://cloud.google.com/compute/docs/reference/rest/v1/instanceTemplates
       Name field is required as we need to rename the template,
       all the other fields are optional. It is important to follow PATCH semantics
       - arrays are replaced fully, so if you need to update an array you should
       provide the whole target array as patch element.
   :type body_patch: dict
   :param project_id: Optional, Google Cloud Project ID where the Compute
       Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
       connection is used.
   :type project_id: str
   :param request_id: Optional, unique request_id that you might add to achieve
       full idempotence (for example when client call times out repeating the request
       with the same request id will not create a new instance template again).
       It should be in UUID format as defined in RFC 4122.
   :type request_id: str
   :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
       Defaults to 'google_cloud_default'.
   :type gcp_conn_id: str
   :param api_version: Optional, API version used (for example v1 - or beta). Defaults
       to v1.
   :type api_version: str
   :param validate_body: Optional, If set to False, body validation is not performed.
       Defaults to False.
   :type validate_body: bool
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
      :annotation: = ['project_id', 'resource_id', 'request_id', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _validate_all_body_fields(self)



   
   .. method:: execute(self, context)




.. py:class:: ComputeEngineInstanceGroupUpdateManagerTemplateOperator(*, resource_id: str, zone: str, source_template: str, destination_template: str, project_id: Optional[str] = None, update_policy: Optional[Dict[str, Any]] = None, request_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', api_version='beta', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator`

   Patches the Instance Group Manager, replacing source template URL with the
   destination one. API V1 does not have update/patch operations for Instance
   Group Manager, so you must use beta or newer API version. Beta is the default.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:ComputeEngineInstanceGroupUpdateManagerTemplateOperator`

   :param resource_id: Name of the Instance Group Manager
   :type resource_id: str
   :param zone: Google Cloud zone where the Instance Group Manager exists.
   :type zone: str
   :param source_template: URL of the template to replace.
   :type source_template: str
   :param destination_template: URL of the target template.
   :type destination_template: str
   :param project_id: Optional, Google Cloud Project ID where the Compute
       Engine Instance exists. If set to None or missing, the default project_id from the Google Cloud
       connection is used.
   :type project_id: str
   :param request_id: Optional, unique request_id that you might add to achieve
       full idempotence (for example when client call times out repeating the request
       with the same request id will not create a new instance template again).
       It should be in UUID format as defined in RFC 4122.
   :type request_id: str
   :param gcp_conn_id: Optional, The connection ID used to connect to Google Cloud.
       Defaults to 'google_cloud_default'.
   :type gcp_conn_id: str
   :param api_version: Optional, API version used (for example v1 - or beta). Defaults
       to v1.
   :type api_version: str
   :param validate_body: Optional, If set to False, body validation is not performed.
       Defaults to False.
   :type validate_body: bool
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
      :annotation: = ['project_id', 'resource_id', 'zone', 'request_id', 'source_template', 'destination_template', 'gcp_conn_id', 'api_version', 'impersonation_chain']

      

   
   .. method:: _possibly_replace_template(self, dictionary: dict)



   
   .. method:: execute(self, context)




