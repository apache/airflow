:mod:`airflow.providers.google.cloud.operators.datafusion`
==========================================================

.. py:module:: airflow.providers.google.cloud.operators.datafusion

.. autoapi-nested-parse::

   This module contains Google DataFusion operators.



Module Contents
---------------

.. py:class:: CloudDataFusionRestartInstanceOperator(*, instance_name: str, location: str, project_id: Optional[str] = None, api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Restart a single Data Fusion instance.
   At the end of an operation instance is fully restarted.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataFusionRestartInstanceOperator`

   :param instance_name: The name of the instance to restart.
   :type instance_name: str
   :param location: The Cloud Data Fusion location in which to handle the request.
   :type location: str
   :param project_id: The ID of the Google Cloud project that the instance belongs to.
   :type project_id: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['instance_name', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudDataFusionDeleteInstanceOperator(*, instance_name: str, location: str, project_id: Optional[str] = None, api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a single Date Fusion instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataFusionDeleteInstanceOperator`

   :param instance_name: The name of the instance to restart.
   :type instance_name: str
   :param location: The Cloud Data Fusion location in which to handle the request.
   :type location: str
   :param project_id: The ID of the Google Cloud project that the instance belongs to.
   :type project_id: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['instance_name', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudDataFusionCreateInstanceOperator(*, instance_name: str, instance: Dict[str, Any], location: str, project_id: Optional[str] = None, api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new Data Fusion instance in the specified project and location.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataFusionCreateInstanceOperator`

   :param instance_name: The name of the instance to create.
   :type instance_name: str
   :param instance: An instance of Instance.
       https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
   :type instance: Dict[str, Any]
   :param location: The Cloud Data Fusion location in which to handle the request.
   :type location: str
   :param project_id: The ID of the Google Cloud project that the instance belongs to.
   :type project_id: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['instance_name', 'instance', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudDataFusionUpdateInstanceOperator(*, instance_name: str, instance: Dict[str, Any], update_mask: str, location: str, project_id: Optional[str] = None, api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates a single Data Fusion instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataFusionUpdateInstanceOperator`

   :param instance_name: The name of the instance to create.
   :type instance_name: str
   :param instance: An instance of Instance.
       https://cloud.google.com/data-fusion/docs/reference/rest/v1beta1/projects.locations.instances#Instance
   :type instance: Dict[str, Any]
   :param update_mask: Field mask is used to specify the fields that the update will overwrite
       in an instance resource. The fields specified in the updateMask are relative to the resource,
       not the full request. A field will be overwritten if it is in the mask. If the user does not
       provide a mask, all the supported fields (labels and options currently) will be overwritten.
       A comma-separated list of fully qualified names of fields. Example: "user.displayName,photo".
       https://developers.google.com/protocol-buffers/docs/reference/google.protobuf?_ga=2.205612571.-968688242.1573564810#google.protobuf.FieldMask
   :type update_mask: str
   :param location: The Cloud Data Fusion location in which to handle the request.
   :type location: str
   :param project_id: The ID of the Google Cloud project that the instance belongs to.
   :type project_id: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['instance_name', 'instance', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudDataFusionGetInstanceOperator(*, instance_name: str, location: str, project_id: Optional[str] = None, api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets details of a single Data Fusion instance.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataFusionGetInstanceOperator`

   :param instance_name: The name of the instance.
   :type instance_name: str
   :param location: The Cloud Data Fusion location in which to handle the request.
   :type location: str
   :param project_id: The ID of the Google Cloud project that the instance belongs to.
   :type project_id: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['instance_name', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudDataFusionCreatePipelineOperator(*, pipeline_name: str, pipeline: Dict[str, Any], instance_name: str, location: str, namespace: str = 'default', project_id: Optional[str] = None, api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a Cloud Data Fusion pipeline.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataFusionCreatePipelineOperator`

   :param pipeline_name: Your pipeline name.
   :type pipeline_name: str
   :param pipeline: The pipeline definition. For more information check:
       https://docs.cdap.io/cdap/current/en/developer-manual/pipelines/developing-pipelines.html#pipeline-configuration-file-format
   :type pipeline: Dict[str, Any]
   :param instance_name: The name of the instance.
   :type instance_name: str
   :param location: The Cloud Data Fusion location in which to handle the request.
   :type location: str
   :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
       is always default. If your pipeline belongs to an Enterprise edition instance, you
       can create a namespace.
   :type namespace: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['instance_name', 'pipeline_name', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudDataFusionDeletePipelineOperator(*, pipeline_name: str, instance_name: str, location: str, version_id: Optional[str] = None, namespace: str = 'default', project_id: Optional[str] = None, api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a Cloud Data Fusion pipeline.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataFusionDeletePipelineOperator`

   :param pipeline_name: Your pipeline name.
   :type pipeline_name: str
   :param version_id: Version of pipeline to delete
   :type version_id: Optional[str]
   :param instance_name: The name of the instance.
   :type instance_name: str
   :param location: The Cloud Data Fusion location in which to handle the request.
   :type location: str
   :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
       is always default. If your pipeline belongs to an Enterprise edition instance, you
       can create a namespace.
   :type namespace: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['instance_name', 'version_id', 'pipeline_name', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudDataFusionListPipelinesOperator(*, instance_name: str, location: str, artifact_name: Optional[str] = None, artifact_version: Optional[str] = None, namespace: str = 'default', project_id: Optional[str] = None, api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists Cloud Data Fusion pipelines.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataFusionListPipelinesOperator`


   :param instance_name: The name of the instance.
   :type instance_name: str
   :param location: The Cloud Data Fusion location in which to handle the request.
   :type location: str
   :param artifact_version: Artifact version to filter instances
   :type artifact_version: Optional[str]
   :param artifact_name: Artifact name to filter instances
   :type artifact_name: Optional[str]
   :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
       is always default. If your pipeline belongs to an Enterprise edition instance, you
       can create a namespace.
   :type namespace: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['instance_name', 'artifact_name', 'artifact_version', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudDataFusionStartPipelineOperator(*, pipeline_name: str, instance_name: str, location: str, runtime_args: Optional[Dict[str, Any]] = None, success_states: Optional[List[str]] = None, namespace: str = 'default', pipeline_timeout: int = 10 * 60, project_id: Optional[str] = None, api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Starts a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataFusionStartPipelineOperator`

   :param pipeline_name: Your pipeline name.
   :type pipeline_name: str
   :param instance_name: The name of the instance.
   :type instance_name: str
   :param success_states: If provided the operator will wait for pipeline to be in one of
       the provided states.
   :type success_states: List[str]
   :param pipeline_timeout: How long (in seconds) operator should wait for the pipeline to be in one of
       ``success_states``. Works only if ``success_states`` are provided.
   :type pipeline_timeout: int
   :param location: The Cloud Data Fusion location in which to handle the request.
   :type location: str
   :param runtime_args: Optional runtime args to be passed to the pipeline
   :type runtime_args: dict
   :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
       is always default. If your pipeline belongs to an Enterprise edition instance, you
       can create a namespace.
   :type namespace: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['instance_name', 'pipeline_name', 'runtime_args', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: CloudDataFusionStopPipelineOperator(*, pipeline_name: str, instance_name: str, location: str, namespace: str = 'default', project_id: Optional[str] = None, api_version: str = 'v1beta1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Stops a Cloud Data Fusion pipeline. Works for both batch and stream pipelines.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDataFusionStopPipelineOperator`

   :param pipeline_name: Your pipeline name.
   :type pipeline_name: str
   :param instance_name: The name of the instance.
   :type instance_name: str
   :param location: The Cloud Data Fusion location in which to handle the request.
   :type location: str
   :param namespace: If your pipeline belongs to a Basic edition instance, the namespace ID
       is always default. If your pipeline belongs to an Enterprise edition instance, you
       can create a namespace.
   :type namespace: str
   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
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
      :annotation: = ['instance_name', 'pipeline_name', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




