:mod:`airflow.providers.google.marketing_platform.operators.campaign_manager`
=============================================================================

.. py:module:: airflow.providers.google.marketing_platform.operators.campaign_manager

.. autoapi-nested-parse::

   This module contains Google CampaignManager operators.



Module Contents
---------------

.. py:class:: GoogleCampaignManagerDeleteReportOperator(*, profile_id: str, report_name: Optional[str] = None, report_id: Optional[str] = None, api_version: str = 'v3.3', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a report by its ID.

   .. seealso::
       Check official API docs:
       https://developers.google.com/doubleclick-advertisers/v3.3/reports/delete

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleCampaignManagerDeleteReportOperator`

   :param profile_id: The DFA user profile ID.
   :type profile_id: str
   :param report_name: The name of the report to delete.
   :type report_name: str
   :param report_id: The ID of the report.
   :type report_id: str
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
      :annotation: = ['profile_id', 'report_id', 'report_name', 'api_version', 'gcp_conn_id', 'delegate_to', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleCampaignManagerDownloadReportOperator(*, profile_id: str, report_id: str, file_id: str, bucket_name: str, report_name: Optional[str] = None, gzip: bool = True, chunk_size: int = 10 * 1024 * 1024, api_version: str = 'v3.3', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Retrieves a report and uploads it to GCS bucket.

   .. seealso::
       Check official API docs:
       https://developers.google.com/doubleclick-advertisers/v3.3/reports/files/get

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleCampaignManagerDownloadReportOperator`

   :param profile_id: The DFA user profile ID.
   :type profile_id: str
   :param report_id: The ID of the report.
   :type report_id: str
   :param file_id: The ID of the report file.
   :type file_id: str
   :param bucket_name: The bucket to upload to.
   :type bucket_name: str
   :param report_name: The report name to set when uploading the local file.
   :type report_name: str
   :param gzip: Option to compress local file or file data for upload
   :type gzip: bool
   :param chunk_size: File will be downloaded in chunks of this many bytes.
   :type chunk_size: int
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
      :annotation: = ['profile_id', 'report_id', 'file_id', 'bucket_name', 'report_name', 'chunk_size', 'api_version', 'gcp_conn_id', 'delegate_to', 'impersonation_chain']

      

   
   .. method:: _resolve_file_name(self, name: str)



   
   .. staticmethod:: _set_bucket_name(name: str)



   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleCampaignManagerInsertReportOperator(*, profile_id: str, report: Dict[str, Any], api_version: str = 'v3.3', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a report.

   .. seealso::
       Check official API docs:
       https://developers.google.com/doubleclick-advertisers/v3.3/reports/insert

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleCampaignManagerInsertReportOperator`

   :param profile_id: The DFA user profile ID.
   :type profile_id: str
   :param report: Report to be created.
   :type report: Dict[str, Any]
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
      :annotation: = ['profile_id', 'report', 'api_version', 'gcp_conn_id', 'delegate_to', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.json']

      

   
   .. method:: prepare_template(self)



   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleCampaignManagerRunReportOperator(*, profile_id: str, report_id: str, synchronous: bool = False, api_version: str = 'v3.3', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Runs a report.

   .. seealso::
       Check official API docs:
       https://developers.google.com/doubleclick-advertisers/v3.3/reports/run

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleCampaignManagerRunReportOperator`

   :param profile_id: The DFA profile ID.
   :type profile_id: str
   :param report_id: The ID of the report.
   :type report_id: str
   :param synchronous: If set and true, tries to run the report synchronously.
   :type synchronous: bool
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
      :annotation: = ['profile_id', 'report_id', 'synchronous', 'api_version', 'gcp_conn_id', 'delegate_to', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleCampaignManagerBatchInsertConversionsOperator(*, profile_id: str, conversions: List[Dict[str, Any]], encryption_entity_type: str, encryption_entity_id: int, encryption_source: str, max_failed_inserts: int = 0, api_version: str = 'v3.3', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Inserts conversions.

   .. seealso::
       Check official API docs:
       https://developers.google.com/doubleclick-advertisers/v3.3/conversions/batchinsert

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleCampaignManagerBatchInsertConversionsOperator`

   :param profile_id: User profile ID associated with this request.
   :type profile_id: str
   :param conversions: Conversations to insert, should by type of Conversation:
       https://developers.google.com/doubleclick-advertisers/v3.3/conversions#resource
   :type conversions: List[Dict[str, Any]]
   :param encryption_entity_type: The encryption entity type. This should match the encryption
       configuration for ad serving or Data Transfer.
   :type encryption_entity_type: str
   :param encryption_entity_id: The encryption entity ID. This should match the encryption
       configuration for ad serving or Data Transfer.
   :type encryption_entity_id: int
   :param encryption_source: Describes whether the encrypted cookie was received from ad serving
       (the %m macro) or from Data Transfer.
   :type encryption_source: str
   :param max_failed_inserts: The maximum number of conversions that failed to be inserted
   :type max_failed_inserts: int
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
      :annotation: = ['profile_id', 'conversions', 'encryption_entity_type', 'encryption_entity_id', 'encryption_source', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleCampaignManagerBatchUpdateConversionsOperator(*, profile_id: str, conversions: List[Dict[str, Any]], encryption_entity_type: str, encryption_entity_id: int, encryption_source: str, max_failed_updates: int = 0, api_version: str = 'v3.3', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates existing conversions.

   .. seealso::
       Check official API docs:
       https://developers.google.com/doubleclick-advertisers/v3.3/conversions/batchupdate

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleCampaignManagerBatchUpdateConversionsOperator`

   :param profile_id: User profile ID associated with this request.
   :type profile_id: str
   :param conversions: Conversations to update, should by type of Conversation:
       https://developers.google.com/doubleclick-advertisers/v3.3/conversions#resource
   :type conversions: List[Dict[str, Any]]
   :param encryption_entity_type: The encryption entity type. This should match the encryption
       configuration for ad serving or Data Transfer.
   :type encryption_entity_type: str
   :param encryption_entity_id: The encryption entity ID. This should match the encryption
       configuration for ad serving or Data Transfer.
   :type encryption_entity_id: int
   :param encryption_source: Describes whether the encrypted cookie was received from ad serving
       (the %m macro) or from Data Transfer.
   :type encryption_source: str
   :param max_failed_updates: The maximum number of conversions that failed to be updated
   :type max_failed_updates: int
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
      :annotation: = ['profile_id', 'conversions', 'encryption_entity_type', 'encryption_entity_id', 'encryption_source', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




