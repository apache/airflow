:mod:`airflow.providers.google.marketing_platform.operators.analytics`
======================================================================

.. py:module:: airflow.providers.google.marketing_platform.operators.analytics

.. autoapi-nested-parse::

   This module contains Google Analytics 360 operators.



Module Contents
---------------

.. py:class:: GoogleAnalyticsListAccountsOperator(*, api_version: str = 'v3', gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists all accounts to which the user has access.

   .. seealso::
       Check official API docs:
       https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/accounts/list
       and for python client
       http://googleapis.github.io/google-api-python-client/docs/dyn/analytics_v3.management.accounts.html#list

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleAnalyticsListAccountsOperator`

   :param api_version: The version of the api that will be requested for example 'v3'.
   :type api_version: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
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
      :annotation: = ['api_version', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: GoogleAnalyticsGetAdsLinkOperator(*, account_id: str, web_property_ad_words_link_id: str, web_property_id: str, api_version: str = 'v3', gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Returns a web property-Google Ads link to which the user has access.

   .. seealso::
       Check official API docs:
       https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/webPropertyAdWordsLinks/get

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleAnalyticsGetAdsLinkOperator`

   :param account_id: ID of the account which the given web property belongs to.
   :type account_id: str
   :param web_property_ad_words_link_id: Web property-Google Ads link ID.
   :type web_property_ad_words_link_id: str
   :param web_property_id: Web property ID to retrieve the Google Ads link for.
   :type web_property_id: str
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
      :annotation: = ['api_version', 'gcp_conn_id', 'account_id', 'web_property_ad_words_link_id', 'web_property_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: GoogleAnalyticsRetrieveAdsLinksListOperator(*, account_id: str, web_property_id: str, api_version: str = 'v3', gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists webProperty-Google Ads links for a given web property

   .. seealso::
       Check official API docs:
       https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/webPropertyAdWordsLinks/list#http-request

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleAnalyticsRetrieveAdsLinksListOperator`

   :param account_id: ID of the account which the given web property belongs to.
   :type account_id: str
   :param web_property_id: Web property UA-string to retrieve the Google Ads links for.
   :type web_property_id: str
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
      :annotation: = ['api_version', 'gcp_conn_id', 'account_id', 'web_property_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: GoogleAnalyticsDataImportUploadOperator(*, storage_bucket: str, storage_name_object: str, account_id: str, web_property_id: str, custom_data_source_id: str, resumable_upload: bool = False, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, api_version: str = 'v3', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Take a file from Cloud Storage and uploads it to GA via data import API.

   :param storage_bucket: The Google cloud storage bucket where the file is stored.
   :type storage_bucket: str
   :param storage_name_object: The name of the object in the desired Google cloud
         storage bucket. (templated) If the destination points to an existing
         folder, the file will be taken from the specified folder.
   :type storage_name_object: str
   :param account_id: The GA account Id (long) to which the data upload belongs.
   :type account_id: str
   :param web_property_id: The web property UA-string associated with the upload.
   :type web_property_id: str
   :param custom_data_source_id: The id to which the data import belongs
   :type custom_data_source_id: str
   :param resumable_upload: flag to upload the file in a resumable fashion, using a
       series of at least two requests.
   :type resumable_upload: bool
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param api_version: The version of the api that will be requested for example 'v3'.
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
      :annotation: = ['storage_bucket', 'storage_name_object', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: GoogleAnalyticsDeletePreviousDataUploadsOperator(account_id: str, web_property_id: str, custom_data_source_id: str, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, api_version: str = 'v3', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes previous GA uploads to leave the latest file to control the size of the Data Set Quota.

   :param account_id: The GA account Id (long) to which the data upload belongs.
   :type account_id: str
   :param web_property_id: The web property UA-string associated with the upload.
   :type web_property_id: str
   :param custom_data_source_id: The id to which the data import belongs.
   :type custom_data_source_id: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param api_version: The version of the api that will be requested for example 'v3'.
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
      :annotation: = ['impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: GoogleAnalyticsModifyFileHeadersDataImportOperator(storage_bucket: str, storage_name_object: str, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, custom_dimension_header_mapping: Optional[Dict[str, str]] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   GA has a very particular naming convention for Data Import. Ability to
   prefix "ga:" to all column headers and also a dict to rename columns to
   match the custom dimension ID in GA i.e clientId : dimensionX.

   :param storage_bucket: The Google cloud storage bucket where the file is stored.
   :type storage_bucket: str
   :param storage_name_object: The name of the object in the desired Google cloud
         storage bucket. (templated) If the destination points to an existing
         folder, the file will be taken from the specified folder.
   :type storage_name_object: str
   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param custom_dimension_header_mapping: Dictionary to handle when uploading
         custom dimensions which have generic IDs ie. 'dimensionX' which are
         set by GA. Dictionary maps the current CSV header to GA ID which will
         be the new header for the CSV to upload to GA eg clientId : dimension1.
   :type custom_dimension_header_mapping: dict
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
      :annotation: = ['storage_bucket', 'storage_name_object', 'impersonation_chain']

      

   
   .. method:: _modify_column_headers(self, tmp_file_location: str, custom_dimension_header_mapping: Dict[str, str])



   
   .. method:: execute(self, context)




