:mod:`airflow.providers.google.marketing_platform.operators.display_video`
==========================================================================

.. py:module:: airflow.providers.google.marketing_platform.operators.display_video

.. autoapi-nested-parse::

   This module contains Google DisplayVideo operators.



Module Contents
---------------

.. py:class:: GoogleDisplayVideo360CreateReportOperator(*, body: Dict[str, Any], api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a query.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleDisplayVideo360CreateReportOperator`

   .. seealso::
       Check also the official API docs:
       `https://developers.google.com/bid-manager/v1/queries/createquery`

   :param body: Report object passed to the request's body as described here:
       https://developers.google.com/bid-manager/v1/queries#resource
   :type body: Dict[str, Any]
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
      :annotation: = ['body', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.json']

      

   
   .. method:: prepare_template(self)



   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleDisplayVideo360DeleteReportOperator(*, report_id: Optional[str] = None, report_name: Optional[str] = None, api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a stored query as well as the associated stored reports.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleDisplayVideo360DeleteReportOperator`

   .. seealso::
       Check also the official API docs:
       `https://developers.google.com/bid-manager/v1/queries/deletequery`

   :param report_id: Report ID to delete.
   :type report_id: str
   :param report_name: Name of the report to delete.
   :type report_name: str
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
      :annotation: = ['report_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleDisplayVideo360DownloadReportOperator(*, report_id: str, bucket_name: str, report_name: Optional[str] = None, gzip: bool = True, chunk_size: int = 10 * 1024 * 1024, api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Retrieves a stored query.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleDisplayVideo360DownloadReportOperator`

   .. seealso::
       Check also the official API docs:
       `https://developers.google.com/bid-manager/v1/queries/getquery`

   :param report_id: Report ID to retrieve.
   :type report_id: str
   :param bucket_name: The bucket to upload to.
   :type bucket_name: str
   :param report_name: The report name to set when uploading the local file.
   :type report_name: str
   :param chunk_size: File will be downloaded in chunks of this many bytes.
   :type chunk_size: int
   :param gzip: Option to compress local file or file data for upload
   :type gzip: bool
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
      :annotation: = ['report_id', 'bucket_name', 'report_name', 'impersonation_chain']

      

   
   .. method:: _resolve_file_name(self, name: str)



   
   .. staticmethod:: _set_bucket_name(name: str)



   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleDisplayVideo360RunReportOperator(*, report_id: str, params: Dict[str, Any], api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Runs a stored query to generate a report.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleDisplayVideo360RunReportOperator`

   .. seealso::
       Check also the official API docs:
       `https://developers.google.com/bid-manager/v1/queries/runquery`

   :param report_id: Report ID to run.
   :type report_id: str
   :param params: Parameters for running a report as described here:
       https://developers.google.com/bid-manager/v1/queries/runquery
   :type params: Dict[str, Any]
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
      :annotation: = ['report_id', 'params', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleDisplayVideo360DownloadLineItemsOperator(*, request_body: Dict[str, Any], bucket_name: str, object_name: str, gzip: bool = False, api_version: str = 'v1.1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Retrieves line items in CSV format.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleDisplayVideo360DownloadLineItemsOperator`

   .. seealso::
       Check also the official API docs:
       `https://developers.google.com/bid-manager/v1.1/lineitems/downloadlineitems`

   :param request_body: dictionary with parameters that should be passed into.
           More information about it can be found here:
           https://developers.google.com/bid-manager/v1.1/lineitems/downloadlineitems
   :type request_body: Dict[str, Any],

   .. attribute:: template_fields
      :annotation: = ['request_body', 'bucket_name', 'object_name', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleDisplayVideo360UploadLineItemsOperator(*, bucket_name: str, object_name: str, api_version: str = 'v1.1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Uploads line items in CSV format.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleDisplayVideo360UploadLineItemsOperator`

   .. seealso::
       Check also the official API docs:
       `https://developers.google.com/bid-manager/v1.1/lineitems/uploadlineitems`

   :param request_body: request to upload line items.
   :type request_body: Dict[str, Any]
   :param bucket_name: The bucket form data is downloaded.
   :type bucket_name: str
   :param object_name: The object to fetch.
   :type object_name: str,
   :param filename: The filename to fetch.
   :type filename: str,
   :param dry_run: Upload status without actually persisting the line items.
   :type filename: str,

   .. attribute:: template_fields
      :annotation: = ['bucket_name', 'object_name', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleDisplayVideo360CreateSDFDownloadTaskOperator(*, body_request: Dict[str, Any], api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates SDF operation task.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleDisplayVideo360CreateSDFDownloadTaskOperator`

   .. seealso::
       Check also the official API docs:
       `https://developers.google.com/display-video/api/reference/rest`

   :param version: The SDF version of the downloaded file..
   :type version: str
   :param partner_id: The ID of the partner to download SDF for.
   :type partner_id: str
   :param advertiser_id: The ID of the advertiser to download SDF for.
   :type advertiser_id: str
   :param parent_entity_filter: Filters on selected file types.
   :type parent_entity_filter: Dict[str, Any]
   :param id_filter: Filters on entities by their entity IDs.
   :type id_filter: Dict[str, Any]
   :param inventory_source_filter: Filters on Inventory Sources by their IDs.
   :type inventory_source_filter: Dict[str, Any]
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
      :annotation: = ['body_request', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




.. py:class:: GoogleDisplayVideo360SDFtoGCSOperator(*, operation_name: str, bucket_name: str, object_name: str, gzip: bool = False, api_version: str = 'v1', gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Download SDF media and save it in the Google Cloud Storage.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GoogleDisplayVideo360SDFtoGCSOperator`

   .. seealso::
       Check also the official API docs:
       `https://developers.google.com/display-video/api/reference/rest`

   :param version: The SDF version of the downloaded file..
   :type version: str
   :param partner_id: The ID of the partner to download SDF for.
   :type partner_id: str
   :param advertiser_id: The ID of the advertiser to download SDF for.
   :type advertiser_id: str
   :param parent_entity_filter: Filters on selected file types.
   :type parent_entity_filter: Dict[str, Any]
   :param id_filter: Filters on entities by their entity IDs.
   :type id_filter: Dict[str, Any]
   :param inventory_source_filter: Filters on Inventory Sources by their IDs.
   :type inventory_source_filter: Dict[str, Any]
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
      :annotation: = ['operation_name', 'bucket_name', 'object_name', 'impersonation_chain']

      

   
   .. method:: execute(self, context: dict)




