:mod:`airflow.providers.google.cloud.hooks.dlp`
===============================================

.. py:module:: airflow.providers.google.cloud.hooks.dlp

.. autoapi-nested-parse::

   This module contains a CloudDLPHook
   which allows you to connect to Google Cloud DLP service.



Module Contents
---------------

.. data:: DLP_JOB_PATH_PATTERN
   :annotation: = ^projects/[^/]+/dlpJobs/(?P<job>.*?)$

   

.. py:class:: CloudDLPHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Cloud Data Loss Prevention (DLP) APIs.
   Cloud DLP allows clients to detect the presence of Personally Identifiable
   Information (PII) and other privacy-sensitive data in user-supplied,
   unstructured data streams, like text blocks or images. The service also
   includes methods for sensitive data redaction and scheduling of data scans
   on Google Cloud based data sets.

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
       account from the list granting this role to the originating account.
   :type impersonation_chain: Union[str, Sequence[str]]

   
   .. method:: get_conn(self)

      Provides a client for interacting with the Cloud DLP API.

      :return: Google Cloud DLP API Client
      :rtype: google.cloud.dlp_v2.DlpServiceClient



   
   .. method:: cancel_dlp_job(self, dlp_job_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Starts asynchronous cancellation on a long-running DLP job.

      :param dlp_job_id: ID of the DLP job resource to be cancelled.
      :type dlp_job_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default project_id
          from the Google Cloud connection is used.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: create_deidentify_template(self, organization_id: Optional[str] = None, project_id: Optional[str] = None, deidentify_template: Optional[Union[dict, DeidentifyTemplate]] = None, template_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Creates a deidentify template for re-using frequently used configuration for
      de-identifying content, images, and storage.

      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param deidentify_template: (Optional) The de-identify template to create.
      :type deidentify_template: dict or google.cloud.dlp_v2.types.DeidentifyTemplate
      :param template_id: (Optional) The template ID.
      :type template_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate



   
   .. method:: create_dlp_job(self, project_id: str, inspect_job: Optional[Union[dict, InspectJobConfig]] = None, risk_job: Optional[Union[dict, RiskAnalysisJobConfig]] = None, job_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, wait_until_finished: bool = True, time_to_sleep_in_seconds: int = 60)

      Creates a new job to inspect storage or calculate risk metrics.

      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param inspect_job: (Optional) The configuration for the inspect job.
      :type inspect_job: dict or google.cloud.dlp_v2.types.InspectJobConfig
      :param risk_job: (Optional) The configuration for the risk job.
      :type risk_job: dict or google.cloud.dlp_v2.types.RiskAnalysisJobConfig
      :param job_id: (Optional) The job ID.
      :type job_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :param wait_until_finished: (Optional) If true, it will keep polling the job state
          until it is set to DONE.
      :type wait_until_finished: bool
      :rtype: google.cloud.dlp_v2.types.DlpJob
      :param time_to_sleep_in_seconds: (Optional) Time to sleep, in seconds, between active checks
          of the operation results. Defaults to 60.
      :type time_to_sleep_in_seconds: int



   
   .. method:: create_inspect_template(self, organization_id: Optional[str] = None, project_id: Optional[str] = None, inspect_template: Optional[Union[dict, InspectTemplate]] = None, template_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Creates an inspect template for re-using frequently used configuration for
      inspecting content, images, and storage.

      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param inspect_template: (Optional) The inspect template to create.
      :type inspect_template: dict or google.cloud.dlp_v2.types.InspectTemplate
      :param template_id: (Optional) The template ID.
      :type template_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.InspectTemplate



   
   .. method:: create_job_trigger(self, project_id: str, job_trigger: Optional[Union[dict, JobTrigger]] = None, trigger_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Creates a job trigger to run DLP actions such as scanning storage for sensitive
      information on a set schedule.

      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param job_trigger: (Optional) The job trigger to create.
      :type job_trigger: dict or google.cloud.dlp_v2.types.JobTrigger
      :param trigger_id: (Optional) The job trigger ID.
      :type trigger_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.JobTrigger



   
   .. method:: create_stored_info_type(self, organization_id: Optional[str] = None, project_id: Optional[str] = None, config: Optional[Union[dict, StoredInfoTypeConfig]] = None, stored_info_type_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Creates a pre-built stored info type to be used for inspection.

      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param config: (Optional) The config for the stored info type.
      :type config: dict or google.cloud.dlp_v2.types.StoredInfoTypeConfig
      :param stored_info_type_id: (Optional) The stored info type ID.
      :type stored_info_type_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.StoredInfoType



   
   .. method:: deidentify_content(self, project_id: str, deidentify_config: Optional[Union[dict, DeidentifyConfig]] = None, inspect_config: Optional[Union[dict, InspectConfig]] = None, item: Optional[Union[dict, ContentItem]] = None, inspect_template_name: Optional[str] = None, deidentify_template_name: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      De-identifies potentially sensitive info from a content item. This method has limits
      on input size and output size.

      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param deidentify_config: (Optional) Configuration for the de-identification of the
          content item. Items specified here will override the template referenced by the
          deidentify_template_name argument.
      :type deidentify_config: dict or google.cloud.dlp_v2.types.DeidentifyConfig
      :param inspect_config: (Optional) Configuration for the inspector. Items specified
          here will override the template referenced by the inspect_template_name argument.
      :type inspect_config: dict or google.cloud.dlp_v2.types.InspectConfig
      :param item: (Optional) The item to de-identify. Will be treated as text.
      :type item: dict or google.cloud.dlp_v2.types.ContentItem
      :param inspect_template_name: (Optional) Optional template to use. Any configuration
          directly specified in inspect_config will override those set in the template.
      :type inspect_template_name: str
      :param deidentify_template_name: (Optional) Optional template to use. Any
          configuration directly specified in deidentify_config will override those set
          in the template.
      :type deidentify_template_name: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.DeidentifyContentResponse



   
   .. method:: delete_deidentify_template(self, template_id, organization_id=None, project_id=None, retry=None, timeout=None, metadata=None)

      Deletes a deidentify template.

      :param template_id: The ID of deidentify template to be deleted.
      :type template_id: str
      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: delete_dlp_job(self, dlp_job_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Deletes a long-running DLP job. This method indicates that the client is no longer
      interested in the DLP job result. The job will be cancelled if possible.

      :param dlp_job_id: The ID of the DLP job resource to be cancelled.
      :type dlp_job_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: delete_inspect_template(self, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Deletes an inspect template.

      :param template_id: The ID of the inspect template to be deleted.
      :type template_id: str
      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: delete_job_trigger(self, job_trigger_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Deletes a job trigger.

      :param job_trigger_id: The ID of the DLP job trigger to be deleted.
      :type job_trigger_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: delete_stored_info_type(self, stored_info_type_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Deletes a stored info type.

      :param stored_info_type_id: The ID of the stored info type to be deleted.
      :type stored_info_type_id: str
      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]



   
   .. method:: get_deidentify_template(self, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets a deidentify template.

      :param template_id: The ID of deidentify template to be read.
      :type template_id: str
      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate



   
   .. method:: get_dlp_job(self, dlp_job_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets the latest state of a long-running Dlp Job.

      :param dlp_job_id: The ID of the DLP job resource to be read.
      :type dlp_job_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.DlpJob



   
   .. method:: get_inspect_template(self, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets an inspect template.

      :param template_id: The ID of inspect template to be read.
      :type template_id: str
      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.InspectTemplate



   
   .. method:: get_job_trigger(self, job_trigger_id: str, project_id: str, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets a DLP job trigger.

      :param job_trigger_id: The ID of the DLP job trigger to be read.
      :type job_trigger_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.JobTrigger



   
   .. method:: get_stored_info_type(self, stored_info_type_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Gets a stored info type.

      :param stored_info_type_id: The ID of the stored info type to be read.
      :type stored_info_type_id: str
      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.StoredInfoType



   
   .. method:: inspect_content(self, project_id: str, inspect_config: Optional[Union[dict, InspectConfig]] = None, item: Optional[Union[dict, ContentItem]] = None, inspect_template_name: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Finds potentially sensitive info in content. This method has limits on input size,
      processing time, and output size.

      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param inspect_config: (Optional) Configuration for the inspector. Items specified
          here will override the template referenced by the inspect_template_name argument.
      :type inspect_config: dict or google.cloud.dlp_v2.types.InspectConfig
      :param item: (Optional) The item to de-identify. Will be treated as text.
      :type item: dict or google.cloud.dlp_v2.types.ContentItem
      :param inspect_template_name: (Optional) Optional template to use. Any configuration
          directly specified in inspect_config will override those set in the template.
      :type inspect_template_name: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.InspectContentResponse



   
   .. method:: list_deidentify_templates(self, organization_id: Optional[str] = None, project_id: Optional[str] = None, page_size: Optional[int] = None, order_by: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists deidentify templates.

      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param page_size: (Optional) The maximum number of resources contained in the
          underlying API response.
      :type page_size: int
      :param order_by: (Optional) Optional comma separated list of fields to order by,
          followed by asc or desc postfix.
      :type order_by: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: List[google.cloud.dlp_v2.types.DeidentifyTemplate]



   
   .. method:: list_dlp_jobs(self, project_id: str, results_filter: Optional[str] = None, page_size: Optional[int] = None, job_type: Optional[str] = None, order_by: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists DLP jobs that match the specified filter in the request.

      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param results_filter: (Optional) Filter used to specify a subset of results.
      :type results_filter: str
      :param page_size: (Optional) The maximum number of resources contained in the
          underlying API response.
      :type page_size: int
      :param job_type: (Optional) The type of job.
      :type job_type: str
      :param order_by: (Optional) Optional comma separated list of fields to order by,
          followed by asc or desc postfix.
      :type order_by: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: List[google.cloud.dlp_v2.types.DlpJob]



   
   .. method:: list_info_types(self, language_code: Optional[str] = None, results_filter: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Returns a list of the sensitive information types that the DLP API supports.

      :param language_code: (Optional) Optional BCP-47 language code for localized info
          type friendly names. If omitted, or if localized strings are not available,
          en-US strings will be returned.
      :type language_code: str
      :param results_filter: (Optional) Filter used to specify a subset of results.
      :type results_filter: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.ListInfoTypesResponse



   
   .. method:: list_inspect_templates(self, organization_id: Optional[str] = None, project_id: Optional[str] = None, page_size: Optional[int] = None, order_by: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists inspect templates.

      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param page_size: (Optional) The maximum number of resources contained in the
          underlying API response.
      :type page_size: int
      :param order_by: (Optional) Optional comma separated list of fields to order by,
          followed by asc or desc postfix.
      :type order_by: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: List[google.cloud.dlp_v2.types.InspectTemplate]



   
   .. method:: list_job_triggers(self, project_id: str, page_size: Optional[int] = None, order_by: Optional[str] = None, results_filter: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists job triggers.

      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param page_size: (Optional) The maximum number of resources contained in the
          underlying API response.
      :type page_size: int
      :param order_by: (Optional) Optional comma separated list of fields to order by,
          followed by asc or desc postfix.
      :type order_by: str
      :param results_filter: (Optional) Filter used to specify a subset of results.
      :type results_filter: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: List[google.cloud.dlp_v2.types.JobTrigger]



   
   .. method:: list_stored_info_types(self, organization_id: Optional[str] = None, project_id: Optional[str] = None, page_size: Optional[int] = None, order_by: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Lists stored info types.

      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param page_size: (Optional) The maximum number of resources contained in the
          underlying API response.
      :type page_size: int
      :param order_by: (Optional) Optional comma separated list of fields to order by,
          followed by asc or desc postfix.
      :type order_by: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: List[google.cloud.dlp_v2.types.StoredInfoType]



   
   .. method:: redact_image(self, project_id: str, inspect_config: Optional[Union[dict, InspectConfig]] = None, image_redaction_configs: Optional[Union[List[dict], List[RedactImageRequest.ImageRedactionConfig]]] = None, include_findings: Optional[bool] = None, byte_item: Optional[Union[dict, ByteContentItem]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Redacts potentially sensitive info from an image. This method has limits on
      input size, processing time, and output size.

      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param inspect_config: (Optional) Configuration for the inspector. Items specified
          here will override the template referenced by the inspect_template_name argument.
      :type inspect_config: dict or google.cloud.dlp_v2.types.InspectConfig
      :param image_redaction_configs: (Optional) The configuration for specifying what
          content to redact from images.
      :type image_redaction_configs: List[dict] or
          List[google.cloud.dlp_v2.types.RedactImageRequest.ImageRedactionConfig]
      :param include_findings: (Optional) Whether the response should include findings
          along with the redacted image.
      :type include_findings: bool
      :param byte_item: (Optional) The content must be PNG, JPEG, SVG or BMP.
      :type byte_item: dict or google.cloud.dlp_v2.types.ByteContentItem
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.RedactImageResponse



   
   .. method:: reidentify_content(self, project_id: str, reidentify_config: Optional[Union[dict, DeidentifyConfig]] = None, inspect_config: Optional[Union[dict, InspectConfig]] = None, item: Optional[Union[dict, ContentItem]] = None, inspect_template_name: Optional[str] = None, reidentify_template_name: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Re-identifies content that has been de-identified.

      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param reidentify_config: (Optional) Configuration for the re-identification of
          the content item.
      :type reidentify_config: dict or google.cloud.dlp_v2.types.DeidentifyConfig
      :param inspect_config: (Optional) Configuration for the inspector.
      :type inspect_config: dict or google.cloud.dlp_v2.types.InspectConfig
      :param item: (Optional) The item to re-identify. Will be treated as text.
      :type item: dict or google.cloud.dlp_v2.types.ContentItem
      :param inspect_template_name: (Optional) Optional template to use. Any configuration
          directly specified in inspect_config will override those set in the template.
      :type inspect_template_name: str
      :param reidentify_template_name: (Optional) Optional template to use. References an
          instance of deidentify template. Any configuration directly specified in
          reidentify_config or inspect_config will override those set in the template.
      :type reidentify_template_name: str
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.ReidentifyContentResponse



   
   .. method:: update_deidentify_template(self, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, deidentify_template: Optional[Union[dict, DeidentifyTemplate]] = None, update_mask: Optional[Union[dict, FieldMask]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Updates the deidentify template.

      :param template_id: The ID of deidentify template to be updated.
      :type template_id: str
      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param deidentify_template: New deidentify template value.
      :type deidentify_template: dict or google.cloud.dlp_v2.types.DeidentifyTemplate
      :param update_mask: Mask to control which fields get updated.
      :type update_mask: dict or google.cloud.dlp_v2.types.FieldMask
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate



   
   .. method:: update_inspect_template(self, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, inspect_template: Optional[Union[dict, InspectTemplate]] = None, update_mask: Optional[Union[dict, FieldMask]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Updates the inspect template.

      :param template_id: The ID of the inspect template to be updated.
      :type template_id: str
      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param inspect_template: New inspect template value.
      :type inspect_template: dict or google.cloud.dlp_v2.types.InspectTemplate
      :param update_mask: Mask to control which fields get updated.
      :type update_mask: dict or google.cloud.dlp_v2.types.FieldMask
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.InspectTemplate



   
   .. method:: update_job_trigger(self, job_trigger_id: str, project_id: str, job_trigger: Optional[Union[dict, JobTrigger]] = None, update_mask: Optional[Union[dict, FieldMask]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Updates a job trigger.

      :param job_trigger_id: The ID of the DLP job trigger to be updated.
      :type job_trigger_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. If set to None or missing, the default
          project_id from the Google Cloud connection is used.
      :type project_id: str
      :param job_trigger: New job trigger value.
      :type job_trigger: dict or google.cloud.dlp_v2.types.JobTrigger
      :param update_mask: Mask to control which fields get updated.
      :type update_mask: dict or google.cloud.dlp_v2.types.FieldMask
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.JobTrigger



   
   .. method:: update_stored_info_type(self, stored_info_type_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, config: Optional[Union[dict, StoredInfoTypeConfig]] = None, update_mask: Optional[Union[dict, FieldMask]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None)

      Updates the stored info type by creating a new version.

      :param stored_info_type_id: The ID of the stored info type to be updated.
      :type stored_info_type_id: str
      :param organization_id: (Optional) The organization ID. Required to set this
          field if parent resource is an organization.
      :type organization_id: str
      :param project_id: (Optional) Google Cloud project ID where the
          DLP Instance exists. Only set this field if the parent resource is
          a project instead of an organization.
      :type project_id: str
      :param config: Updated configuration for the stored info type. If not provided, a new
          version of the stored info type will be created with the existing configuration.
      :type config: dict or google.cloud.dlp_v2.types.StoredInfoTypeConfig
      :param update_mask: Mask to control which fields get updated.
      :type update_mask: dict or google.cloud.dlp_v2.types.FieldMask
      :param retry: (Optional) A retry object used to retry requests.
          If None is specified, requests will not be retried.
      :type retry: google.api_core.retry.Retry
      :param timeout: (Optional) The amount of time, in seconds, to wait for the request
          to complete. Note that if retry is specified, the timeout applies to each
          individual attempt.
      :type timeout: float
      :param metadata: (Optional) Additional metadata that is provided to the method.
      :type metadata: Sequence[Tuple[str, str]]
      :rtype: google.cloud.dlp_v2.types.StoredInfoType




