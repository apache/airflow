:mod:`airflow.providers.google.cloud.operators.dlp`
===================================================

.. py:module:: airflow.providers.google.cloud.operators.dlp

.. autoapi-nested-parse::

   This module contains various Google Cloud DLP operators
   which allow you to perform basic operations using
   Cloud DLP.



Module Contents
---------------

.. py:class:: CloudDLPCancelDLPJobOperator(*, dlp_job_id: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Starts asynchronous cancellation on a long-running DlpJob.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPCancelDLPJobOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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
      :annotation: = ['dlp_job_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPCreateDeidentifyTemplateOperator(*, organization_id: Optional[str] = None, project_id: Optional[str] = None, deidentify_template: Optional[Union[Dict, DeidentifyTemplate]] = None, template_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a DeidentifyTemplate for re-using frequently used configuration for
   de-identifying content, images, and storage.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPCreateDeidentifyTemplateOperator`

   :param organization_id: (Optional) The organization ID. Required to set this
       field if parent resource is an organization.
   :type organization_id: str
   :param project_id: (Optional) Google Cloud project ID where the
       DLP Instance exists. Only set this field if the parent resource is
       a project instead of an organization.
   :type project_id: str
   :param deidentify_template: (Optional) The DeidentifyTemplate to create.
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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate

   .. attribute:: template_fields
      :annotation: = ['organization_id', 'project_id', 'deidentify_template', 'template_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPCreateDLPJobOperator(*, project_id: Optional[str] = None, inspect_job: Optional[Union[Dict, InspectJobConfig]] = None, risk_job: Optional[Union[Dict, RiskAnalysisJobConfig]] = None, job_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, wait_until_finished: bool = True, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new job to inspect storage or calculate risk metrics.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPCreateDLPJobOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param wait_until_finished: (Optional) If true, it will keep polling the job state
       until it is set to DONE.
   :type wait_until_finished: bool
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.DlpJob

   .. attribute:: template_fields
      :annotation: = ['project_id', 'inspect_job', 'risk_job', 'job_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPCreateInspectTemplateOperator(*, organization_id: Optional[str] = None, project_id: Optional[str] = None, inspect_template: Optional[InspectTemplate] = None, template_id: Optional[Union[Dict, InspectTemplate]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates an InspectTemplate for re-using frequently used configuration for
   inspecting content, images, and storage.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPCreateInspectTemplateOperator`

   :param organization_id: (Optional) The organization ID. Required to set this
       field if parent resource is an organization.
   :type organization_id: str
   :param project_id: (Optional) Google Cloud project ID where the
       DLP Instance exists. Only set this field if the parent resource is
       a project instead of an organization.
   :type project_id: str
   :param inspect_template: (Optional) The InspectTemplate to create.
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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.InspectTemplate

   .. attribute:: template_fields
      :annotation: = ['organization_id', 'project_id', 'inspect_template', 'template_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPCreateJobTriggerOperator(*, project_id: Optional[str] = None, job_trigger: Optional[Union[Dict, JobTrigger]] = None, trigger_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a job trigger to run DLP actions such as scanning storage for sensitive
   information on a set schedule.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPCreateJobTriggerOperator`

   :param project_id: (Optional) Google Cloud project ID where the
       DLP Instance exists. If set to None or missing, the default
       project_id from the Google Cloud connection is used.
   :type project_id: str
   :param job_trigger: (Optional) The JobTrigger to create.
   :type job_trigger: dict or google.cloud.dlp_v2.types.JobTrigger
   :param trigger_id: (Optional) The JobTrigger ID.
   :type trigger_id: str
   :param retry: (Optional) A retry object used to retry requests.
       If None is specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request
       to complete. Note that if retry is specified, the timeout applies to each
       individual attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.JobTrigger

   .. attribute:: template_fields
      :annotation: = ['project_id', 'job_trigger', 'trigger_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPCreateStoredInfoTypeOperator(*, organization_id: Optional[str] = None, project_id: Optional[str] = None, config: Optional[StoredInfoTypeConfig] = None, stored_info_type_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a pre-built stored infoType to be used for inspection.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPCreateStoredInfoTypeOperator`

   :param organization_id: (Optional) The organization ID. Required to set this
       field if parent resource is an organization.
   :type organization_id: str
   :param project_id: (Optional) Google Cloud project ID where the
       DLP Instance exists. Only set this field if the parent resource is
       a project instead of an organization.
   :type project_id: str
   :param config: (Optional) The config for the StoredInfoType.
   :type config: dict or google.cloud.dlp_v2.types.StoredInfoTypeConfig
   :param stored_info_type_id: (Optional) The StoredInfoType ID.
   :type stored_info_type_id: str
   :param retry: (Optional) A retry object used to retry requests.
       If None is specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request
       to complete. Note that if retry is specified, the timeout applies to each
       individual attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.StoredInfoType

   .. attribute:: template_fields
      :annotation: = ['organization_id', 'project_id', 'config', 'stored_info_type_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPDeidentifyContentOperator(*, project_id: Optional[str] = None, deidentify_config: Optional[Union[Dict, DeidentifyConfig]] = None, inspect_config: Optional[Union[Dict, InspectConfig]] = None, item: Optional[Union[Dict, ContentItem]] = None, inspect_template_name: Optional[str] = None, deidentify_template_name: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   De-identifies potentially sensitive info from a ContentItem. This method has limits
   on input size and output size.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPDeidentifyContentOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.DeidentifyContentResponse

   .. attribute:: template_fields
      :annotation: = ['project_id', 'deidentify_config', 'inspect_config', 'item', 'inspect_template_name', 'deidentify_template_name', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPDeleteDeidentifyTemplateOperator(*, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a DeidentifyTemplate.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPDeleteDeidentifyTemplateOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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
      :annotation: = ['template_id', 'organization_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPDeleteDLPJobOperator(*, dlp_job_id: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a long-running DlpJob. This method indicates that the client is no longer
   interested in the DlpJob result. The job will be cancelled if possible.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPDeleteDLPJobOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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
      :annotation: = ['dlp_job_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPDeleteInspectTemplateOperator(*, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes an InspectTemplate.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPDeleteInspectTemplateOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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
      :annotation: = ['template_id', 'organization_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPDeleteJobTriggerOperator(*, job_trigger_id: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a job trigger.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPDeleteJobTriggerOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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
      :annotation: = ['job_trigger_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPDeleteStoredInfoTypeOperator(*, stored_info_type_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a stored infoType.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPDeleteStoredInfoTypeOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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
      :annotation: = ['stored_info_type_id', 'organization_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPGetDeidentifyTemplateOperator(*, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets a DeidentifyTemplate.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPGetDeidentifyTemplateOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate

   .. attribute:: template_fields
      :annotation: = ['template_id', 'organization_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPGetDLPJobOperator(*, dlp_job_id: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets the latest state of a long-running DlpJob.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPGetDLPJobOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.DlpJob

   .. attribute:: template_fields
      :annotation: = ['dlp_job_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPGetInspectTemplateOperator(*, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets an InspectTemplate.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPGetInspectTemplateOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.InspectTemplate

   .. attribute:: template_fields
      :annotation: = ['template_id', 'organization_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPGetDLPJobTriggerOperator(*, job_trigger_id: str, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets a job trigger.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPGetDLPJobTriggerOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.JobTrigger

   .. attribute:: template_fields
      :annotation: = ['job_trigger_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPGetStoredInfoTypeOperator(*, stored_info_type_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Gets a stored infoType.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPGetStoredInfoTypeOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.StoredInfoType

   .. attribute:: template_fields
      :annotation: = ['stored_info_type_id', 'organization_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPInspectContentOperator(*, project_id: Optional[str] = None, inspect_config: Optional[Union[Dict, InspectConfig]] = None, item: Optional[Union[Dict, ContentItem]] = None, inspect_template_name: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Finds potentially sensitive info in content. This method has limits on
   input size, processing time, and output size.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPInspectContentOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.tasks_v2.types.InspectContentResponse

   .. attribute:: template_fields
      :annotation: = ['project_id', 'inspect_config', 'item', 'inspect_template_name', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPListDeidentifyTemplatesOperator(*, organization_id: Optional[str] = None, project_id: Optional[str] = None, page_size: Optional[int] = None, order_by: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists DeidentifyTemplates.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPListDeidentifyTemplatesOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: list[google.cloud.dlp_v2.types.DeidentifyTemplate]

   .. attribute:: template_fields
      :annotation: = ['organization_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPListDLPJobsOperator(*, project_id: Optional[str] = None, results_filter: Optional[str] = None, page_size: Optional[int] = None, job_type: Optional[str] = None, order_by: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists DlpJobs that match the specified filter in the request.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPListDLPJobsOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: list[google.cloud.dlp_v2.types.DlpJob]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPListInfoTypesOperator(*, language_code: Optional[str] = None, results_filter: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Returns a list of the sensitive information types that the DLP API supports.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPListInfoTypesOperator`

   :param language_code: (Optional) Optional BCP-47 language code for localized infoType
       friendly names. If omitted, or if localized strings are not available, en-US
       strings will be returned.
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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: ListInfoTypesResponse

   .. attribute:: template_fields
      :annotation: = ['language_code', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPListInspectTemplatesOperator(*, organization_id: Optional[str] = None, project_id: Optional[str] = None, page_size: Optional[int] = None, order_by: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists InspectTemplates.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPListInspectTemplatesOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: list[google.cloud.dlp_v2.types.InspectTemplate]

   .. attribute:: template_fields
      :annotation: = ['organization_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPListJobTriggersOperator(*, project_id: Optional[str] = None, page_size: Optional[int] = None, order_by: Optional[str] = None, results_filter: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists job triggers.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPListJobTriggersOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: list[google.cloud.dlp_v2.types.JobTrigger]

   .. attribute:: template_fields
      :annotation: = ['project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPListStoredInfoTypesOperator(*, organization_id: Optional[str] = None, project_id: Optional[str] = None, page_size: Optional[int] = None, order_by: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Lists stored infoTypes.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPListStoredInfoTypesOperator`

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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: list[google.cloud.dlp_v2.types.StoredInfoType]

   .. attribute:: template_fields
      :annotation: = ['organization_id', 'project_id', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPRedactImageOperator(*, project_id: Optional[str] = None, inspect_config: Optional[Union[Dict, InspectConfig]] = None, image_redaction_configs: Optional[Union[Dict, RedactImageRequest.ImageRedactionConfig]] = None, include_findings: Optional[bool] = None, byte_item: Optional[Union[Dict, ByteContentItem]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Redacts potentially sensitive info from an image. This method has limits on
   input size, processing time, and output size.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPRedactImageOperator`

   :param project_id: (Optional) Google Cloud project ID where the
       DLP Instance exists. If set to None or missing, the default
       project_id from the Google Cloud connection is used.
   :type project_id: str
   :param inspect_config: (Optional) Configuration for the inspector. Items specified
       here will override the template referenced by the inspect_template_name argument.
   :type inspect_config: dict or google.cloud.dlp_v2.types.InspectConfig
   :param image_redaction_configs: (Optional) The configuration for specifying what
       content to redact from images.
   :type image_redaction_configs: list[dict] or
       list[google.cloud.dlp_v2.types.RedactImageRequest.ImageRedactionConfig]
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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.RedactImageResponse

   .. attribute:: template_fields
      :annotation: = ['project_id', 'inspect_config', 'image_redaction_configs', 'include_findings', 'byte_item', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPReidentifyContentOperator(*, project_id: Optional[str] = None, reidentify_config: Optional[Union[Dict, DeidentifyConfig]] = None, inspect_config: Optional[Union[Dict, InspectConfig]] = None, item: Optional[Union[Dict, ContentItem]] = None, inspect_template_name: Optional[str] = None, reidentify_template_name: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Re-identifies content that has been de-identified.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPReidentifyContentOperator`

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
       instance of DeidentifyTemplate. Any configuration directly specified in
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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.ReidentifyContentResponse

   .. attribute:: template_fields
      :annotation: = ['project_id', 'reidentify_config', 'inspect_config', 'item', 'inspect_template_name', 'reidentify_template_name', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPUpdateDeidentifyTemplateOperator(*, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, deidentify_template: Optional[Union[Dict, DeidentifyTemplate]] = None, update_mask: Optional[Union[Dict, FieldMask]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates the DeidentifyTemplate.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPUpdateDeidentifyTemplateOperator`

   :param template_id: The ID of deidentify template to be updated.
   :type template_id: str
   :param organization_id: (Optional) The organization ID. Required to set this
       field if parent resource is an organization.
   :type organization_id: str
   :param project_id: (Optional) Google Cloud project ID where the
       DLP Instance exists. Only set this field if the parent resource is
       a project instead of an organization.
   :type project_id: str
   :param deidentify_template: New DeidentifyTemplate value.
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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.DeidentifyTemplate

   .. attribute:: template_fields
      :annotation: = ['template_id', 'organization_id', 'project_id', 'deidentify_template', 'update_mask', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPUpdateInspectTemplateOperator(*, template_id: str, organization_id: Optional[str] = None, project_id: Optional[str] = None, inspect_template: Optional[Union[Dict, InspectTemplate]] = None, update_mask: Optional[Union[Dict, FieldMask]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates the InspectTemplate.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPUpdateInspectTemplateOperator`

   :param template_id: The ID of the inspect template to be updated.
   :type template_id: str
   :param organization_id: (Optional) The organization ID. Required to set this
       field if parent resource is an organization.
   :type organization_id: str
   :param project_id: (Optional) Google Cloud project ID where the
       DLP Instance exists. Only set this field if the parent resource is
       a project instead of an organization.
   :type project_id: str
   :param inspect_template: New InspectTemplate value.
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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.InspectTemplate

   .. attribute:: template_fields
      :annotation: = ['template_id', 'organization_id', 'project_id', 'inspect_template', 'update_mask', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPUpdateJobTriggerOperator(*, job_trigger_id, project_id: Optional[str] = None, job_trigger: Optional[JobTrigger] = None, update_mask: Optional[Union[Dict, FieldMask]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates a job trigger.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPUpdateJobTriggerOperator`

   :param job_trigger_id: The ID of the DLP job trigger to be updated.
   :type job_trigger_id: str
   :param project_id: (Optional) Google Cloud project ID where the
       DLP Instance exists. If set to None or missing, the default
       project_id from the Google Cloud connection is used.
   :type project_id: str
   :param job_trigger: New JobTrigger value.
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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.InspectTemplate

   .. attribute:: template_fields
      :annotation: = ['job_trigger_id', 'project_id', 'job_trigger', 'update_mask', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: CloudDLPUpdateStoredInfoTypeOperator(*, stored_info_type_id, organization_id: Optional[str] = None, project_id: Optional[str] = None, config: Optional[Union[Dict, StoredInfoTypeConfig]] = None, update_mask: Optional[Union[Dict, FieldMask]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, gcp_conn_id: str = 'google_cloud_default', impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Updates the stored infoType by creating a new version.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:CloudDLPUpdateStoredInfoTypeOperator`

   :param stored_info_type_id: The ID of the stored info type to be updated.
   :type stored_info_type_id: str
   :param organization_id: (Optional) The organization ID. Required to set this
       field if parent resource is an organization.
   :type organization_id: str
   :param project_id: (Optional) Google Cloud project ID where the
       DLP Instance exists. Only set this field if the parent resource is
       a project instead of an organization.
   :type project_id: str
   :param config: Updated configuration for the storedInfoType. If not provided, a new
       version of the storedInfoType will be created with the existing configuration.
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
   :type metadata: sequence[tuple[str, str]]]
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
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

   :rtype: google.cloud.dlp_v2.types.StoredInfoType

   .. attribute:: template_fields
      :annotation: = ['stored_info_type_id', 'organization_id', 'project_id', 'config', 'update_mask', 'gcp_conn_id', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




