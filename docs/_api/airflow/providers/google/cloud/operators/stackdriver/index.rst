:mod:`airflow.providers.google.cloud.operators.stackdriver`
===========================================================

.. py:module:: airflow.providers.google.cloud.operators.stackdriver


Module Contents
---------------

.. py:class:: StackdriverListAlertPoliciesOperator(*, format_: Optional[str] = None, filter_: Optional[str] = None, order_by: Optional[str] = None, page_size: Optional[int] = None, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Fetches all the Alert Policies identified by the filter passed as
   filter parameter. The desired return type can be specified by the
   format parameter, the supported formats are "dict", "json" and None
   which returns python dictionary, stringified JSON and protobuf
   respectively.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StackdriverListAlertPoliciesOperator`

   :param format_: (Optional) Desired output format of the result. The
       supported formats are "dict", "json" and None which returns
       python dictionary, stringified JSON and protobuf respectively.
   :type format_: str
   :param filter_:  If provided, this field specifies the criteria that must be met by alert
       policies to be included in the response.
       For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
   :type filter_: str
   :param order_by: A comma-separated list of fields by which to sort the result.
       Supports the same set of field references as the ``filter`` field. Entries
       can be prefixed with a minus sign to sort by the field in descending order.
       For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
   :type order_by: str
   :param page_size: The maximum number of resources contained in the
       underlying API response. If page streaming is performed per-
       resource, this parameter does not affect the return value. If page
       streaming is performed per-page, this determines the maximum number
       of resources in a page.
   :type page_size: int
   :param retry: A retry object used to retry requests. If ``None`` is
       specified, requests will be retried using a default configuration.
   :type retry: str
   :param timeout: The amount of time, in seconds, to wait
       for the request to complete. Note that if ``retry`` is
       specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param project_id: The project to fetch alerts from.
   :type project_id: str
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
      :annotation: = ['filter_', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #e5ffcc

      

   
   .. method:: execute(self, context)




.. py:class:: StackdriverEnableAlertPoliciesOperator(*, filter_: Optional[str] = None, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Enables one or more disabled alerting policies identified by filter
   parameter. Inoperative in case the policy is already enabled.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StackdriverEnableAlertPoliciesOperator`

   :param filter_:  If provided, this field specifies the criteria that
       must be met by alert policies to be enabled.
       For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
   :type filter_: str
   :param retry: A retry object used to retry requests. If ``None`` is
       specified, requests will be retried using a default configuration.
   :type retry: str
   :param timeout: The amount of time, in seconds, to wait
       for the request to complete. Note that if ``retry`` is
       specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param project_id: The project in which alert needs to be enabled.
   :type project_id: str
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

   .. attribute:: ui_color
      :annotation: = #e5ffcc

      

   .. attribute:: template_fields
      :annotation: = ['filter_', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: StackdriverDisableAlertPoliciesOperator(*, filter_: Optional[str] = None, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Disables one or more enabled alerting policies identified by filter
   parameter. Inoperative in case the policy is already disabled.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StackdriverDisableAlertPoliciesOperator`

   :param filter_:  If provided, this field specifies the criteria that
       must be met by alert policies to be disabled.
       For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
   :type filter_: str
   :param retry: A retry object used to retry requests. If ``None`` is
       specified, requests will be retried using a default configuration.
   :type retry: str
   :param timeout: The amount of time, in seconds, to wait
       for the request to complete. Note that if ``retry`` is
       specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param project_id: The project in which alert needs to be disabled.
   :type project_id: str
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

   .. attribute:: ui_color
      :annotation: = #e5ffcc

      

   .. attribute:: template_fields
      :annotation: = ['filter_', 'impersonation_chain']

      

   
   .. method:: execute(self, context)




.. py:class:: StackdriverUpsertAlertOperator(*, alerts: str, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new alert or updates an existing policy identified
   the name field in the alerts parameter.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StackdriverUpsertAlertOperator`

   :param alerts: A JSON string or file that specifies all the alerts that needs
       to be either created or updated. For more details, see
       https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.alertPolicies#AlertPolicy.
       (templated)
   :type alerts: str
   :param retry: A retry object used to retry requests. If ``None`` is
       specified, requests will be retried using a default configuration.
   :type retry: str
   :param timeout: The amount of time, in seconds, to wait
       for the request to complete. Note that if ``retry`` is
       specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param project_id: The project in which alert needs to be created/updated.
   :type project_id: str
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
      :annotation: = ['alerts', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.json']

      

   .. attribute:: ui_color
      :annotation: = #e5ffcc

      

   
   .. method:: execute(self, context)




.. py:class:: StackdriverDeleteAlertOperator(*, name: str, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes an alerting policy.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StackdriverDeleteAlertOperator`

   :param name: The alerting policy to delete. The format is:
                    ``projects/[PROJECT_ID]/alertPolicies/[ALERT_POLICY_ID]``.
   :type name: str
   :param retry: A retry object used to retry requests. If ``None`` is
       specified, requests will be retried using a default configuration.
   :type retry: str
   :param timeout: The amount of time, in seconds, to wait
       for the request to complete. Note that if ``retry`` is
       specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param project_id: The project from which alert needs to be deleted.
   :type project_id: str
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
      :annotation: = ['name', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #e5ffcc

      

   
   .. method:: execute(self, context)




.. py:class:: StackdriverListNotificationChannelsOperator(*, format_: Optional[str] = None, filter_: Optional[str] = None, order_by: Optional[str] = None, page_size: Optional[int] = None, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Fetches all the Notification Channels identified by the filter passed as
   filter parameter. The desired return type can be specified by the
   format parameter, the supported formats are "dict", "json" and None
   which returns python dictionary, stringified JSON and protobuf
   respectively.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StackdriverListNotificationChannelsOperator`

   :param format_: (Optional) Desired output format of the result. The
       supported formats are "dict", "json" and None which returns
       python dictionary, stringified JSON and protobuf respectively.
   :type format_: str
   :param filter_:  If provided, this field specifies the criteria that
       must be met by notification channels to be included in the response.
       For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
   :type filter_: str
   :param order_by: A comma-separated list of fields by which to sort the result.
       Supports the same set of field references as the ``filter`` field. Entries
       can be prefixed with a minus sign to sort by the field in descending order.
       For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
   :type order_by: str
   :param page_size: The maximum number of resources contained in the
       underlying API response. If page streaming is performed per-
       resource, this parameter does not affect the return value. If page
       streaming is performed per-page, this determines the maximum number
       of resources in a page.
   :type page_size: int
   :param retry: A retry object used to retry requests. If ``None`` is
       specified, requests will be retried using a default configuration.
   :type retry: str
   :param timeout: The amount of time, in seconds, to wait
       for the request to complete. Note that if ``retry`` is
       specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param project_id: The project to fetch notification channels from.
   :type project_id: str
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
      :annotation: = ['filter_', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #e5ffcc

      

   
   .. method:: execute(self, context)




.. py:class:: StackdriverEnableNotificationChannelsOperator(*, filter_: Optional[str] = None, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Enables one or more disabled alerting policies identified by filter
   parameter. Inoperative in case the policy is already enabled.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StackdriverEnableNotificationChannelsOperator`

   :param filter_:  If provided, this field specifies the criteria that
       must be met by notification channels to be enabled.
       For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
   :type filter_: str
   :param retry: A retry object used to retry requests. If ``None`` is
       specified, requests will be retried using a default configuration.
   :type retry: str
   :param timeout: The amount of time, in seconds, to wait
       for the request to complete. Note that if ``retry`` is
       specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param project_id: The location used for the operation.
   :type project_id: str
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
      :annotation: = ['filter_', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #e5ffcc

      

   
   .. method:: execute(self, context)




.. py:class:: StackdriverDisableNotificationChannelsOperator(*, filter_: Optional[str] = None, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Disables one or more enabled notification channels identified by filter
   parameter. Inoperative in case the policy is already disabled.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StackdriverDisableNotificationChannelsOperator`

   :param filter_:  If provided, this field specifies the criteria that
       must be met by alert policies to be disabled.
       For more details, see https://cloud.google.com/monitoring/api/v3/sorting-and-filtering.
   :type filter_: str
   :param retry: A retry object used to retry requests. If ``None`` is
       specified, requests will be retried using a default configuration.
   :type retry: str
   :param timeout: The amount of time, in seconds, to wait
       for the request to complete. Note that if ``retry`` is
       specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param project_id: The project in which notification channels needs to be enabled.
   :type project_id: str
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
      :annotation: = ['filter_', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #e5ffcc

      

   
   .. method:: execute(self, context)




.. py:class:: StackdriverUpsertNotificationChannelOperator(*, channels: str, retry: Optional[str] = DEFAULT, timeout: Optional[str] = DEFAULT, metadata: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Creates a new notification or updates an existing notification channel
   identified the name field in the alerts parameter.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StackdriverUpsertNotificationChannelOperator`

   :param channels: A JSON string or file that specifies all the alerts that needs
       to be either created or updated. For more details, see
       https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.notificationChannels.
       (templated)
   :type channels: str
   :param retry: A retry object used to retry requests. If ``None`` is
       specified, requests will be retried using a default configuration.
   :type retry: str
   :param timeout: The amount of time, in seconds, to wait
       for the request to complete. Note that if ``retry`` is
       specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param project_id: The project in which notification channels needs to be created/updated.
   :type project_id: str
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
      :annotation: = ['channels', 'impersonation_chain']

      

   .. attribute:: template_ext
      :annotation: = ['.json']

      

   .. attribute:: ui_color
      :annotation: = #e5ffcc

      

   
   .. method:: execute(self, context)




.. py:class:: StackdriverDeleteNotificationChannelOperator(*, name: str, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', project_id: Optional[str] = None, delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Deletes a notification channel.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:StackdriverDeleteNotificationChannelOperator`

   :param name: The alerting policy to delete. The format is:
                    ``projects/[PROJECT_ID]/notificationChannels/[CHANNEL_ID]``.
   :type name: str
   :param retry: A retry object used to retry requests. If ``None`` is
       specified, requests will be retried using a default configuration.
   :type retry: str
   :param timeout: The amount of time, in seconds, to wait
       for the request to complete. Note that if ``retry`` is
       specified, the timeout applies to each individual attempt.
   :type timeout: float
   :param metadata: Additional metadata that is provided to the method.
   :type metadata: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google
       Cloud Platform.
   :type gcp_conn_id: str
   :param project_id: The project from which notification channel needs to be deleted.
   :type project_id: str
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
      :annotation: = ['name', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #e5ffcc

      

   
   .. method:: execute(self, context)




