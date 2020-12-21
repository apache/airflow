:mod:`airflow.providers.google.cloud.hooks.stackdriver`
=======================================================

.. py:module:: airflow.providers.google.cloud.hooks.stackdriver

.. autoapi-nested-parse::

   This module contains Google Cloud Stackdriver operators.



Module Contents
---------------

.. py:class:: StackdriverHook(gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Stackdriver Hook for connecting with Google Cloud Stackdriver

   
   .. method:: _get_policy_client(self)



   
   .. method:: _get_channel_client(self)



   
   .. method:: list_alert_policies(self, project_id: str, format_: Optional[str] = None, filter_: Optional[str] = None, order_by: Optional[str] = None, page_size: Optional[int] = None, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None)

      Fetches all the Alert Policies identified by the filter passed as
      filter parameter. The desired return type can be specified by the
      format parameter, the supported formats are "dict", "json" and None
      which returns python dictionary, stringified JSON and protobuf
      respectively.

      :param format_: (Optional) Desired output format of the result. The
          supported formats are "dict", "json" and None which returns
          python dictionary, stringified JSON and protobuf respectively.
      :type format_: str
      :param filter_:  If provided, this field specifies the criteria that
          must be met by alert policies to be included in the response.
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
      :param project_id: The project to fetch alerts from.
      :type project_id: str



   
   .. method:: _toggle_policy_status(self, new_state: bool, project_id: str, filter_: Optional[str] = None, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None)



   
   .. method:: enable_alert_policies(self, project_id: str, filter_: Optional[str] = None, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None)

      Enables one or more disabled alerting policies identified by filter
      parameter. Inoperative in case the policy is already enabled.

      :param project_id: The project in which alert needs to be enabled.
      :type project_id: str
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



   
   .. method:: disable_alert_policies(self, project_id: str, filter_: Optional[str] = None, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None)

      Disables one or more enabled alerting policies identified by filter
      parameter. Inoperative in case the policy is already disabled.

      :param project_id: The project in which alert needs to be disabled.
      :type project_id: str
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



   
   .. method:: upsert_alert(self, alerts: str, project_id: str, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None)

      Creates a new alert or updates an existing policy identified
       the name field in the alerts parameter.

      :param project_id: The project in which alert needs to be created/updated.
      :type project_id: str
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



   
   .. method:: delete_alert_policy(self, name: str, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None)

      Deletes an alerting policy.

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



   
   .. method:: list_notification_channels(self, project_id: str, format_: Optional[str] = None, filter_: Optional[str] = None, order_by: Optional[str] = None, page_size: Optional[int] = None, retry: Optional[str] = DEFAULT, timeout: Optional[str] = DEFAULT, metadata: Optional[str] = None)

      Fetches all the Notification Channels identified by the filter passed as
      filter parameter. The desired return type can be specified by the
      format parameter, the supported formats are "dict", "json" and None
      which returns python dictionary, stringified JSON and protobuf
      respectively.

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
      :param project_id: The project to fetch notification channels from.
      :type project_id: str



   
   .. method:: _toggle_channel_status(self, new_state: bool, project_id: str, filter_: Optional[str] = None, retry: Optional[str] = DEFAULT, timeout: Optional[str] = DEFAULT, metadata: Optional[str] = None)



   
   .. method:: enable_notification_channels(self, project_id: str, filter_: Optional[str] = None, retry: Optional[str] = DEFAULT, timeout: Optional[str] = DEFAULT, metadata: Optional[str] = None)

      Enables one or more disabled alerting policies identified by filter
      parameter. Inoperative in case the policy is already enabled.

      :param project_id: The project in which notification channels needs to be enabled.
      :type project_id: str
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



   
   .. method:: disable_notification_channels(self, project_id: str, filter_: Optional[str] = None, retry: Optional[str] = DEFAULT, timeout: Optional[str] = DEFAULT, metadata: Optional[str] = None)

      Disables one or more enabled notification channels identified by filter
      parameter. Inoperative in case the policy is already disabled.

      :param project_id: The project in which notification channels needs to be enabled.
      :type project_id: str
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



   
   .. method:: upsert_channel(self, channels: str, project_id: str, retry: Optional[str] = DEFAULT, timeout: Optional[float] = DEFAULT, metadata: Optional[str] = None)

      Creates a new notification or updates an existing notification channel
      identified the name field in the alerts parameter.

      :param channels: A JSON string or file that specifies all the alerts that needs
          to be either created or updated. For more details, see
          https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.notificationChannels.
          (templated)
      :type channels: str
      :param project_id: The project in which notification channels needs to be created/updated.
      :type project_id: str
      :param retry: A retry object used to retry requests. If ``None`` is
          specified, requests will be retried using a default configuration.
      :type retry: str
      :param timeout: The amount of time, in seconds, to wait
          for the request to complete. Note that if ``retry`` is
          specified, the timeout applies to each individual attempt.
      :type timeout: float
      :param metadata: Additional metadata that is provided to the method.
      :type metadata: str



   
   .. method:: delete_notification_channel(self, name: str, retry: Optional[str] = DEFAULT, timeout: Optional[str] = DEFAULT, metadata: Optional[str] = None)

      Deletes a notification channel.

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




