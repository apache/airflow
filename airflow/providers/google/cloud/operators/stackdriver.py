#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import Optional, Sequence, Union

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.monitoring_v3 import AlertPolicy, NotificationChannel

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.stackdriver import StackdriverHook


class StackdriverListAlertPoliciesOperator(BaseOperator):
    """
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
    """

    template_fields = (
        'filter_',
        'impersonation_chain',
    )
    ui_color = "#e5ffcc"

    def __init__(
        self,
        *,
        format_: Optional[str] = None,
        filter_: Optional[str] = None,
        order_by: Optional[str] = None,
        page_size: Optional[int] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.format_ = format_
        self.filter_ = filter_
        self.order_by = order_by
        self.page_size = page_size
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook = None

    def execute(self, context):
        self.log.info(
            'List Alert Policies: Project id: %s Format: %s Filter: %s Order By: %s Page Size: %s',
            self.project_id,
            self.format_,
            self.filter_,
            self.order_by,
            self.page_size,
        )
        if self.hook is None:
            self.hook = StackdriverHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )

        result = self.hook.list_alert_policies(
            project_id=self.project_id,
            format_=self.format_,
            filter_=self.filter_,
            order_by=self.order_by,
            page_size=self.page_size,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [AlertPolicy.to_dict(policy) for policy in result]


class StackdriverEnableAlertPoliciesOperator(BaseOperator):
    """
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
    """

    ui_color = "#e5ffcc"
    template_fields = (
        'filter_',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        filter_: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.filter_ = filter_
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.impersonation_chain = impersonation_chain
        self.hook = None

    def execute(self, context):
        self.log.info('Enable Alert Policies: Project id: %s Filter: %s', self.project_id, self.filter_)
        if self.hook is None:
            self.hook = StackdriverHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        self.hook.enable_alert_policies(
            filter_=self.filter_,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


# Disable Alert Operator
class StackdriverDisableAlertPoliciesOperator(BaseOperator):
    """
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
    """

    ui_color = "#e5ffcc"
    template_fields = (
        'filter_',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        filter_: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.filter_ = filter_
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.impersonation_chain = impersonation_chain
        self.hook = None

    def execute(self, context):
        self.log.info('Disable Alert Policies: Project id: %s Filter: %s', self.project_id, self.filter_)
        if self.hook is None:
            self.hook = StackdriverHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        self.hook.disable_alert_policies(
            filter_=self.filter_,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class StackdriverUpsertAlertOperator(BaseOperator):
    """
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
    """

    template_fields = (
        'alerts',
        'impersonation_chain',
    )
    template_ext = ('.json',)

    ui_color = "#e5ffcc"

    def __init__(
        self,
        *,
        alerts: str,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.alerts = alerts
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook = None

    def execute(self, context):
        self.log.info('Upsert Alert Policies: Alerts: %s Project id: %s', self.alerts, self.project_id)
        if self.hook is None:
            self.hook = StackdriverHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        self.hook.upsert_alert(
            alerts=self.alerts,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class StackdriverDeleteAlertOperator(BaseOperator):
    """
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
    """

    template_fields = (
        'name',
        'impersonation_chain',
    )

    ui_color = "#e5ffcc"

    def __init__(
        self,
        *,
        name: str,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook = None

    def execute(self, context):
        self.log.info('Delete Alert Policy: Project id: %s Name: %s', self.project_id, self.name)
        if self.hook is None:
            self.hook = StackdriverHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        self.hook.delete_alert_policy(
            name=self.name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class StackdriverListNotificationChannelsOperator(BaseOperator):
    """
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
    """

    template_fields = (
        'filter_',
        'impersonation_chain',
    )

    ui_color = "#e5ffcc"

    def __init__(
        self,
        *,
        format_: Optional[str] = None,
        filter_: Optional[str] = None,
        order_by: Optional[str] = None,
        page_size: Optional[int] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.format_ = format_
        self.filter_ = filter_
        self.order_by = order_by
        self.page_size = page_size
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook = None

    def execute(self, context):
        self.log.info(
            'List Notification Channels: Project id: %s Format: %s Filter: %s Order By: %s Page Size: %s',
            self.project_id,
            self.format_,
            self.filter_,
            self.order_by,
            self.page_size,
        )
        if self.hook is None:
            self.hook = StackdriverHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        channels = self.hook.list_notification_channels(
            format_=self.format_,
            project_id=self.project_id,
            filter_=self.filter_,
            order_by=self.order_by,
            page_size=self.page_size,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        result = [NotificationChannel.to_dict(channel) for channel in channels]
        return result


class StackdriverEnableNotificationChannelsOperator(BaseOperator):
    """
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
    """

    template_fields = (
        'filter_',
        'impersonation_chain',
    )

    ui_color = "#e5ffcc"

    def __init__(
        self,
        *,
        filter_: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.filter_ = filter_
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook = None

    def execute(self, context):
        self.log.info(
            'Enable Notification Channels: Project id: %s Filter: %s', self.project_id, self.filter_
        )
        if self.hook is None:
            self.hook = StackdriverHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        self.hook.enable_notification_channels(
            filter_=self.filter_,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class StackdriverDisableNotificationChannelsOperator(BaseOperator):
    """
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
    """

    template_fields = (
        'filter_',
        'impersonation_chain',
    )

    ui_color = "#e5ffcc"

    def __init__(
        self,
        *,
        filter_: Optional[str] = None,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.filter_ = filter_
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook = None

    def execute(self, context):
        self.log.info(
            'Disable Notification Channels: Project id: %s Filter: %s', self.project_id, self.filter_
        )
        if self.hook is None:
            self.hook = StackdriverHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        self.hook.disable_notification_channels(
            filter_=self.filter_,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class StackdriverUpsertNotificationChannelOperator(BaseOperator):
    """
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
    """

    template_fields = (
        'channels',
        'impersonation_chain',
    )
    template_ext = ('.json',)

    ui_color = "#e5ffcc"

    def __init__(
        self,
        *,
        channels: str,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[str] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.channels = channels
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook = None

    def execute(self, context):
        self.log.info(
            'Upsert Notification Channels: Channels: %s Project id: %s', self.channels, self.project_id
        )
        if self.hook is None:
            self.hook = StackdriverHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        self.hook.upsert_channel(
            channels=self.channels,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class StackdriverDeleteNotificationChannelOperator(BaseOperator):
    """
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
    """

    template_fields = (
        'name',
        'impersonation_chain',
    )

    ui_color = "#e5ffcc"

    def __init__(
        self,
        *,
        name: str,
        retry: Optional[str] = DEFAULT,
        timeout: Optional[float] = DEFAULT,
        metadata: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        project_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.hook = None

    def execute(self, context):
        self.log.info('Delete Notification Channel: Project id: %s Name: %s', self.project_id, self.name)
        if self.hook is None:
            self.hook = StackdriverHook(
                gcp_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            )
        self.hook.delete_notification_channel(
            name=self.name, retry=self.retry, timeout=self.timeout, metadata=self.metadata
        )
