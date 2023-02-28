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
"""
This module contains Google PubSub operators.

.. spelling::

    MessageStoragePolicy
"""
from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Callable, Sequence

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.cloud.pubsub_v1.types import (
    DeadLetterPolicy,
    Duration,
    ExpirationPolicy,
    MessageStoragePolicy,
    PushConfig,
    ReceivedMessage,
    RetryPolicy,
)

from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.cloud.links.pubsub import PubSubSubscriptionLink, PubSubTopicLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PubSubCreateTopicOperator(GoogleCloudBaseOperator):
    """Create a PubSub topic.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubCreateTopicOperator`

    By default, if the topic already exists, this operator will
    not cause the DAG to fail. ::

        with DAG('successful DAG') as dag:
            (
                PubSubCreateTopicOperator(project_id='my-project', topic='my_new_topic')
                >> PubSubCreateTopicOperator(project_id='my-project', topic='my_new_topic')
            )

    The operator can be configured to fail if the topic already exists. ::

        with DAG('failing DAG') as dag:
            (
                PubSubCreateTopicOperator(project_id='my-project', topic='my_new_topic')
                >> PubSubCreateTopicOperator(
                    project_id='my-project',
                    topic='my_new_topic',
                    fail_if_exists=True,
                )
            )

    Both ``project_id`` and ``topic`` are templated so you can use Jinja templating in their values.

    :param project_id: Optional, the Google Cloud project ID where the topic will be created.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param topic: the topic to create. Do not include the
        full topic path. In other words, instead of
        ``projects/{project}/topics/{topic}``, provide only
        ``{topic}``. (templated)
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param labels: Client-assigned labels; see
        https://cloud.google.com/pubsub/docs/labels
    :param message_storage_policy: Policy constraining the set
        of Google Cloud regions where messages published to
        the topic may be stored. If not present, then no constraints
        are in effect.
        Union[dict, google.cloud.pubsub_v1.types.MessageStoragePolicy]
    :param kms_key_name: The resource name of the Cloud KMS CryptoKey
        to be used to protect access to messages published on this topic.
        The expected format is
        ``projects/*/locations/*/keyRings/*/cryptoKeys/*``.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "topic",
        "impersonation_chain",
    )
    ui_color = "#0273d4"
    operator_extra_links = (PubSubTopicLink(),)

    def __init__(
        self,
        *,
        topic: str,
        project_id: str | None = None,
        fail_if_exists: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        labels: dict[str, str] | None = None,
        message_storage_policy: dict | MessageStoragePolicy = None,
        kms_key_name: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.project_id = project_id
        self.topic = topic
        self.fail_if_exists = fail_if_exists
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.labels = labels
        self.message_storage_policy = message_storage_policy
        self.kms_key_name = kms_key_name
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = PubSubHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Creating topic %s", self.topic)
        hook.create_topic(
            project_id=self.project_id,
            topic=self.topic,
            fail_if_exists=self.fail_if_exists,
            labels=self.labels,
            message_storage_policy=self.message_storage_policy,
            kms_key_name=self.kms_key_name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Created topic %s", self.topic)
        PubSubTopicLink.persist(
            context=context,
            task_instance=self,
            topic_id=self.topic,
            project_id=self.project_id or hook.project_id,
        )


class PubSubCreateSubscriptionOperator(GoogleCloudBaseOperator):
    """Create a PubSub subscription.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubCreateSubscriptionOperator`

    By default, the subscription will be created in ``project_id``. If
    ``subscription_project_id`` is specified and the Google Cloud credentials allow, the
    Subscription can be created in a different project from its topic.

    By default, if the subscription already exists, this operator will
    not cause the DAG to fail. However, the topic must exist in the project. ::

        with DAG('successful DAG') as dag:
            (
                PubSubCreateSubscriptionOperator(
                    project_id='my-project',
                    topic='my-topic',
                    subscription='my-subscription'
                )
                >> PubSubCreateSubscriptionOperator(
                    project_id='my-project',
                    topic='my-topic',
                    subscription='my-subscription',
                )
            )

    The operator can be configured to fail if the subscription already exists.
    ::

        with DAG('failing DAG') as dag:
            (
                PubSubCreateSubscriptionOperator(
                    project_id='my-project',
                    topic='my-topic',
                    subscription='my-subscription',
                )
                >> PubSubCreateSubscriptionOperator(
                    project_id='my-project',
                    topic='my-topic',
                    subscription='my-subscription',
                    fail_if_exists=True,
                )
            )

    Finally, subscription is not required. If not passed, the operator will
    generated a universally unique identifier for the subscription's name. ::

        with DAG('DAG') as dag:
            PubSubCreateSubscriptionOperator(project_id='my-project', topic='my-topic')

    ``project_id``, ``topic``, ``subscription``, ``subscription_project_id`` and
    ``impersonation_chain`` are templated so you can use Jinja templating in their values.

    :param project_id: Optional, the Google Cloud project ID where the topic exists.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param topic: the topic to create. Do not include the
        full topic path. In other words, instead of
        ``projects/{project}/topics/{topic}``, provide only
        ``{topic}``. (templated)
    :param subscription: the Pub/Sub subscription name. If empty, a random
        name will be generated using the uuid module
    :param subscription_project_id: the Google Cloud project ID where the subscription
        will be created. If empty, ``topic_project`` will be used.
    :param ack_deadline_secs: Number of seconds that a subscriber has to
        acknowledge each message pulled from the subscription
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param push_config: If push delivery is used with this subscription,
        this field is used to configure it. An empty ``pushConfig`` signifies
        that the subscriber will pull and ack messages using API methods.
    :param retain_acked_messages: Indicates whether to retain acknowledged
        messages. If true, then messages are not expunged from the subscription's
        backlog, even if they are acknowledged, until they fall out of the
        ``message_retention_duration`` window. This must be true if you would
        like to Seek to a timestamp.
    :param message_retention_duration: How long to retain unacknowledged messages
        in the subscription's backlog, from the moment a message is published. If
        ``retain_acked_messages`` is true, then this also configures the
        retention of acknowledged messages, and thus configures how far back in
        time a ``Seek`` can be done. Defaults to 7 days. Cannot be more than 7
        days or less than 10 minutes.
    :param labels: Client-assigned labels; see
        https://cloud.google.com/pubsub/docs/labels
    :param enable_message_ordering: If true, messages published with the same
        ordering_key in PubsubMessage will be delivered to the subscribers in the order
        in which they are received by the Pub/Sub system. Otherwise, they may be
        delivered in any order.
    :param expiration_policy: A policy that specifies the conditions for this
        subscription's expiration. A subscription is considered active as long as any
        connected subscriber is successfully consuming messages from the subscription or
        is issuing operations on the subscription. If expiration_policy is not set,
        a default policy with ttl of 31 days will be used. The minimum allowed value for
        expiration_policy.ttl is 1 day.
    :param filter_: An expression written in the Cloud Pub/Sub filter language. If
        non-empty, then only PubsubMessages whose attributes field matches the filter are
        delivered on this subscription. If empty, then no messages are filtered out.
    :param dead_letter_policy: A policy that specifies the conditions for dead lettering
        messages in this subscription. If dead_letter_policy is not set, dead lettering is
        disabled.
    :param retry_policy: A policy that specifies how Pub/Sub retries message delivery
        for this subscription. If not set, the default retry policy is applied. This
        generally implies that messages will be retried as soon as possible for healthy
        subscribers. RetryPolicy will be triggered on NACKs or acknowledgement deadline
        exceeded events for a given message.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "topic",
        "subscription",
        "subscription_project_id",
        "impersonation_chain",
    )
    ui_color = "#0273d4"
    operator_extra_links = (PubSubSubscriptionLink(),)

    def __init__(
        self,
        *,
        topic: str,
        project_id: str | None = None,
        subscription: str | None = None,
        subscription_project_id: str | None = None,
        ack_deadline_secs: int = 10,
        fail_if_exists: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        push_config: dict | PushConfig | None = None,
        retain_acked_messages: bool | None = None,
        message_retention_duration: dict | Duration | None = None,
        labels: dict[str, str] | None = None,
        enable_message_ordering: bool = False,
        expiration_policy: dict | ExpirationPolicy | None = None,
        filter_: str | None = None,
        dead_letter_policy: dict | DeadLetterPolicy | None = None,
        retry_policy: dict | RetryPolicy | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.topic = topic
        self.subscription = subscription
        self.subscription_project_id = subscription_project_id
        self.ack_deadline_secs = ack_deadline_secs
        self.fail_if_exists = fail_if_exists
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.push_config = push_config
        self.retain_acked_messages = retain_acked_messages
        self.message_retention_duration = message_retention_duration
        self.labels = labels
        self.enable_message_ordering = enable_message_ordering
        self.expiration_policy = expiration_policy
        self.filter_ = filter_
        self.dead_letter_policy = dead_letter_policy
        self.retry_policy = retry_policy
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> str:
        hook = PubSubHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Creating subscription for topic %s", self.topic)
        result = hook.create_subscription(
            project_id=self.project_id,
            topic=self.topic,
            subscription=self.subscription,
            subscription_project_id=self.subscription_project_id,
            ack_deadline_secs=self.ack_deadline_secs,
            fail_if_exists=self.fail_if_exists,
            push_config=self.push_config,
            retain_acked_messages=self.retain_acked_messages,
            message_retention_duration=self.message_retention_duration,
            labels=self.labels,
            enable_message_ordering=self.enable_message_ordering,
            expiration_policy=self.expiration_policy,
            filter_=self.filter_,
            dead_letter_policy=self.dead_letter_policy,
            retry_policy=self.retry_policy,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        self.log.info("Created subscription for topic %s", self.topic)
        PubSubSubscriptionLink.persist(
            context=context,
            task_instance=self,
            subscription_id=self.subscription or result,  # result returns subscription name
            project_id=self.project_id or hook.project_id,
        )
        return result


class PubSubDeleteTopicOperator(GoogleCloudBaseOperator):
    """Delete a PubSub topic.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubDeleteTopicOperator`

    By default, if the topic does not exist, this operator will
    not cause the DAG to fail. ::

        with DAG('successful DAG') as dag:
            PubSubDeleteTopicOperator(project_id='my-project', topic='non_existing_topic')

    The operator can be configured to fail if the topic does not exist. ::

        with DAG('failing DAG') as dag:
            PubSubDeleteTopicOperator(
                project_id='my-project', topic='non_existing_topic', fail_if_not_exists=True,
            )

    Both ``project_id`` and ``topic`` are templated so you can use Jinja templating in their values.

    :param project_id: Optional, the Google Cloud project ID in which to work (templated).
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param topic: the topic to delete. Do not include the
        full topic path. In other words, instead of
        ``projects/{project}/topics/{topic}``, provide only
        ``{topic}``. (templated)
    :param fail_if_not_exists: If True and the topic does not exist, fail
        the task
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "topic",
        "impersonation_chain",
    )
    ui_color = "#cb4335"

    def __init__(
        self,
        *,
        topic: str,
        project_id: str | None = None,
        fail_if_not_exists: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.topic = topic
        self.fail_if_not_exists = fail_if_not_exists
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = PubSubHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Deleting topic %s", self.topic)
        hook.delete_topic(
            project_id=self.project_id,
            topic=self.topic,
            fail_if_not_exists=self.fail_if_not_exists,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Deleted topic %s", self.topic)


class PubSubDeleteSubscriptionOperator(GoogleCloudBaseOperator):
    """Delete a PubSub subscription.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubDeleteSubscriptionOperator`

    By default, if the subscription does not exist, this operator will
    not cause the DAG to fail. ::

        with DAG('successful DAG') as dag:
            PubSubDeleteSubscriptionOperator(project_id='my-project', subscription='non-existing')

    The operator can be configured to fail if the subscription already exists.

    ::

        with DAG('failing DAG') as dag:
            PubSubDeleteSubscriptionOperator(
                project_id='my-project', subscription='non-existing', fail_if_not_exists=True,
            )

    ``project_id``, and ``subscription`` are templated so you can use Jinja templating in their values.

    :param project_id: Optional, the Google Cloud project ID in which to work (templated).
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param subscription: the subscription to delete. Do not include the
        full subscription path. In other words, instead of
        ``projects/{project}/subscription/{subscription}``, provide only
        ``{subscription}``. (templated)
    :param fail_if_not_exists: If True and the subscription does not exist,
        fail the task
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param retry: (Optional) A retry object used to retry requests.
        If None is specified, requests will not be retried.
    :param timeout: (Optional) The amount of time, in seconds, to wait for the request
        to complete. Note that if retry is specified, the timeout applies to each
        individual attempt.
    :param metadata: (Optional) Additional metadata that is provided to the method.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "subscription",
        "impersonation_chain",
    )
    ui_color = "#cb4335"

    def __init__(
        self,
        *,
        subscription: str,
        project_id: str | None = None,
        fail_if_not_exists: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.subscription = subscription
        self.fail_if_not_exists = fail_if_not_exists
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = PubSubHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Deleting subscription %s", self.subscription)
        hook.delete_subscription(
            project_id=self.project_id,
            subscription=self.subscription,
            fail_if_not_exists=self.fail_if_not_exists,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Deleted subscription %s", self.subscription)


class PubSubPublishMessageOperator(GoogleCloudBaseOperator):
    """Publish messages to a PubSub topic.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PubSubPublishMessageOperator`

    Each Task publishes all provided messages to the same topic
    in a single Google Cloud project. If the topic does not exist, this
    task will fail. ::

        m1 = {'data': b'Hello, World!',
              'attributes': {'type': 'greeting'}
             }
        m2 = {'data': b'Knock, knock'}
        m3 = {'attributes': {'foo': ''}}

        t1 = PubSubPublishMessageOperator(
            project_id='my-project',
            topic='my_topic',
            messages=[m1, m2, m3],
            create_topic=True,
            dag=dag,
        )

    ``project_id``, ``topic``, and ``messages`` are templated so you can use Jinja templating
    in their values.

    :param project_id: Optional, the Google Cloud project ID in which to work (templated).
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param topic: the topic to which to publish. Do not include the
        full topic path. In other words, instead of
        ``projects/{project}/topics/{topic}``, provide only
        ``{topic}``. (templated)
    :param messages: a list of messages to be published to the
        topic. Each message is a dict with one or more of the
        following keys-value mappings:
        * 'data': a bytestring (utf-8 encoded)
        * 'attributes': {'key1': 'value1', ...}
        Each message must contain at least a non-empty 'data' value
        or an attribute dict with at least one key (templated). See
        https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "topic",
        "messages",
        "impersonation_chain",
    )
    ui_color = "#0273d4"

    def __init__(
        self,
        *,
        topic: str,
        messages: list,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.topic = topic
        self.messages = messages
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = PubSubHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Publishing to topic %s", self.topic)
        hook.publish(project_id=self.project_id, topic=self.topic, messages=self.messages)
        self.log.info("Published to topic %s", self.topic)


class PubSubPullOperator(GoogleCloudBaseOperator):
    """Pulls messages from a PubSub subscription and passes them through XCom.
    If the queue is empty, returns empty list - never waits for messages.
    If you do need to wait, please use :class:`airflow.providers.google.cloud.sensors.PubSubPullSensor`
    instead.

    .. seealso::
        For more information on how to use this operator and the PubSubPullSensor, take a look at the guide:
        :ref:`howto/operator:PubSubPullSensor`

    This operator will pull up to ``max_messages`` messages from the
    specified PubSub subscription. When the subscription returns messages,
    the messages will be returned immediately from the operator and passed through XCom for downstream tasks.

    If ``ack_messages`` is set to True, messages will be immediately
    acknowledged before being returned, otherwise, downstream tasks will be
    responsible for acknowledging them.

    ``project_id `` and ``subscription`` are templated so you can use Jinja templating in their values.

    :param project_id: the Google Cloud project ID for the subscription (templated)
    :param subscription: the Pub/Sub subscription name. Do not include the
        full subscription path.
    :param max_messages: The maximum number of messages to retrieve per
        PubSub pull request
    :param ack_messages: If True, each message will be acknowledged
        immediately rather than by any downstream tasks
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param messages_callback: (Optional) Callback to process received messages.
        It's return value will be saved to XCom.
        If you are pulling large messages, you probably want to provide a custom callback.
        If not provided, the default implementation will convert `ReceivedMessage` objects
        into JSON-serializable dicts using `google.protobuf.json_format.MessageToDict` function.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "project_id",
        "subscription",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        subscription: str,
        max_messages: int = 5,
        ack_messages: bool = False,
        messages_callback: Callable[[list[ReceivedMessage], Context], Any] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.project_id = project_id
        self.subscription = subscription
        self.max_messages = max_messages
        self.ack_messages = ack_messages
        self.messages_callback = messages_callback
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list:
        hook = PubSubHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        pulled_messages = hook.pull(
            project_id=self.project_id,
            subscription=self.subscription,
            max_messages=self.max_messages,
            return_immediately=True,
        )

        handle_messages = self.messages_callback or self._default_message_callback

        ret = handle_messages(pulled_messages, context)

        if pulled_messages and self.ack_messages:
            hook.acknowledge(
                project_id=self.project_id,
                subscription=self.subscription,
                messages=pulled_messages,
            )

        return ret

    def _default_message_callback(
        self,
        pulled_messages: list[ReceivedMessage],
        context: Context,
    ) -> list:
        """
        This method can be overridden by subclasses or by `messages_callback` constructor argument.
        This default implementation converts `ReceivedMessage` objects into JSON-serializable dicts.

        :param pulled_messages: messages received from the topic.
        :param context: same as in `execute`
        :return: value to be saved to XCom.
        """
        messages_json = [ReceivedMessage.to_dict(m) for m in pulled_messages]

        return messages_json
