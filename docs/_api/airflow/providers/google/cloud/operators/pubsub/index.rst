:mod:`airflow.providers.google.cloud.operators.pubsub`
======================================================

.. py:module:: airflow.providers.google.cloud.operators.pubsub

.. autoapi-nested-parse::

   This module contains Google PubSub operators.



Module Contents
---------------

.. py:class:: PubSubCreateTopicOperator(*, topic: str, project_id: Optional[str] = None, fail_if_exists: bool = False, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, labels: Optional[Dict[str, str]] = None, message_storage_policy: Union[Dict, MessageStoragePolicy] = None, kms_key_name: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, project: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Create a PubSub topic.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:PubSubCreateTopicOperator`

   By default, if the topic already exists, this operator will
   not cause the DAG to fail. ::

       with DAG('successful DAG') as dag:
           (
               PubSubTopicCreateOperator(project='my-project',
                                            topic='my_new_topic')
               >> PubSubTopicCreateOperator(project='my-project',
                                            topic='my_new_topic')
           )

   The operator can be configured to fail if the topic already exists. ::

       with DAG('failing DAG') as dag:
           (
               PubSubTopicCreateOperator(project='my-project',
                                            topic='my_new_topic')
               >> PubSubTopicCreateOperator(project='my-project',
                                            topic='my_new_topic',
                                            fail_if_exists=True)
           )

   Both ``project`` and ``topic`` are templated so you can use
   variables in them.

   :param project_id: Optional, the Google Cloud project ID where the topic will be created.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param topic: the topic to create. Do not include the
       full topic path. In other words, instead of
       ``projects/{project}/topics/{topic}``, provide only
       ``{topic}``. (templated)
   :type topic: str
   :param gcp_conn_id: The connection ID to use connecting to
       Google Cloud.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param labels: Client-assigned labels; see
       https://cloud.google.com/pubsub/docs/labels
   :type labels: Dict[str, str]
   :param message_storage_policy: Policy constraining the set
       of Google Cloud regions where messages published to
       the topic may be stored. If not present, then no constraints
       are in effect.
   :type message_storage_policy:
       Union[Dict, google.cloud.pubsub_v1.types.MessageStoragePolicy]
   :param kms_key_name: The resource name of the Cloud KMS CryptoKey
       to be used to protect access to messages published on this topic.
       The expected format is
       ``projects/*/locations/*/keyRings/*/cryptoKeys/*``.
   :type kms_key_name: str
   :param retry: (Optional) A retry object used to retry requests.
       If None is specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request
       to complete. Note that if retry is specified, the timeout applies to each
       individual attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]]
   :param project: (Deprecated) the Google Cloud project ID where the topic will be created
   :type project: str
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
      :annotation: = ['project_id', 'topic', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #0273d4

      

   
   .. method:: execute(self, context)




.. py:class:: PubSubCreateSubscriptionOperator(*, topic: str, project_id: Optional[str] = None, subscription: Optional[str] = None, subscription_project_id: Optional[str] = None, ack_deadline_secs: int = 10, fail_if_exists: bool = False, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, push_config: Optional[Union[Dict, PushConfig]] = None, retain_acked_messages: Optional[bool] = None, message_retention_duration: Optional[Union[Dict, Duration]] = None, labels: Optional[Dict[str, str]] = None, enable_message_ordering: bool = False, expiration_policy: Optional[Union[Dict, ExpirationPolicy]] = None, filter_: Optional[str] = None, dead_letter_policy: Optional[Union[Dict, DeadLetterPolicy]] = None, retry_policy: Optional[Union[Dict, RetryPolicy]] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, topic_project: Optional[str] = None, subscription_project: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Create a PubSub subscription.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:PubSubCreateSubscriptionOperator`

   By default, the subscription will be created in ``topic_project``. If
   ``subscription_project`` is specified and the Google Cloud credentials allow, the
   Subscription can be created in a different project from its topic.

   By default, if the subscription already exists, this operator will
   not cause the DAG to fail. However, the topic must exist in the project. ::

       with DAG('successful DAG') as dag:
           (
               PubSubSubscriptionCreateOperator(
                   topic_project='my-project', topic='my-topic',
                   subscription='my-subscription')
               >> PubSubSubscriptionCreateOperator(
                   topic_project='my-project', topic='my-topic',
                   subscription='my-subscription')
           )

   The operator can be configured to fail if the subscription already exists.
   ::

       with DAG('failing DAG') as dag:
           (
               PubSubSubscriptionCreateOperator(
                   topic_project='my-project', topic='my-topic',
                   subscription='my-subscription')
               >> PubSubSubscriptionCreateOperator(
                   topic_project='my-project', topic='my-topic',
                   subscription='my-subscription', fail_if_exists=True)
           )

   Finally, subscription is not required. If not passed, the operator will
   generated a universally unique identifier for the subscription's name. ::

       with DAG('DAG') as dag:
           (
               PubSubSubscriptionCreateOperator(
                   topic_project='my-project', topic='my-topic')
           )

   ``topic_project``, ``topic``, ``subscription``, and
   ``subscription`` are templated so you can use variables in them.

   :param project_id: Optional, the Google Cloud project ID where the topic exists.
       If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param topic: the topic to create. Do not include the
       full topic path. In other words, instead of
       ``projects/{project}/topics/{topic}``, provide only
       ``{topic}``. (templated)
   :type topic: str
   :param subscription: the Pub/Sub subscription name. If empty, a random
       name will be generated using the uuid module
   :type subscription: str
   :param subscription_project_id: the Google Cloud project ID where the subscription
       will be created. If empty, ``topic_project`` will be used.
   :type subscription_project_id: str
   :param ack_deadline_secs: Number of seconds that a subscriber has to
       acknowledge each message pulled from the subscription
   :type ack_deadline_secs: int
   :param gcp_conn_id: The connection ID to use connecting to
       Google Cloud.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param push_config: If push delivery is used with this subscription,
       this field is used to configure it. An empty ``pushConfig`` signifies
       that the subscriber will pull and ack messages using API methods.
   :type push_config: Union[Dict, google.cloud.pubsub_v1.types.PushConfig]
   :param retain_acked_messages: Indicates whether to retain acknowledged
       messages. If true, then messages are not expunged from the subscription's
       backlog, even if they are acknowledged, until they fall out of the
       ``message_retention_duration`` window. This must be true if you would
       like to Seek to a timestamp.
   :type retain_acked_messages: bool
   :param message_retention_duration: How long to retain unacknowledged messages
       in the subscription's backlog, from the moment a message is published. If
       ``retain_acked_messages`` is true, then this also configures the
       retention of acknowledged messages, and thus configures how far back in
       time a ``Seek`` can be done. Defaults to 7 days. Cannot be more than 7
       days or less than 10 minutes.
   :type message_retention_duration: Union[Dict, google.cloud.pubsub_v1.types.Duration]
   :param labels: Client-assigned labels; see
       https://cloud.google.com/pubsub/docs/labels
   :type labels: Dict[str, str]
   :param enable_message_ordering: If true, messages published with the same
       ordering_key in PubsubMessage will be delivered to the subscribers in the order
       in which they are received by the Pub/Sub system. Otherwise, they may be
       delivered in any order.
   :type enable_message_ordering: bool
   :param expiration_policy: A policy that specifies the conditions for this
       subscription’s expiration. A subscription is considered active as long as any
       connected subscriber is successfully consuming messages from the subscription or
       is issuing operations on the subscription. If expiration_policy is not set,
       a default policy with ttl of 31 days will be used. The minimum allowed value for
       expiration_policy.ttl is 1 day.
   :type expiration_policy: Union[Dict, google.cloud.pubsub_v1.types.ExpirationPolicy`]
   :param filter_: An expression written in the Cloud Pub/Sub filter language. If
       non-empty, then only PubsubMessages whose attributes field matches the filter are
       delivered on this subscription. If empty, then no messages are filtered out.
   :type filter_: str
   :param dead_letter_policy: A policy that specifies the conditions for dead lettering
       messages in this subscription. If dead_letter_policy is not set, dead lettering is
       disabled.
   :type dead_letter_policy: Union[Dict, google.cloud.pubsub_v1.types.DeadLetterPolicy]
   :param retry_policy: A policy that specifies how Pub/Sub retries message delivery
       for this subscription. If not set, the default retry policy is applied. This
       generally implies that messages will be retried as soon as possible for healthy
       subscribers. RetryPolicy will be triggered on NACKs or acknowledgement deadline
       exceeded events for a given message.
   :type retry_policy: Union[Dict, google.cloud.pubsub_v1.types.RetryPolicy]
   :param retry: (Optional) A retry object used to retry requests.
       If None is specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request
       to complete. Note that if retry is specified, the timeout applies to each
       individual attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]]
   :param topic_project: (Deprecated) the Google Cloud project ID where the topic exists
   :type topic_project: str
   :param subscription_project: (Deprecated) the Google Cloud project ID where the subscription
       will be created. If empty, ``topic_project`` will be used.
   :type subscription_project: str
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
      :annotation: = ['project_id', 'topic', 'subscription', 'subscription_project_id', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #0273d4

      

   
   .. method:: execute(self, context)




.. py:class:: PubSubDeleteTopicOperator(*, topic: str, project_id: Optional[str] = None, fail_if_not_exists: bool = False, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, project: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Delete a PubSub topic.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:PubSubDeleteTopicOperator`

   By default, if the topic does not exist, this operator will
   not cause the DAG to fail. ::

       with DAG('successful DAG') as dag:
           (
               PubSubTopicDeleteOperator(project='my-project',
                                            topic='non_existing_topic')
           )

   The operator can be configured to fail if the topic does not exist. ::

       with DAG('failing DAG') as dag:
           (
               PubSubTopicCreateOperator(project='my-project',
                                            topic='non_existing_topic',
                                            fail_if_not_exists=True)
           )

   Both ``project`` and ``topic`` are templated so you can use
   variables in them.

   :param project_id: Optional, the Google Cloud project ID in which to work (templated).
       If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param topic: the topic to delete. Do not include the
       full topic path. In other words, instead of
       ``projects/{project}/topics/{topic}``, provide only
       ``{topic}``. (templated)
   :type topic: str
   :param fail_if_not_exists: If True and the topic does not exist, fail
       the task
   :type fail_if_not_exists: bool
   :param gcp_conn_id: The connection ID to use connecting to
       Google Cloud.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param retry: (Optional) A retry object used to retry requests.
       If None is specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request
       to complete. Note that if retry is specified, the timeout applies to each
       individual attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]]
   :param project: (Deprecated) the Google Cloud project ID where the topic will be created
   :type project: str
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
      :annotation: = ['project_id', 'topic', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #cb4335

      

   
   .. method:: execute(self, context)




.. py:class:: PubSubDeleteSubscriptionOperator(*, subscription: str, project_id: Optional[str] = None, fail_if_not_exists: bool = False, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, retry: Optional[Retry] = None, timeout: Optional[float] = None, metadata: Optional[Sequence[Tuple[str, str]]] = None, project: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Delete a PubSub subscription.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:PubSubDeleteSubscriptionOperator`

   By default, if the subscription does not exist, this operator will
   not cause the DAG to fail. ::

       with DAG('successful DAG') as dag:
           (
               PubSubSubscriptionDeleteOperator(project='my-project',
                                                   subscription='non-existing')
           )

   The operator can be configured to fail if the subscription already exists.

   ::

       with DAG('failing DAG') as dag:
           (
               PubSubSubscriptionDeleteOperator(
                    project='my-project', subscription='non-existing',
                    fail_if_not_exists=True)
           )

   ``project``, and ``subscription`` are templated so you can use
   variables in them.

   :param project_id: Optional, the Google Cloud project ID in which to work (templated).
       If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param subscription: the subscription to delete. Do not include the
       full subscription path. In other words, instead of
       ``projects/{project}/subscription/{subscription}``, provide only
       ``{subscription}``. (templated)
   :type subscription: str
   :param fail_if_not_exists: If True and the subscription does not exist,
       fail the task
   :type fail_if_not_exists: bool
   :param gcp_conn_id: The connection ID to use connecting to
       Google Cloud.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param retry: (Optional) A retry object used to retry requests.
       If None is specified, requests will not be retried.
   :type retry: google.api_core.retry.Retry
   :param timeout: (Optional) The amount of time, in seconds, to wait for the request
       to complete. Note that if retry is specified, the timeout applies to each
       individual attempt.
   :type timeout: float
   :param metadata: (Optional) Additional metadata that is provided to the method.
   :type metadata: Sequence[Tuple[str, str]]]
   :param project: (Deprecated) the Google Cloud project ID where the topic will be created
   :type project: str
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
      :annotation: = ['project_id', 'subscription', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #cb4335

      

   
   .. method:: execute(self, context)




.. py:class:: PubSubPublishMessageOperator(*, topic: str, messages: List, project_id: Optional[str] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, project: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Publish messages to a PubSub topic.

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

       t1 = PubSubPublishOperator(
           project='my-project',topic='my_topic',
           messages=[m1, m2, m3],
           create_topic=True,
           dag=dag)

   ``project`` , ``topic``, and ``messages`` are templated so you can use
   variables in them.

   :param project_id: Optional, the Google Cloud project ID in which to work (templated).
       If set to None or missing, the default project_id from the Google Cloud connection is used.
   :type project_id: str
   :param topic: the topic to which to publish. Do not include the
       full topic path. In other words, instead of
       ``projects/{project}/topics/{topic}``, provide only
       ``{topic}``. (templated)
   :type topic: str
   :param messages: a list of messages to be published to the
       topic. Each message is a dict with one or more of the
       following keys-value mappings:
       * 'data': a bytestring (utf-8 encoded)
       * 'attributes': {'key1': 'value1', ...}
       Each message must contain at least a non-empty 'data' value
       or an attribute dict with at least one key (templated). See
       https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
   :type messages: list
   :param gcp_conn_id: The connection ID to use connecting to
       Google Cloud.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param project: (Deprecated) the Google Cloud project ID where the topic will be created
   :type project: str
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
      :annotation: = ['project_id', 'topic', 'messages', 'impersonation_chain']

      

   .. attribute:: ui_color
      :annotation: = #0273d4

      

   
   .. method:: execute(self, context)




.. py:class:: PubSubPullOperator(*, project_id: str, subscription: str, max_messages: int = 5, ack_messages: bool = False, messages_callback: Optional[Callable[[List[ReceivedMessage], Dict[str, Any]], Any]] = None, gcp_conn_id: str = 'google_cloud_default', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Pulls messages from a PubSub subscription and passes them through XCom.
   If the queue is empty, returns empty list - never waits for messages.
   If you do need to wait, please use :class:`airflow.providers.google.cloud.sensors.PubSubPullSensor`
   instead.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:PubSubPullSensor`

   This sensor operator will pull up to ``max_messages`` messages from the
   specified PubSub subscription. When the subscription returns messages,
   the poke method's criteria will be fulfilled and the messages will be
   returned from the operator and passed through XCom for downstream tasks.

   If ``ack_messages`` is set to True, messages will be immediately
   acknowledged before being returned, otherwise, downstream tasks will be
   responsible for acknowledging them.

   ``project`` and ``subscription`` are templated so you can use
   variables in them.

   :param project: the Google Cloud project ID for the subscription (templated)
   :type project: str
   :param subscription: the Pub/Sub subscription name. Do not include the
       full subscription path.
   :type subscription: str
   :param max_messages: The maximum number of messages to retrieve per
       PubSub pull request
   :type max_messages: int
   :param ack_messages: If True, each message will be acknowledged
       immediately rather than by any downstream tasks
   :type ack_messages: bool
   :param gcp_conn_id: The connection ID to use connecting to
       Google Cloud.
   :type gcp_conn_id: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param messages_callback: (Optional) Callback to process received messages.
       It's return value will be saved to XCom.
       If you are pulling large messages, you probably want to provide a custom callback.
       If not provided, the default implementation will convert `ReceivedMessage` objects
       into JSON-serializable dicts using `google.protobuf.json_format.MessageToDict` function.
   :type messages_callback: Optional[Callable[[List[ReceivedMessage], Dict[str, Any]], Any]]
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
      :annotation: = ['project_id', 'subscription', 'impersonation_chain']

      

   
   .. method:: execute(self, context)



   
   .. method:: _default_message_callback(self, pulled_messages: List[ReceivedMessage], context: Dict[str, Any])

      This method can be overridden by subclasses or by `messages_callback` constructor argument.
      This default implementation converts `ReceivedMessage` objects into JSON-serializable dicts.

      :param pulled_messages: messages received from the topic.
      :type pulled_messages: List[ReceivedMessage]
      :param context: same as in `execute`
      :return: value to be saved to XCom.




