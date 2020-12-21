:mod:`airflow.providers.google.cloud.sensors.pubsub`
====================================================

.. py:module:: airflow.providers.google.cloud.sensors.pubsub

.. autoapi-nested-parse::

   This module contains a Google PubSub sensor.



Module Contents
---------------

.. py:class:: PubSubPullSensor(*, project_id: str, subscription: str, max_messages: int = 5, return_immediately: bool = True, ack_messages: bool = False, gcp_conn_id: str = 'google_cloud_default', messages_callback: Optional[Callable[[List[ReceivedMessage], Dict[str, Any]], Any]] = None, delegate_to: Optional[str] = None, project: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Pulls messages from a PubSub subscription and passes them through XCom.
   Always waits for at least one message to be returned from the subscription.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:PubSubPullSensor`

   .. seealso::
       If you don't want to wait for at least one message to come, use Operator instead:
       :class:`~airflow.providers.google.cloud.operators.pubsub.PubSubPullOperator`

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
   :param return_immediately:
       (Deprecated) This is an underlying PubSub API implementation detail.
       It has no real effect on Sensor behaviour other than some internal wait time before retrying
       on empty queue.
       The Sensor task will (by definition) always wait for a message, regardless of this argument value.

       If you want a non-blocking task that does not to wait for messages, please use
       :class:`~airflow.providers.google.cloud.operators.pubsub.PubSubPullOperator`
       instead.
   :type return_immediately: bool
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

      

   .. attribute:: ui_color
      :annotation: = #ff7f50

      

   
   .. method:: execute(self, context: dict)

      Overridden to allow messages to be passed



   
   .. method:: poke(self, context: dict)



   
   .. method:: _default_message_callback(self, pulled_messages: List[ReceivedMessage], context: Dict[str, Any])

      This method can be overridden by subclasses or by `messages_callback` constructor argument.
      This default implementation converts `ReceivedMessage` objects into JSON-serializable dicts.

      :param pulled_messages: messages received from the topic.
      :type pulled_messages: List[ReceivedMessage]
      :param context: same as in `execute`
      :return: value to be saved to XCom.




