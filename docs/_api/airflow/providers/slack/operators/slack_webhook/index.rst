:mod:`airflow.providers.slack.operators.slack_webhook`
======================================================

.. py:module:: airflow.providers.slack.operators.slack_webhook


Module Contents
---------------

.. py:class:: SlackWebhookOperator(*, http_conn_id: str, webhook_token: Optional[str] = None, message: str = '', attachments: Optional[list] = None, blocks: Optional[list] = None, channel: Optional[str] = None, username: Optional[str] = None, icon_emoji: Optional[str] = None, icon_url: Optional[str] = None, link_names: bool = False, proxy: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.providers.http.operators.http.SimpleHttpOperator`

   This operator allows you to post messages to Slack using incoming webhooks.
   Takes both Slack webhook token directly and connection that has Slack webhook token.
   If both supplied, http_conn_id will be used as base_url,
   and webhook_token will be taken as endpoint, the relative path of the url.

   Each Slack webhook token can be pre-configured to use a specific channel, username and
   icon. You can override these defaults in this hook.

   :param http_conn_id: connection that has Slack webhook token in the extra field
   :type http_conn_id: str
   :param webhook_token: Slack webhook token
   :type webhook_token: str
   :param message: The message you want to send on Slack
   :type message: str
   :param attachments: The attachments to send on Slack. Should be a list of
       dictionaries representing Slack attachments.
   :type attachments: list
   :param blocks: The blocks to send on Slack. Should be a list of
       dictionaries representing Slack blocks.
   :type blocks: list
   :param channel: The channel the message should be posted to
   :type channel: str
   :param username: The username to post to slack with
   :type username: str
   :param icon_emoji: The emoji to use as icon for the user posting to Slack
   :type icon_emoji: str
   :param icon_url: The icon image URL string to use in place of the default icon.
   :type icon_url: str
   :param link_names: Whether or not to find and link channel and usernames in your
       message
   :type link_names: bool
   :param proxy: Proxy to use to make the Slack webhook call
   :type proxy: str

   .. attribute:: template_fields
      :annotation: = ['webhook_token', 'message', 'attachments', 'blocks', 'channel', 'username', 'proxy']

      

   
   .. method:: execute(self, context: Dict[str, Any])

      Call the SlackWebhookHook to post the provided Slack message




