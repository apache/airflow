:mod:`airflow.providers.slack.hooks.slack_webhook`
==================================================

.. py:module:: airflow.providers.slack.hooks.slack_webhook


Module Contents
---------------

.. py:class:: SlackWebhookHook(http_conn_id=None, webhook_token=None, message='', attachments=None, blocks=None, channel=None, username=None, icon_emoji=None, icon_url=None, link_names=False, proxy=None, *args, **kwargs)

   Bases: :class:`airflow.providers.http.hooks.http.HttpHook`

   This hook allows you to post messages to Slack using incoming webhooks.
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

   
   .. method:: _get_token(self, token: str, http_conn_id: Optional[str])

      Given either a manually set token or a conn_id, return the webhook_token to use.

      :param token: The manually provided token
      :type token: str
      :param http_conn_id: The conn_id provided
      :type http_conn_id: str
      :return: webhook_token to use
      :rtype: str



   
   .. method:: _build_slack_message(self)

      Construct the Slack message. All relevant parameters are combined here to a valid
      Slack json message.

      :return: Slack message to send
      :rtype: str



   
   .. method:: execute(self)

      Remote Popen (actually execute the slack webhook call)




