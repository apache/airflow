:mod:`airflow.providers.slack.hooks.slack`
==========================================

.. py:module:: airflow.providers.slack.hooks.slack

.. autoapi-nested-parse::

   Hook for Slack



Module Contents
---------------

.. py:class:: SlackHook(token: Optional[str] = None, slack_conn_id: Optional[str] = None, **client_args)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Creates a Slack connection, to be used for calls. Takes both Slack API token directly and
   connection that has Slack API token. If both supplied, Slack API token will be used.
   Exposes also the rest of slack.WebClient args
   Examples:

   .. code-block:: python

       # Create hook
       slack_hook = SlackHook(token="xxx")  # or slack_hook = SlackHook(slack_conn_id="slack")

       # Call generic API with parameters (errors are handled by hook)
       #  For more details check https://api.slack.com/methods/chat.postMessage
       slack_hook.call("chat.postMessage", json={"channel": "#random", "text": "Hello world!"})

       # Call method from Slack SDK (you have to handle errors yourself)
       #  For more details check https://slack.dev/python-slackclient/basic_usage.html#sending-a-message
       slack_hook.client.chat_postMessage(channel="#random", text="Hello world!")

   :param token: Slack API token
   :type token: str
   :param slack_conn_id: connection that has Slack API token in the password field
   :type slack_conn_id: str
   :param use_session: A boolean specifying if the client should take advantage of
       connection pooling. Default is True.
   :type use_session: bool
   :param base_url: A string representing the Slack API base URL. Default is
       ``https://www.slack.com/api/``
   :type base_url: str
   :param timeout: The maximum number of seconds the client will wait
       to connect and receive a response from Slack. Default is 30 seconds.
   :type timeout: int

   
   .. method:: __get_token(self, token: Any, slack_conn_id: Any)



   
   .. method:: call(self, api_method: str, *args, **kwargs)

      Calls Slack WebClient `WebClient.api_call` with given arguments.

      :param api_method: The target Slack API method. e.g. 'chat.postMessage'. Required.
      :type api_method: str
      :param http_verb: HTTP Verb. Optional (defaults to 'POST')
      :type http_verb: str
      :param files: Files to multipart upload. e.g. {imageORfile: file_objectORfile_path}
      :type files: dict
      :param data: The body to attach to the request. If a dictionary is provided,
          form-encoding will take place. Optional.
      :type data: dict or aiohttp.FormData
      :param params: The URL parameters to append to the URL. Optional.
      :type params: dict
      :param json: JSON for the body to attach to the request. Optional.
      :type json: dict




