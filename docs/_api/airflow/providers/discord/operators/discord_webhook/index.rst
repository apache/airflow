:mod:`airflow.providers.discord.operators.discord_webhook`
==========================================================

.. py:module:: airflow.providers.discord.operators.discord_webhook


Module Contents
---------------

.. py:class:: DiscordWebhookOperator(*, http_conn_id: Optional[str] = None, webhook_endpoint: Optional[str] = None, message: str = '', username: Optional[str] = None, avatar_url: Optional[str] = None, tts: bool = False, proxy: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.providers.http.operators.http.SimpleHttpOperator`

   This operator allows you to post messages to Discord using incoming webhooks.
   Takes a Discord connection ID with a default relative webhook endpoint. The
   default endpoint can be overridden using the webhook_endpoint parameter
   (https://discordapp.com/developers/docs/resources/webhook).

   Each Discord webhook can be pre-configured to use a specific username and
   avatar_url. You can override these defaults in this operator.

   :param http_conn_id: Http connection ID with host as "https://discord.com/api/" and
                        default webhook endpoint in the extra field in the form of
                        {"webhook_endpoint": "webhooks/{webhook.id}/{webhook.token}"}
   :type http_conn_id: str
   :param webhook_endpoint: Discord webhook endpoint in the form of
                            "webhooks/{webhook.id}/{webhook.token}"
   :type webhook_endpoint: str
   :param message: The message you want to send to your Discord channel
                   (max 2000 characters). (templated)
   :type message: str
   :param username: Override the default username of the webhook. (templated)
   :type username: str
   :param avatar_url: Override the default avatar of the webhook
   :type avatar_url: str
   :param tts: Is a text-to-speech message
   :type tts: bool
   :param proxy: Proxy to use to make the Discord webhook call
   :type proxy: str

   .. attribute:: template_fields
      :annotation: = ['username', 'message']

      

   
   .. method:: execute(self, context: Dict)

      Call the DiscordWebhookHook to post message




