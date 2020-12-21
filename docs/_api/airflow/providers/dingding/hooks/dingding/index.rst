:mod:`airflow.providers.dingding.hooks.dingding`
================================================

.. py:module:: airflow.providers.dingding.hooks.dingding


Module Contents
---------------

.. py:class:: DingdingHook(dingding_conn_id='dingding_default', message_type: str = 'text', message: Optional[Union[str, dict]] = None, at_mobiles: Optional[List[str]] = None, at_all: bool = False, *args, **kwargs)

   Bases: :class:`airflow.providers.http.hooks.http.HttpHook`

   This hook allows you send Dingding message using Dingding custom bot.
   Get Dingding token from conn_id.password. And prefer set domain to
   conn_id.host, if not will use default ``https://oapi.dingtalk.com``.

   For more detail message in
   `Dingding custom bot <https://open-doc.dingtalk.com/microapp/serverapi2/qf2nxq>`_

   :param dingding_conn_id: The name of the Dingding connection to use
   :type dingding_conn_id: str
   :param message_type: Message type you want to send to Dingding, support five type so far
       including text, link, markdown, actionCard, feedCard
   :type message_type: str
   :param message: The message send to Dingding chat group
   :type message: str or dict
   :param at_mobiles: Remind specific users with this message
   :type at_mobiles: list[str]
   :param at_all: Remind all people in group or not. If True, will overwrite ``at_mobiles``
   :type at_all: bool

   
   .. method:: _get_endpoint(self)

      Get Dingding endpoint for sending message.



   
   .. method:: _build_message(self)

      Build different type of Dingding message
      As most commonly used type, text message just need post message content
      rather than a dict like ``{'content': 'message'}``



   
   .. method:: get_conn(self, headers: Optional[dict] = None)

      Overwrite HttpHook get_conn because just need base_url and headers and
      not don't need generic params

      :param headers: additional headers to be passed through as a dictionary
      :type headers: dict



   
   .. method:: send(self)

      Send Dingding message




