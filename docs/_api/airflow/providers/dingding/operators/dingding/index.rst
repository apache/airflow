:mod:`airflow.providers.dingding.operators.dingding`
====================================================

.. py:module:: airflow.providers.dingding.operators.dingding


Module Contents
---------------

.. py:class:: DingdingOperator(*, dingding_conn_id: str = 'dingding_default', message_type: str = 'text', message: Union[str, dict, None] = None, at_mobiles: Optional[List[str]] = None, at_all: bool = False, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator allows you send Dingding message using Dingding custom bot.
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

   .. attribute:: template_fields
      :annotation: = ['message']

      

   .. attribute:: ui_color
      :annotation: = #4ea4d4

      

   
   .. method:: execute(self, context)




