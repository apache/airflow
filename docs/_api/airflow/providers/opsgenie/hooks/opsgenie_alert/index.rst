:mod:`airflow.providers.opsgenie.hooks.opsgenie_alert`
======================================================

.. py:module:: airflow.providers.opsgenie.hooks.opsgenie_alert


Module Contents
---------------

.. py:class:: OpsgenieAlertHook(opsgenie_conn_id: str = 'opsgenie_default', *args, **kwargs)

   Bases: :class:`airflow.providers.http.hooks.http.HttpHook`

   This hook allows you to post alerts to Opsgenie.
   Accepts a connection that has an Opsgenie API key as the connection's password.
   This hook sets the domain to conn_id.host, and if not set will default
   to ``https://api.opsgenie.com``.

   Each Opsgenie API key can be pre-configured to a team integration.
   You can override these defaults in this hook.

   :param opsgenie_conn_id: The name of the Opsgenie connection to use
   :type opsgenie_conn_id: str

   
   .. method:: _get_api_key(self)

      Get Opsgenie api_key for creating alert



   
   .. method:: get_conn(self, headers: Optional[dict] = None)

      Overwrite HttpHook get_conn because this hook just needs base_url
      and headers, and does not need generic params

      :param headers: additional headers to be passed through as a dictionary
      :type headers: dict



   
   .. method:: execute(self, payload: Optional[dict] = None)

      Execute the Opsgenie Alert call

      :param payload: Opsgenie API Create Alert payload values
          See https://docs.opsgenie.com/docs/alert-api#section-create-alert
      :type payload: dict




