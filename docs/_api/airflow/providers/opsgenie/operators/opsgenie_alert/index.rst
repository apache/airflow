:mod:`airflow.providers.opsgenie.operators.opsgenie_alert`
==========================================================

.. py:module:: airflow.providers.opsgenie.operators.opsgenie_alert


Module Contents
---------------

.. py:class:: OpsgenieAlertOperator(*, message: str, opsgenie_conn_id: str = 'opsgenie_default', alias: Optional[str] = None, description: Optional[str] = None, responders: Optional[List[dict]] = None, visible_to: Optional[List[dict]] = None, actions: Optional[List[dict]] = None, tags: Optional[List[dict]] = None, details: Optional[dict] = None, entity: Optional[str] = None, source: Optional[str] = None, priority: Optional[str] = None, user: Optional[str] = None, note: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator allows you to post alerts to Opsgenie.
   Accepts a connection that has an Opsgenie API key as the connection's password.
   This operator sets the domain to conn_id.host, and if not set will default
   to ``https://api.opsgenie.com``.

   Each Opsgenie API key can be pre-configured to a team integration.
   You can override these defaults in this operator.

   :param opsgenie_conn_id: The name of the Opsgenie connection to use
   :type opsgenie_conn_id: str
   :param message: The Message of the Opsgenie alert (templated)
   :type message: str
   :param alias: Client-defined identifier of the alert (templated)
   :type alias: str
   :param description: Description field of the alert (templated)
   :type description: str
   :param responders: Teams, users, escalations and schedules that
       the alert will be routed to send notifications.
   :type responders: list[dict]
   :param visible_to: Teams and users that the alert will become visible
       to without sending any notification.
   :type visible_to: list[dict]
   :param actions: Custom actions that will be available for the alert.
   :type actions: list[str]
   :param tags: Tags of the alert.
   :type tags: list[str]
   :param details: Map of key-value pairs to use as custom properties of the alert.
   :type details: dict
   :param entity: Entity field of the alert that is
       generally used to specify which domain alert is related to. (templated)
   :type entity: str
   :param source: Source field of the alert. Default value is
       IP address of the incoming request.
   :type source: str
   :param priority: Priority level of the alert. Default value is P3. (templated)
   :type priority: str
   :param user: Display name of the request owner.
   :type user: str
   :param note: Additional note that will be added while creating the alert. (templated)
   :type note: str

   .. attribute:: template_fields
      :annotation: = ['message', 'alias', 'description', 'entity', 'priority', 'note']

      

   
   .. method:: _build_opsgenie_payload(self)

      Construct the Opsgenie JSON payload. All relevant parameters are combined here
      to a valid Opsgenie JSON payload.

      :return: Opsgenie payload (dict) to send



   
   .. method:: execute(self, context)

      Call the OpsgenieAlertHook to post message




