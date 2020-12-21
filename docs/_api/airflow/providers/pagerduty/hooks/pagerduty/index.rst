:mod:`airflow.providers.pagerduty.hooks.pagerduty`
==================================================

.. py:module:: airflow.providers.pagerduty.hooks.pagerduty

.. autoapi-nested-parse::

   Hook for sending or receiving data from PagerDuty as well as creating PagerDuty incidents.



Module Contents
---------------

.. py:class:: PagerdutyHook(token: Optional[str] = None, pagerduty_conn_id: Optional[str] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`

   Takes both PagerDuty API token directly and connection that has PagerDuty API token.

   If both supplied, PagerDuty API token will be used.

   :param token: PagerDuty API token
   :param pagerduty_conn_id: connection that has PagerDuty API token in the password field

   
   .. method:: get_session(self)

      Returns `pdpyras.APISession` for use with sending or receiving data through the PagerDuty REST API.

      The `pdpyras` library supplies a class `pdpyras.APISession` extending `requests.Session` from the
      Requests HTTP library.

      Documentation on how to use the `APISession` class can be found at:
      https://pagerduty.github.io/pdpyras/#data-access-abstraction



   
   .. method:: create_event(self, summary: str, severity: str, source: str = 'airflow', action: str = 'trigger', routing_key: Optional[str] = None, dedup_key: Optional[str] = None, custom_details: Optional[Any] = None, group: Optional[str] = None, component: Optional[str] = None, class_type: Optional[str] = None, images: Optional[List[Any]] = None, links: Optional[List[Any]] = None)

      Create event for service integration.

      :param summary: Summary for the event
      :type summary: str
      :param severity: Severity for the event, needs to be one of: info, warning, error, critical
      :type severity: str
      :param source: Specific human-readable unique identifier, such as a
          hostname, for the system having the problem.
      :type source: str
      :param action: Event action, needs to be one of: trigger, acknowledge,
          resolve. Default to trigger if not specified.
      :type action: str
      :param routing_key: Integration key. If not specified, will try to read
          from connection's extra json blob.
      :type routing_key: str
      :param dedup_key: A string which identifies the alert triggered for the given event.
          Required for the actions acknowledge and resolve.
      :type dedup_key: str
      :param custom_details: Free-form details from the event. Can be a dictionary or a string.
          If a dictionary is passed it will show up in PagerDuty as a table.
      :type custom_details: dict or str
      :param group: A cluster or grouping of sources. For example, sources
          “prod-datapipe-02” and “prod-datapipe-03” might both be part of “prod-datapipe”
      :type group: str
      :param component: The part or component of the affected system that is broken.
      :type component: str
      :param class_type: The class/type of the event.
      :type class_type: str
      :param images: List of images to include. Each dictionary in the list accepts the following keys:
          `src`: The source (URL) of the image being attached to the incident. This image must be served via
          HTTPS.
          `href`: [Optional] URL to make the image a clickable link.
          `alt`: [Optional] Alternative text for the image.
      :type images: list[dict]
      :param links: List of links to include. Each dictionary in the list accepts the following keys:
          `href`: URL of the link to be attached.
          `text`: [Optional] Plain text that describes the purpose of the link, and can be used as the
          link's text.
      :type links: list[dict]
      :return: PagerDuty Events API v2 response.
      :rtype: dict




