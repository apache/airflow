:mod:`airflow.providers.datadog.hooks.datadog`
==============================================

.. py:module:: airflow.providers.datadog.hooks.datadog


Module Contents
---------------

.. py:class:: DatadogHook(datadog_conn_id: str = 'datadog_default')

   Bases: :class:`airflow.hooks.base_hook.BaseHook`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Uses datadog API to send metrics of practically anything measurable,
   so it's possible to track # of db records inserted/deleted, records read
   from file and many other useful metrics.

   Depends on the datadog API, which has to be deployed on the same server where
   Airflow runs.

   :param datadog_conn_id: The connection to datadog, containing metadata for api keys.
   :param datadog_conn_id: str

   
   .. method:: validate_response(self, response: Dict[str, Any])

      Validate Datadog response



   
   .. method:: send_metric(self, metric_name: str, datapoint: Union[float, int], tags: Optional[List[str]] = None, type_: Optional[str] = None, interval: Optional[int] = None)

      Sends a single datapoint metric to DataDog

      :param metric_name: The name of the metric
      :type metric_name: str
      :param datapoint: A single integer or float related to the metric
      :type datapoint: int or float
      :param tags: A list of tags associated with the metric
      :type tags: list
      :param type_: Type of your metric: gauge, rate, or count
      :type type_: str
      :param interval: If the type of the metric is rate or count, define the corresponding interval
      :type interval: int



   
   .. method:: query_metric(self, query: str, from_seconds_ago: int, to_seconds_ago: int)

      Queries datadog for a specific metric, potentially with some
      function applied to it and returns the results.

      :param query: The datadog query to execute (see datadog docs)
      :type query: str
      :param from_seconds_ago: How many seconds ago to start querying for.
      :type from_seconds_ago: int
      :param to_seconds_ago: Up to how many seconds ago to query for.
      :type to_seconds_ago: int



   
   .. method:: post_event(self, title: str, text: str, aggregation_key: Optional[str] = None, alert_type: Optional[str] = None, date_happened: Optional[int] = None, handle: Optional[str] = None, priority: Optional[str] = None, related_event_id: Optional[int] = None, tags: Optional[List[str]] = None, device_name: Optional[List[str]] = None)

      Posts an event to datadog (processing finished, potentially alerts, other issues)
      Think about this as a means to maintain persistence of alerts, rather than
      alerting itself.

      :param title: The title of the event
      :type title: str
      :param text: The body of the event (more information)
      :type text: str
      :param aggregation_key: Key that can be used to aggregate this event in a stream
      :type aggregation_key: str
      :param alert_type: The alert type for the event, one of
          ["error", "warning", "info", "success"]
      :type alert_type: str
      :param date_happened: POSIX timestamp of the event; defaults to now
      :type date_happened: int
      :handle: User to post the event as; defaults to owner of the application key used
          to submit.
      :param handle: str
      :param priority: Priority to post the event as. ("normal" or "low", defaults to "normal")
      :type priority: str
      :param related_event_id: Post event as a child of the given event
      :type related_event_id: id
      :param tags: List of tags to apply to the event
      :type tags: list[str]
      :param device_name: device_name to post the event with
      :type device_name: list




