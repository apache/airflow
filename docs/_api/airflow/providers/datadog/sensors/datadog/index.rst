:mod:`airflow.providers.datadog.sensors.datadog`
================================================

.. py:module:: airflow.providers.datadog.sensors.datadog


Module Contents
---------------

.. py:class:: DatadogSensor(*, datadog_conn_id: str = 'datadog_default', from_seconds_ago: int = 3600, up_to_seconds_from_now: int = 0, priority: Optional[str] = None, sources: Optional[str] = None, tags: Optional[List[str]] = None, response_check: Optional[Callable[[Dict[str, Any]], bool]] = None, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   A sensor to listen, with a filter, to datadog event streams and determine
   if some event was emitted.

   Depends on the datadog API, which has to be deployed on the same server where
   Airflow runs.

   :param datadog_conn_id: The connection to datadog, containing metadata for api keys.
   :param datadog_conn_id: str

   .. attribute:: ui_color
      :annotation: = #66c3dd

      

   
   .. method:: poke(self, context: Dict[str, Any])




