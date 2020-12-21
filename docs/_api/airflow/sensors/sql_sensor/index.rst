:mod:`airflow.sensors.sql_sensor`
=================================

.. py:module:: airflow.sensors.sql_sensor


Module Contents
---------------

.. py:class:: SqlSensor(*, conn_id, sql, parameters=None, success=None, failure=None, fail_on_empty=False, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Runs a sql statement repeatedly until a criteria is met. It will keep trying until
   success or failure criteria are met, or if the first cell is not in (0, '0', '', None).
   Optional success and failure callables are called with the first cell returned as the argument.
   If success callable is defined the sensor will keep retrying until the criteria is met.
   If failure callable is defined and the criteria is met the sensor will raise AirflowException.
   Failure criteria is evaluated before success criteria. A fail_on_empty boolean can also
   be passed to the sensor in which case it will fail if no rows have been returned

   :param conn_id: The connection to run the sensor against
   :type conn_id: str
   :param sql: The sql to run. To pass, it needs to return at least one cell
       that contains a non-zero / empty string value.
   :type sql: str
   :param parameters: The parameters to render the SQL query with (optional).
   :type parameters: dict or iterable
   :param success: Success criteria for the sensor is a Callable that takes first_cell
       as the only argument, and returns a boolean (optional).
   :type: success: Optional<Callable[[Any], bool]>
   :param failure: Failure criteria for the sensor is a Callable that takes first_cell
       as the only argument and return a boolean (optional).
   :type: failure: Optional<Callable[[Any], bool]>
   :param fail_on_empty: Explicitly fail on no rows returned.
   :type: fail_on_empty: bool

   .. attribute:: template_fields
      :annotation: :Iterable[str] = ['sql']

      

   .. attribute:: template_ext
      :annotation: :Iterable[str] = ['.hql', '.sql']

      

   .. attribute:: ui_color
      :annotation: = #7c7287

      

   
   .. method:: _get_hook(self)



   
   .. method:: poke(self, context)




