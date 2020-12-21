:mod:`airflow.sensors.external_task_sensor`
===========================================

.. py:module:: airflow.sensors.external_task_sensor


Module Contents
---------------

.. py:class:: ExternalTaskSensorLink

   Bases: :class:`airflow.models.BaseOperatorLink`

   Operator link for ExternalTaskSensor. It allows users to access
   DAG waited with ExternalTaskSensor.

   .. attribute:: name
      :annotation: = External DAG

      

   
   .. method:: get_link(self, operator, dttm)




.. py:class:: ExternalTaskSensor(*, external_dag_id, external_task_id=None, allowed_states=None, failed_states=None, execution_delta=None, execution_date_fn=None, check_existence=False, **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Waits for a different DAG or a task in a different DAG to complete for a
   specific execution_date

   :param external_dag_id: The dag_id that contains the task you want to
       wait for
   :type external_dag_id: str
   :param external_task_id: The task_id that contains the task you want to
       wait for. If ``None`` (default value) the sensor waits for the DAG
   :type external_task_id: str or None
   :param allowed_states: list of allowed states, default is ``['success']``
   :type allowed_states: list
   :param failed_states: list of failed or dis-allowed states, default is ``None``
   :type failed_states: list
   :param execution_delta: time difference with the previous execution to
       look at, the default is the same execution_date as the current task or DAG.
       For yesterday, use [positive!] datetime.timedelta(days=1). Either
       execution_delta or execution_date_fn can be passed to
       ExternalTaskSensor, but not both.
   :type execution_delta: Optional[datetime.timedelta]
   :param execution_date_fn: function that receives the current execution date as the first
       positional argument and optionally any number of keyword arguments available in the
       context dictionary, and returns the desired execution dates to query.
       Either execution_delta or execution_date_fn can be passed to ExternalTaskSensor,
       but not both.
   :type execution_date_fn: Optional[Callable]
   :param check_existence: Set to `True` to check if the external task exists (when
       external_task_id is not None) or check if the DAG to wait for exists (when
       external_task_id is None), and immediately cease waiting if the external task
       or DAG does not exist (default value: False).
   :type check_existence: bool

   .. attribute:: template_fields
      :annotation: = ['external_dag_id', 'external_task_id']

      

   .. attribute:: ui_color
      :annotation: = #19647e

      

   .. attribute:: operator_extra_links
      

      Return operator extra links


   
   .. method:: poke(self, context, session=None)



   
   .. method:: get_count(self, dttm_filter, session, states)

      Get the count of records against dttm filter and states

      :param dttm_filter: date time filter for execution date
      :type dttm_filter: list
      :param session: airflow session object
      :type session: SASession
      :param states: task or dag states
      :type states: list
      :return: count of record against the filters



   
   .. method:: _handle_execution_date_fn(self, context)

      This function is to handle backwards compatibility with how this operator was
      previously where it only passes the execution date, but also allow for the newer
      implementation to pass all context variables as keyword arguments, to allow
      for more sophisticated returns of dates to return.




.. py:class:: ExternalTaskMarker(*, external_dag_id, external_task_id, execution_date: Optional[Union[str, datetime.datetime]] = '{{ execution_date.isoformat() }}', recursion_depth: int = 10, **kwargs)

   Bases: :class:`airflow.operators.dummy_operator.DummyOperator`

   Use this operator to indicate that a task on a different DAG depends on this task.
   When this task is cleared with "Recursive" selected, Airflow will clear the task on
   the other DAG and its downstream tasks recursively. Transitive dependencies are followed
   until the recursion_depth is reached.

   :param external_dag_id: The dag_id that contains the dependent task that needs to be cleared.
   :type external_dag_id: str
   :param external_task_id: The task_id of the dependent task that needs to be cleared.
   :type external_task_id: str
   :param execution_date: The execution_date of the dependent task that needs to be cleared.
   :type execution_date: str or datetime.datetime
   :param recursion_depth: The maximum level of transitive dependencies allowed. Default is 10.
       This is mostly used for preventing cyclic dependencies. It is fine to increase
       this number if necessary. However, too many levels of transitive dependencies will make
       it slower to clear tasks in the web UI.

   .. attribute:: template_fields
      :annotation: = ['external_dag_id', 'external_task_id', 'execution_date']

      

   .. attribute:: ui_color
      :annotation: = #19647e

      

   .. attribute:: __serialized_fields
      :annotation: :Optional[FrozenSet[str]]

      

   
   .. classmethod:: get_serialized_fields(cls)

      Serialized ExternalTaskMarker contain exactly these fields + templated_fields .




