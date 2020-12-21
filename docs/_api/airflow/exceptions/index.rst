:mod:`airflow.exceptions`
=========================

.. py:module:: airflow.exceptions

.. autoapi-nested-parse::

   Exceptions used by Airflow



Module Contents
---------------

.. py:exception:: AirflowException

   Bases: :class:`Exception`

   Base class for all Airflow's errors.
   Each custom exception should be derived from this class

   .. attribute:: status_code
      :annotation: = 500

      


.. py:exception:: AirflowBadRequest

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when the application or server cannot handle the request

   .. attribute:: status_code
      :annotation: = 400

      


.. py:exception:: AirflowNotFoundException

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when the requested object/resource is not available in the system

   .. attribute:: status_code
      :annotation: = 404

      


.. py:exception:: AirflowConfigException

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when there is configuration problem


.. py:exception:: AirflowSensorTimeout

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when there is a timeout on sensor polling


.. py:exception:: AirflowRescheduleException(reschedule_date)

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when the task should be re-scheduled at a later time.

   :param reschedule_date: The date when the task should be rescheduled
   :type reschedule_date: datetime.datetime


.. py:exception:: AirflowSmartSensorException

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise after the task register itself in the smart sensor service
   It should exit without failing a task


.. py:exception:: InvalidStatsNameException

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when name of the stats is invalid


.. py:exception:: AirflowTaskTimeout

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when the task execution times-out


.. py:exception:: AirflowWebServerTimeout

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when the web server times out


.. py:exception:: AirflowSkipException

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when the task should be skipped


.. py:exception:: AirflowFailException

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when the task should be failed without retrying


.. py:exception:: AirflowDagCycleException

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when there is a cycle in Dag definition


.. py:exception:: AirflowClusterPolicyViolation

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when there is a violation of a Cluster Policy in Dag definition


.. py:exception:: DagNotFound

   Bases: :class:`airflow.exceptions.AirflowNotFoundException`

   Raise when a DAG is not available in the system


.. py:exception:: DagCodeNotFound

   Bases: :class:`airflow.exceptions.AirflowNotFoundException`

   Raise when a DAG code is not available in the system


.. py:exception:: DagRunNotFound

   Bases: :class:`airflow.exceptions.AirflowNotFoundException`

   Raise when a DAG Run is not available in the system


.. py:exception:: DagRunAlreadyExists

   Bases: :class:`airflow.exceptions.AirflowBadRequest`

   Raise when creating a DAG run for DAG which already has DAG run entry


.. py:exception:: DagFileExists

   Bases: :class:`airflow.exceptions.AirflowBadRequest`

   Raise when a DAG ID is still in DagBag i.e., DAG file is in DAG folder


.. py:exception:: DuplicateTaskIdFound

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when a Task with duplicate task_id is defined in the same DAG


.. py:exception:: SerializedDagNotFound

   Bases: :class:`airflow.exceptions.DagNotFound`

   Raise when DAG is not found in the serialized_dags table in DB


.. py:exception:: TaskNotFound

   Bases: :class:`airflow.exceptions.AirflowNotFoundException`

   Raise when a Task is not available in the system


.. py:exception:: TaskInstanceNotFound

   Bases: :class:`airflow.exceptions.AirflowNotFoundException`

   Raise when a Task Instance is not available in the system


.. py:exception:: PoolNotFound

   Bases: :class:`airflow.exceptions.AirflowNotFoundException`

   Raise when a Pool is not available in the system


.. py:exception:: NoAvailablePoolSlot

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when there is not enough slots in pool


.. py:exception:: DagConcurrencyLimitReached

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when DAG concurrency limit is reached


.. py:exception:: TaskConcurrencyLimitReached

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when task concurrency limit is reached


.. py:exception:: BackfillUnfinished(message, ti_status)

   Bases: :class:`airflow.exceptions.AirflowException`

   Raises when not all tasks succeed in backfill.

   :param message: The human-readable description of the exception
   :param ti_status: The information about all task statuses


.. py:class:: FileSyntaxError

   Bases: :class:`typing.NamedTuple`

   Information about a single error in a file.

   .. attribute:: line_no
      :annotation: :Optional[int]

      

   .. attribute:: message
      :annotation: :str

      

   
   .. method:: __str__(self)




.. py:exception:: AirflowFileParseException(msg: str, file_path: str, parse_errors: List[FileSyntaxError])

   Bases: :class:`airflow.exceptions.AirflowException`

   Raises when connection or variable file can not be parsed

   :param msg: The human-readable description of the exception
   :param file_path: A processed file that contains errors
   :param parse_errors: File syntax errors

   
   .. method:: __str__(self)




.. py:exception:: ConnectionNotUnique

   Bases: :class:`airflow.exceptions.AirflowException`

   Raise when multiple values are found for the same conn_id


