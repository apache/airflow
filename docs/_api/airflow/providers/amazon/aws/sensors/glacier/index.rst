:mod:`airflow.providers.amazon.aws.sensors.glacier`
===================================================

.. py:module:: airflow.providers.amazon.aws.sensors.glacier


Module Contents
---------------

.. py:class:: JobStatus

   Bases: :class:`enum.Enum`

   Glacier jobs description

   .. attribute:: IN_PROGRESS
      :annotation: = InProgress

      

   .. attribute:: SUCCEEDED
      :annotation: = Succeeded

      


.. py:class:: GlacierJobOperationSensor(*, aws_conn_id: str = 'aws_default', vault_name: str, job_id: str, poke_interval: int = 60 * 20, mode: str = 'reschedule', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Glacier sensor for checking job state. This operator runs only in reschedule mode.

   :param aws_conn_id: The reference to the AWS connection details
   :type aws_conn_id: str
   :param vault_name: name of Glacier vault on which job is executed
   :type vault_name: str
   :param job_id: the job ID was returned by retrieve_inventory()
   :type job_id: str
   :param poke_interval: Time in seconds that the job should wait in
       between each tries
   :type poke_interval: float
   :param mode: How the sensor operates.
       Options are: ``{ poke | reschedule }``, default is ``poke``.
       When set to ``poke`` the sensor is taking up a worker slot for its
       whole execution time and sleeps between pokes. Use this mode if the
       expected runtime of the sensor is short or if a short poke interval
       is required. Note that the sensor will hold onto a worker slot and
       a pool slot for the duration of the sensor's runtime in this mode.
       When set to ``reschedule`` the sensor task frees the worker slot when
       the criteria is not yet met and it's rescheduled at a later time. Use
       this mode if the time before the criteria is met is expected to be
       quite long. The poke interval should be more than one minute to
       prevent too much load on the scheduler.
   :type mode: str

   .. attribute:: template_fields
      :annotation: = ['vault_name', 'job_id']

      

   
   .. method:: poke(self, context)




