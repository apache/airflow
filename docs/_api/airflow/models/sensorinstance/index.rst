:mod:`airflow.models.sensorinstance`
====================================

.. py:module:: airflow.models.sensorinstance


Module Contents
---------------

.. py:class:: SensorInstance(ti)

   Bases: :class:`airflow.models.base.Base`

   SensorInstance support the smart sensor service. It stores the sensor task states
   and context that required for poking include poke context and execution context.
   In sensor_instance table we also save the sensor operator classpath so that inside
   smart sensor there is no need to import the dagbag and create task object for each
   sensor task.

   SensorInstance include another set of columns to support the smart sensor shard on
   large number of sensor instance. The key idea is to generate the hash code from the
   poke context and use it to map to a shorter shard code which can be used as an index.
   Every smart sensor process takes care of tasks whose `shardcode` are in a certain range.

   .. attribute:: __tablename__
      :annotation: = sensor_instance

      

   .. attribute:: id
      

      

   .. attribute:: task_id
      

      

   .. attribute:: dag_id
      

      

   .. attribute:: execution_date
      

      

   .. attribute:: state
      

      

   .. attribute:: _try_number
      

      

   .. attribute:: start_date
      

      

   .. attribute:: operator
      

      

   .. attribute:: op_classpath
      

      

   .. attribute:: hashcode
      

      

   .. attribute:: shardcode
      

      

   .. attribute:: poke_context
      

      

   .. attribute:: execution_context
      

      

   .. attribute:: created_at
      

      

   .. attribute:: updated_at
      

      

   .. attribute:: __table_args__
      

      

   .. attribute:: try_number
      

      Return the try number that this task number will be when it is actually
      run.
      If the TI is currently running, this will match the column in the
      database, in all other cases this will be incremented.


   
   .. staticmethod:: get_classpath(obj)

      Get the object dotted class path. Used for getting operator classpath.

      :param obj:
      :type obj:
      :return: The class path of input object
      :rtype: str



   
   .. classmethod:: register(cls, ti, poke_context, execution_context, session=None)

      Register task instance ti for a sensor in sensor_instance table. Persist the
      context used for a sensor and set the sensor_instance table state to sensing.

      :param ti: The task instance for the sensor to be registered.
      :type: ti:
      :param poke_context: Context used for sensor poke function.
      :type poke_context: dict
      :param execution_context: Context used for execute sensor such as timeout
          setting and email configuration.
      :type execution_context: dict
      :param session: SQLAlchemy ORM Session
      :type session: Session
      :return: True if the ti was registered successfully.
      :rtype: Boolean



   
   .. method:: __repr__(self)




