:mod:`airflow.providers.qubole.sensors.qubole`
==============================================

.. py:module:: airflow.providers.qubole.sensors.qubole


Module Contents
---------------

.. py:class:: QuboleSensor(*, data, qubole_conn_id: str = 'qubole_default', **kwargs)

   Bases: :class:`airflow.sensors.base_sensor_operator.BaseSensorOperator`

   Base class for all Qubole Sensors

   .. attribute:: template_fields
      :annotation: = ['data', 'qubole_conn_id']

      

   .. attribute:: template_ext
      :annotation: = ['.txt']

      

   
   .. method:: poke(self, context: dict)




.. py:class:: QuboleFileSensor(**kwargs)

   Bases: :class:`airflow.providers.qubole.sensors.qubole.QuboleSensor`

   Wait for a file or folder to be present in cloud storage
   and check for its presence via QDS APIs

   :param qubole_conn_id: Connection id which consists of qds auth_token
   :type qubole_conn_id: str
   :param data: a JSON object containing payload, whose presence needs to be checked
       Check this `example <https://github.com/apache/airflow/blob/master        /airflow/providers/qubole/example_dags/example_qubole_sensor.py>`_ for sample payload
       structure.
   :type data: dict

   .. note:: Both ``data`` and ``qubole_conn_id`` fields support templating. You can
       also use ``.txt`` files for template-driven use cases.


.. py:class:: QubolePartitionSensor(**kwargs)

   Bases: :class:`airflow.providers.qubole.sensors.qubole.QuboleSensor`

   Wait for a Hive partition to show up in QHS (Qubole Hive Service)
   and check for its presence via QDS APIs

   :param qubole_conn_id: Connection id which consists of qds auth_token
   :type qubole_conn_id: str
   :param data: a JSON object containing payload, whose presence needs to be checked.
       Check this `example <https://github.com/apache/airflow/blob/master        /airflow/providers/qubole/example_dags/example_qubole_sensor.py>`_ for sample payload
       structure.
   :type data: dict

   .. note:: Both ``data`` and ``qubole_conn_id`` fields support templating. You can
       also use ``.txt`` files for template-driven use cases.


