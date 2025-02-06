 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

:py:mod:`airflow.providers.amazon.aws.sensors.quicksight`
=========================================================

.. py:module:: airflow.providers.amazon.aws.sensors.quicksight


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.quicksight.QuickSightSensor




.. py:class:: QuickSightSensor(*, data_set_id, ingestion_id, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Watches for the status of an Amazon QuickSight Ingestion.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:QuickSightSensor`

   :param data_set_id:  ID of the dataset used in the ingestion.
   :param ingestion_id: ID for the ingestion.
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('data_set_id', 'ingestion_id', 'aws_conn_id')



   .. py:method:: poke(context)

      Pokes until the QuickSight Ingestion has successfully finished.

      :param context: The task context during execution.
      :return: True if it COMPLETED and False if not.


   .. py:method:: quicksight_hook()


   .. py:method:: sts_hook()
