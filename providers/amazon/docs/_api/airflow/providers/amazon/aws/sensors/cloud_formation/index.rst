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

:py:mod:`airflow.providers.amazon.aws.sensors.cloud_formation`
==============================================================

.. py:module:: airflow.providers.amazon.aws.sensors.cloud_formation

.. autoapi-nested-parse::

   This module contains sensors for AWS CloudFormation.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.cloud_formation.CloudFormationCreateStackSensor
   airflow.providers.amazon.aws.sensors.cloud_formation.CloudFormationDeleteStackSensor




.. py:class:: CloudFormationCreateStackSensor(*, stack_name, aws_conn_id='aws_default', region_name=None, **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Waits for a stack to be created successfully on AWS CloudFormation.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:CloudFormationCreateStackSensor`

   :param stack_name: The name of the stack to wait for (templated)
   :param aws_conn_id: ID of the Airflow connection where credentials and extra configuration are
       stored
   :param poke_interval: Time in seconds that the job should wait between each try

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('stack_name',)



   .. py:attribute:: ui_color
      :value: '#C5CAE9'



   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: hook()

      Create and return a CloudFormationHook.



.. py:class:: CloudFormationDeleteStackSensor(*, stack_name, aws_conn_id = 'aws_default', region_name = None, **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Waits for a stack to be deleted successfully on AWS CloudFormation.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:CloudFormationDeleteStackSensor`

   :param stack_name: The name of the stack to wait for (templated)
   :param aws_conn_id: ID of the Airflow connection where credentials and extra configuration are
       stored
   :param poke_interval: Time in seconds that the job should wait between each try

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('stack_name',)



   .. py:attribute:: ui_color
      :value: '#C5CAE9'



   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: hook()

      Create and return a CloudFormationHook.
