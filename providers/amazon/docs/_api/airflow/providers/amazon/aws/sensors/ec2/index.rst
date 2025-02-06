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

:py:mod:`airflow.providers.amazon.aws.sensors.ec2`
==================================================

.. py:module:: airflow.providers.amazon.aws.sensors.ec2


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.ec2.EC2InstanceStateSensor




.. py:class:: EC2InstanceStateSensor(*, target_state, instance_id, aws_conn_id = 'aws_default', region_name = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Poll the state of the AWS EC2 instance until the instance reaches the target state.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:EC2InstanceStateSensor`

   :param target_state: target state of instance
   :param instance_id: id of the AWS EC2 instance
   :param region_name: (optional) aws region name associated with the client
   :param deferrable: if True, the sensor will run in deferrable mode

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('target_state', 'instance_id', 'region_name')



   .. py:attribute:: ui_color
      :value: '#cc8811'



   .. py:attribute:: ui_fgcolor
      :value: '#ffffff'



   .. py:attribute:: valid_states
      :value: ['running', 'stopped', 'terminated']



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: hook()


   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: execute_complete(context, event=None)
