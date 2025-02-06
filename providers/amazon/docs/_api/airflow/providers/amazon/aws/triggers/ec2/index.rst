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

:py:mod:`airflow.providers.amazon.aws.triggers.ec2`
===================================================

.. py:module:: airflow.providers.amazon.aws.triggers.ec2


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.ec2.EC2StateSensorTrigger




.. py:class:: EC2StateSensorTrigger(instance_id, target_state, aws_conn_id = 'aws_default', region_name = None, poll_interval = 60)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Poll the EC2 instance and yield a TriggerEvent once the state of the instance matches the target_state.

   :param instance_id: id of the AWS EC2 instance
   :param target_state: target state of instance
   :param aws_conn_id: aws connection to use
   :param region_name: (optional) aws region name associated with the client
   :param poll_interval: number of seconds to wait before attempting the next poll

   .. py:method:: serialize()

      Return the information needed to reconstruct this Trigger.

      :return: Tuple of (class path, keyword arguments needed to re-instantiate).


   .. py:method:: hook()


   .. py:method:: run()
      :async:

      Run the trigger in an asynchronous context.

      The trigger should yield an Event whenever it wants to fire off
      an event, and return None if it is finished. Single-event triggers
      should thus yield and then immediately return.

      If it yields, it is likely that it will be resumed very quickly,
      but it may not be (e.g. if the workload is being moved to another
      triggerer process, or a multi-event trigger was being used for a
      single-event task defer).

      In either case, Trigger classes should assume they will be persisted,
      and then rely on cleanup() being called when they are no longer needed.
