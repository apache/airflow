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

:py:mod:`airflow.providers.amazon.aws.triggers.rds`
===================================================

.. py:module:: airflow.providers.amazon.aws.triggers.rds


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.rds.RdsDbInstanceTrigger
   airflow.providers.amazon.aws.triggers.rds.RdsDbAvailableTrigger
   airflow.providers.amazon.aws.triggers.rds.RdsDbDeletedTrigger
   airflow.providers.amazon.aws.triggers.rds.RdsDbStoppedTrigger




.. py:class:: RdsDbInstanceTrigger(waiter_name, db_instance_identifier, waiter_delay, waiter_max_attempts, aws_conn_id, region_name, response)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Deprecated Trigger for RDS operations. Do not use.

   :param waiter_name: Name of the waiter to use, for instance 'db_instance_available'
       or 'db_instance_deleted'.
   :param db_instance_identifier: The DB instance identifier for the DB instance to be polled.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region_name: AWS region where the DB is located, if different from the default one.
   :param response: The response from the RdsHook, to be passed back to the operator.

   .. py:method:: serialize()

      Return the information needed to reconstruct this Trigger.

      :return: Tuple of (class path, keyword arguments needed to re-instantiate).


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



.. py:class:: RdsDbAvailableTrigger(db_identifier, waiter_delay, waiter_max_attempts, aws_conn_id, response, db_type, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger to wait asynchronously for a DB instance or cluster to be available.

   :param db_identifier: The DB identifier for the DB instance or cluster to be polled.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region_name: AWS region where the DB is located, if different from the default one.
   :param response: The response from the RdsHook, to be passed back to the operator.
   :param db_type: The type of DB: instance or cluster.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: RdsDbDeletedTrigger(db_identifier, waiter_delay, waiter_max_attempts, aws_conn_id, response, db_type, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger to wait asynchronously for a DB instance or cluster to be deleted.

   :param db_identifier: The DB identifier for the DB instance or cluster to be polled.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region_name: AWS region where the DB is located, if different from the default one.
   :param response: The response from the RdsHook, to be passed back to the operator.
   :param db_type: The type of DB: instance or cluster.

   .. py:method:: hook()

      Override in subclasses to return the right hook.



.. py:class:: RdsDbStoppedTrigger(db_identifier, waiter_delay, waiter_max_attempts, aws_conn_id, response, db_type, region_name = None)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Trigger to wait asynchronously for a DB instance or cluster to be stopped.

   :param db_identifier: The DB identifier for the DB instance or cluster to be polled.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials.
   :param region_name: AWS region where the DB is located, if different from the default one.
   :param response: The response from the RdsHook, to be passed back to the operator.
   :param db_type: The type of DB: instance or cluster.

   .. py:method:: hook()

      Override in subclasses to return the right hook.
