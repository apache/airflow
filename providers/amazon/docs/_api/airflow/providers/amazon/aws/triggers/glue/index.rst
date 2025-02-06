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

:py:mod:`airflow.providers.amazon.aws.triggers.glue`
====================================================

.. py:module:: airflow.providers.amazon.aws.triggers.glue


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.glue.GlueJobCompleteTrigger
   airflow.providers.amazon.aws.triggers.glue.GlueCatalogPartitionTrigger




.. py:class:: GlueJobCompleteTrigger(job_name, run_id, verbose, aws_conn_id, job_poll_interval)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Watches for a glue job, triggers when it finishes.

   :param job_name: glue job name
   :param run_id: the ID of the specific run to watch for that job
   :param verbose: whether to print the job's logs in airflow logs or not
   :param aws_conn_id: The Airflow connection used for AWS credentials.

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



.. py:class:: GlueCatalogPartitionTrigger(database_name, table_name, expression = '', aws_conn_id = 'aws_default', region_name = None, waiter_delay = 60)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Asynchronously waits for a partition to show up in AWS Glue Catalog.

   :param database_name: The name of the catalog database where the partitions reside.
   :param table_name: The name of the table to wait for, supports the dot
       notation (my_database.my_table)
   :param expression: The partition clause to wait for. This is passed as
       is to the AWS Glue Catalog API's get_partitions function,
       and supports SQL like notation as in ``ds='2015-01-01'
       AND type='value'`` and comparison operators as in ``"ds>=2015-01-01"``.
       See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html
       #aws-glue-api-catalog-partitions-GetPartitions
   :param aws_conn_id: ID of the Airflow connection where
       credentials and extra configuration are stored
   :param region_name: Optional aws region name (example: us-east-1). Uses region from connection
       if not specified.
   :param waiter_delay: Number of seconds to wait between two checks. Default is 60 seconds.

   .. py:method:: serialize()

      Return the information needed to reconstruct this Trigger.

      :return: Tuple of (class path, keyword arguments needed to re-instantiate).


   .. py:method:: hook()


   .. py:method:: poke(client)
      :async:


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
