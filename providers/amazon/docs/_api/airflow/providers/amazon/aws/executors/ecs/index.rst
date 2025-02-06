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

:py:mod:`airflow.providers.amazon.aws.executors.ecs`
====================================================

.. py:module:: airflow.providers.amazon.aws.executors.ecs

.. autoapi-nested-parse::

   AWS ECS Executor.

   Each Airflow task gets delegated out to an Amazon ECS Task.



Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   boto_schema/index.rst
   ecs_executor_config/index.rst
   utils/index.rst


Package Contents
----------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.executors.ecs.AwsEcsExecutor




.. py:class:: AwsEcsExecutor(*args, **kwargs)


   Bases: :py:obj:`airflow.executors.base_executor.BaseExecutor`

   Executes the provided Airflow command on an ECS instance.

   The Airflow Scheduler creates a shell command, and passes it to the executor. This ECS Executor
   runs said Airflow command on a remote Amazon ECS Cluster with a task-definition configured to
   launch the same containers as the Scheduler. It then periodically checks in with the launched
   tasks (via task ARNs) to determine the status.

   This allows individual tasks to specify CPU, memory, GPU, env variables, etc. When initializing a task,
   there's an option for "executor config" which should be a dictionary with keys that match the
   ``ContainerOverride`` definition per AWS documentation (see link below).

   Prerequisite: proper configuration of Boto3 library
   .. seealso:: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html for
   authentication and access-key management. You can store an environmental variable, setup aws config from
   console, or use IAM roles.

   .. seealso:: https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_ContainerOverride.html for an
    Airflow TaskInstance's executor_config.

   .. py:attribute:: MAX_RUN_TASK_ATTEMPTS



   .. py:attribute:: DESCRIBE_TASKS_BATCH_SIZE
      :value: 99



   .. py:method:: sync()

      Sync will get called periodically by the heartbeat method.

      Executors should override this to perform gather statuses.


   .. py:method:: sync_running_tasks()

      Checks and update state on all running tasks.


   .. py:method:: attempt_task_runs()

      Takes tasks from the pending_tasks queue, and attempts to find an instance to run it on.

      If the launch type is EC2, this will attempt to place tasks on empty EC2 instances.  If
          there are no EC2 instances available, no task is placed and this function will be
          called again in the next heart-beat.

      If the launch type is FARGATE, this will run the tasks on new AWS Fargate instances.


   .. py:method:: execute_async(key, command, queue=None, executor_config=None)

      Save the task to be executed in the next sync by inserting the commands into a queue.


   .. py:method:: end(heartbeat_interval=10)

      Waits for all currently running tasks to end, and doesn't launch any tasks.


   .. py:method:: terminate()

      Kill all ECS processes by calling Boto3's StopTask API.


   .. py:method:: get_container(container_list)

      Searches task list for core Airflow container.
