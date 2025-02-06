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

:py:mod:`airflow.providers.amazon.aws.log.cloudwatch_task_handler`
==================================================================

.. py:module:: airflow.providers.amazon.aws.log.cloudwatch_task_handler


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.log.cloudwatch_task_handler.CloudwatchTaskHandler



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.log.cloudwatch_task_handler.json_serialize_legacy
   airflow.providers.amazon.aws.log.cloudwatch_task_handler.json_serialize



.. py:function:: json_serialize_legacy(value)

   JSON serializer replicating legacy watchtower behavior.

   The legacy `watchtower@2.0.1` json serializer function that serialized
   datetime objects as ISO format and all other non-JSON-serializable to `null`.

   :param value: the object to serialize
   :return: string representation of `value` if it is an instance of datetime or `None` otherwise


.. py:function:: json_serialize(value)

   JSON serializer replicating current watchtower behavior.

   This provides customers with an accessible import,
   `airflow.providers.amazon.aws.log.cloudwatch_task_handler.json_serialize`

   :param value: the object to serialize
   :return: string representation of `value`


.. py:class:: CloudwatchTaskHandler(base_log_folder, log_group_arn, filename_template = None)


   Bases: :py:obj:`airflow.utils.log.file_task_handler.FileTaskHandler`, :py:obj:`airflow.utils.log.logging_mixin.LoggingMixin`

   CloudwatchTaskHandler is a python log handler that handles and reads task instance logs.

   It extends airflow FileTaskHandler and uploads to and reads from Cloudwatch.

   :param base_log_folder: base folder to store logs locally
   :param log_group_arn: ARN of the Cloudwatch log group for remote log storage
       with format ``arn:aws:logs:{region name}:{account id}:log-group:{group name}``
   :param filename_template: template for file name (local storage) or log stream name (remote)

   .. py:attribute:: trigger_should_wrap
      :value: True



   .. py:method:: hook()

      Returns AwsLogsHook.


   .. py:method:: set_context(ti)

      Provide task_instance context to airflow task handler.

      Generally speaking returns None.  But if attr `maintain_propagate` has
      been set to propagate, then returns sentinel MAINTAIN_PROPAGATE. This
      has the effect of overriding the default behavior to set `propagate`
      to False whenever set_context is called.  At time of writing, this
      functionality is only used in unit testing.

      :param ti: task instance object


   .. py:method:: close()

      Close the handler responsible for the upload of the local log file to Cloudwatch.


   .. py:method:: get_cloudwatch_logs(stream_name, task_instance)

      Return all logs from the given log stream.

      :param stream_name: name of the Cloudwatch log stream to get all logs from
      :param task_instance: the task instance to get logs about
      :return: string of all logs from the given log stream
