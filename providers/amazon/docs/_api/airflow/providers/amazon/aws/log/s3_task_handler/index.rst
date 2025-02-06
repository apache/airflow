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

:py:mod:`airflow.providers.amazon.aws.log.s3_task_handler`
==========================================================

.. py:module:: airflow.providers.amazon.aws.log.s3_task_handler


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.log.s3_task_handler.S3TaskHandler



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.log.s3_task_handler.get_default_delete_local_copy



.. py:function:: get_default_delete_local_copy()

   Load delete_local_logs conf if Airflow version > 2.6 and return False if not.

   TODO: delete this function when min airflow version >= 2.6


.. py:class:: S3TaskHandler(base_log_folder, s3_log_folder, filename_template = None, **kwargs)


   Bases: :py:obj:`airflow.utils.log.file_task_handler.FileTaskHandler`, :py:obj:`airflow.utils.log.logging_mixin.LoggingMixin`

   S3TaskHandler is a python log handler that handles and reads task instance logs.

   It extends airflow FileTaskHandler and uploads to and reads from S3 remote storage.

   .. py:attribute:: trigger_should_wrap
      :value: True



   .. py:method:: hook()

      Returns S3Hook.


   .. py:method:: set_context(ti)

      Provide task_instance context to airflow task handler.

      Generally speaking returns None.  But if attr `maintain_propagate` has
      been set to propagate, then returns sentinel MAINTAIN_PROPAGATE. This
      has the effect of overriding the default behavior to set `propagate`
      to False whenever set_context is called.  At time of writing, this
      functionality is only used in unit testing.

      :param ti: task instance object


   .. py:method:: close()

      Close and upload local log file to remote storage S3.


   .. py:method:: s3_log_exists(remote_log_location)

      Check if remote_log_location exists in remote storage.

      :param remote_log_location: log's location in remote storage
      :return: True if location exists else False


   .. py:method:: s3_read(remote_log_location, return_error = False)

      Return the log found at the remote_log_location or '' if no logs are found or there is an error.

      :param remote_log_location: the log's location in remote storage
      :param return_error: if True, returns a string error message if an
          error occurs. Otherwise returns '' when an error occurs.
      :return: the log found at the remote_log_location


   .. py:method:: s3_write(log, remote_log_location, append = True, max_retry = 1)

      Write the log to the remote_log_location; return `True` or fails silently and return `False`.

      :param log: the log to write to the remote_log_location
      :param remote_log_location: the log's location in remote storage
      :param append: if False, any existing log file is overwritten. If True,
          the new log is appended to any existing logs.
      :param max_retry: Maximum number of times to retry on upload failure
      :return: whether the log is successfully written to remote location or not.
