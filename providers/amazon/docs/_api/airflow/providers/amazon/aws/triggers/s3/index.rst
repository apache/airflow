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

:py:mod:`airflow.providers.amazon.aws.triggers.s3`
==================================================

.. py:module:: airflow.providers.amazon.aws.triggers.s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.s3.S3KeyTrigger
   airflow.providers.amazon.aws.triggers.s3.S3KeysUnchangedTrigger




.. py:class:: S3KeyTrigger(bucket_name, bucket_key, wildcard_match = False, aws_conn_id = 'aws_default', poke_interval = 5.0, should_check_fn = False, **hook_params)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   S3KeyTrigger is fired as deferred class with params to run the task in trigger worker.

   :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
       is not provided as a full s3:// url.
   :param bucket_key:  The key being waited on. Supports full s3:// style url
       or relative path from root level. When it's specified as a full s3://
       url, please leave bucket_name as `None`.
   :param wildcard_match: whether the bucket_key should be interpreted as a
       Unix wildcard pattern
   :param aws_conn_id: reference to the s3 connection
   :param hook_params: params for hook its optional

   .. py:method:: serialize()

      Serialize S3KeyTrigger arguments and classpath.


   .. py:method:: hook()


   .. py:method:: run()
      :async:

      Make an asynchronous connection using S3HookAsync.



.. py:class:: S3KeysUnchangedTrigger(bucket_name, prefix, inactivity_period = 60 * 60, min_objects = 1, inactivity_seconds = 0, previous_objects = None, allow_delete = True, aws_conn_id = 'aws_default', last_activity_time = None, verify = None, **hook_params)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   S3KeysUnchangedTrigger is fired as deferred class with params to run the task in trigger worker.

   :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
       is not provided as a full s3:// url.
   :param prefix: The prefix being waited on. Relative path from bucket root level.
   :param inactivity_period: The total seconds of inactivity to designate
       keys unchanged. Note, this mechanism is not real time and
       this operator may not return until a poke_interval after this period
       has passed with no additional objects sensed.
   :param min_objects: The minimum number of objects needed for keys unchanged
       sensor to be considered valid.
   :param inactivity_seconds: reference to the seconds of inactivity
   :param previous_objects: The set of object ids found during the last poke.
   :param allow_delete: Should this sensor consider objects being deleted
   :param aws_conn_id: reference to the s3 connection
   :param last_activity_time: last modified or last active time
   :param verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
   :param hook_params: params for hook its optional

   .. py:method:: serialize()

      Serialize S3KeysUnchangedTrigger arguments and classpath.


   .. py:method:: hook()


   .. py:method:: run()
      :async:

      Make an asynchronous connection using S3Hook.
