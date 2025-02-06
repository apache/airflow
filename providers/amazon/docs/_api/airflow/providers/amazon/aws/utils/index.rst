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

:py:mod:`airflow.providers.amazon.aws.utils`
============================================

.. py:module:: airflow.providers.amazon.aws.utils


Submodules
----------
.. toctree::
   :titlesonly:
   :maxdepth: 1

   connection_wrapper/index.rst
   eks_get_token/index.rst
   emailer/index.rst
   identifiers/index.rst
   mixins/index.rst
   rds/index.rst
   redshift/index.rst
   sagemaker/index.rst
   sqs/index.rst
   suppress/index.rst
   tags/index.rst
   task_log_fetcher/index.rst
   waiter/index.rst
   waiter_with_logging/index.rst


Package Contents
----------------


Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.trim_none_values
   airflow.providers.amazon.aws.utils.datetime_to_epoch
   airflow.providers.amazon.aws.utils.datetime_to_epoch_ms
   airflow.providers.amazon.aws.utils.datetime_to_epoch_utc_ms
   airflow.providers.amazon.aws.utils.datetime_to_epoch_us
   airflow.providers.amazon.aws.utils.get_airflow_version



Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.log


.. py:data:: log



.. py:function:: trim_none_values(obj)


.. py:function:: datetime_to_epoch(date_time)

   Convert a datetime object to an epoch integer (seconds).


.. py:function:: datetime_to_epoch_ms(date_time)

   Convert a datetime object to an epoch integer (milliseconds).


.. py:function:: datetime_to_epoch_utc_ms(date_time)

   Convert a datetime object to an epoch integer (milliseconds) in UTC timezone.


.. py:function:: datetime_to_epoch_us(date_time)

   Convert a datetime object to an epoch integer (microseconds).


.. py:function:: get_airflow_version()
