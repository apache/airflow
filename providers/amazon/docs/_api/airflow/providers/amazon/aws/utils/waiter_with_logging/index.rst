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

:py:mod:`airflow.providers.amazon.aws.utils.waiter_with_logging`
================================================================

.. py:module:: airflow.providers.amazon.aws.utils.waiter_with_logging


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.waiter_with_logging.wait
   airflow.providers.amazon.aws.utils.waiter_with_logging.async_wait



.. py:function:: wait(waiter, waiter_delay, waiter_max_attempts, args, failure_message, status_message, status_args)

   Use a boto waiter to poll an AWS service for the specified state.

   Although this function uses boto waiters to poll the state of the
   service, it logs the response of the service after every attempt,
   which is not currently supported by boto waiters.

   :param waiter: The boto waiter to use.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param args: The arguments to pass to the waiter.
   :param failure_message: The message to log if a failure state is reached.
   :param status_message: The message logged when printing the status of the service.
   :param status_args: A list containing the JMESPath queries to retrieve status information from
       the waiter response.
       e.g.
       response = {"Cluster": {"state": "CREATING"}}
       status_args = ["Cluster.state"]

       response = {
       "Clusters": [{"state": "CREATING", "details": "User initiated."},]
       }
       status_args = ["Clusters[0].state", "Clusters[0].details"]


.. py:function:: async_wait(waiter, waiter_delay, waiter_max_attempts, args, failure_message, status_message, status_args)
   :async:

   Use an async boto waiter to poll an AWS service for the specified state.

   Although this function uses boto waiters to poll the state of the
   service, it logs the response of the service after every attempt,
   which is not currently supported by boto waiters.

   :param waiter: The boto waiter to use.
   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param args: The arguments to pass to the waiter.
   :param failure_message: The message to log if a failure state is reached.
   :param status_message: The message logged when printing the status of the service.
   :param status_args: A list containing the JMESPath queries to retrieve status information from
       the waiter response.
       e.g.
       response = {"Cluster": {"state": "CREATING"}}
       status_args = ["Cluster.state"]

       response = {
       "Clusters": [{"state": "CREATING", "details": "User initiated."},]
       }
       status_args = ["Clusters[0].state", "Clusters[0].details"]
