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

:py:mod:`airflow.providers.amazon.aws.hooks.appflow`
====================================================

.. py:module:: airflow.providers.amazon.aws.hooks.appflow


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.appflow.AppflowHook




.. py:class:: AppflowHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Appflow.

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("appflow") <Appflow.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
       - `Amazon Appflow API Reference <https://docs.aws.amazon.com/appflow/1.0/APIReference/Welcome.html>`__

   .. py:method:: conn()

      Get the underlying boto3 Appflow client (cached).


   .. py:method:: run_flow(flow_name, poll_interval = 20, wait_for_completion = True, max_attempts = 60)

      Execute an AppFlow run.

      :param flow_name: The flow name
      :param poll_interval: Time (seconds) to wait between two consecutive calls to check the run status
      :param wait_for_completion: whether to wait for the run to end to return
      :param max_attempts: the number of polls to do before timing out/returning a failure.
      :return: The run execution ID


   .. py:method:: update_flow_filter(flow_name, filter_tasks, set_trigger_ondemand = False)

      Update the flow task filter; all filters will be removed if an empty array is passed to filter_tasks.

      :param flow_name: The flow name
      :param filter_tasks: List flow tasks to be added
      :param set_trigger_ondemand: If True, set the trigger to on-demand; otherwise, keep the trigger as is
      :return: None
