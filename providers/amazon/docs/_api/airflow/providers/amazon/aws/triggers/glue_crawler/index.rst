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

:py:mod:`airflow.providers.amazon.aws.triggers.glue_crawler`
============================================================

.. py:module:: airflow.providers.amazon.aws.triggers.glue_crawler


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.glue_crawler.GlueCrawlerCompleteTrigger




.. py:class:: GlueCrawlerCompleteTrigger(crawler_name, poll_interval = None, aws_conn_id = 'aws_default', waiter_delay = 5, waiter_max_attempts = 1500)


   Bases: :py:obj:`airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger`

   Watches for a glue crawl, triggers when it finishes.

   :param crawler_name: name of the crawler to watch
   :param poll_interval: The amount of time in seconds to wait between attempts.
   :param aws_conn_id: The Airflow connection used for AWS credentials.

   .. py:method:: hook()

      Override in subclasses to return the right hook.
