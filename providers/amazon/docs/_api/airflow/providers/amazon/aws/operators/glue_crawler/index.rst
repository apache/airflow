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

:py:mod:`airflow.providers.amazon.aws.operators.glue_crawler`
=============================================================

.. py:module:: airflow.providers.amazon.aws.operators.glue_crawler


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerOperator




.. py:class:: GlueCrawlerOperator(config, aws_conn_id='aws_default', region_name = None, poll_interval = 5, wait_for_completion = True, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates, updates and triggers an AWS Glue Crawler.

   AWS Glue Crawler is a serverless service that manages a catalog of
   metadata tables that contain the inferred schema, format and data
   types of data stores within the AWS cloud.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GlueCrawlerOperator`

   :param config: Configurations for the AWS Glue crawler
   :param aws_conn_id: aws connection to use
   :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
   :param wait_for_completion: Whether to wait for crawl execution completion. (default: True)
   :param deferrable: If True, the operator will wait asynchronously for the crawl to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('config',)



   .. py:attribute:: ui_color
      :value: '#ededed'



   .. py:method:: hook()

      Create and return a GlueCrawlerHook.


   .. py:method:: execute(context)

      Execute AWS Glue Crawler from Airflow.

      :return: the name of the current glue crawler.


   .. py:method:: execute_complete(context, event=None)
