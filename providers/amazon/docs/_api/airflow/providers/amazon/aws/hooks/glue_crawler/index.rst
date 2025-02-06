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

:py:mod:`airflow.providers.amazon.aws.hooks.glue_crawler`
=========================================================

.. py:module:: airflow.providers.amazon.aws.hooks.glue_crawler


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.glue_crawler.GlueCrawlerHook




.. py:class:: GlueCrawlerHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interacts with AWS Glue Crawler.

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("glue") <Glue.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
       - `AWS Glue crawlers and classifiers         <https://docs.aws.amazon.com/glue/latest/dg/components-overview.html#crawling-intro>`__

   .. py:method:: glue_client()

      :return: AWS Glue client


   .. py:method:: has_crawler(crawler_name)

      Check if the crawler already exists.

      :param crawler_name: unique crawler name per AWS account
      :return: Returns True if the crawler already exists and False if not.


   .. py:method:: get_crawler(crawler_name)

      Get crawler configurations.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.get_crawler`

      :param crawler_name: unique crawler name per AWS account
      :return: Nested dictionary of crawler configurations


   .. py:method:: update_crawler(**crawler_kwargs)

      Update crawler configurations.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.update_crawler`

      :param crawler_kwargs: Keyword args that define the configurations used for the crawler
      :return: True if crawler was updated and false otherwise


   .. py:method:: update_tags(crawler_name, crawler_tags)

      Update crawler tags.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.tag_resource`

      :param crawler_name: Name of the crawler for which to update tags
      :param crawler_tags: Dictionary of new tags. If empty, all tags will be deleted
      :return: True if tags were updated and false otherwise


   .. py:method:: create_crawler(**crawler_kwargs)

      Create an AWS Glue Crawler.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.create_crawler`

      :param crawler_kwargs: Keyword args that define the configurations used to create the crawler
      :return: Name of the crawler


   .. py:method:: start_crawler(crawler_name)

      Triggers the AWS Glue Crawler.

      .. seealso::
          - :external+boto3:py:meth:`Glue.Client.start_crawler`

      :param crawler_name: unique crawler name per AWS account
      :return: Empty dictionary


   .. py:method:: wait_for_crawler_completion(crawler_name, poll_interval = 5)

      Wait until Glue crawler completes; returns the status of the latest crawl or raises AirflowException.

      :param crawler_name: unique crawler name per AWS account
      :param poll_interval: Time (in seconds) to wait between two consecutive calls to check crawler status
      :return: Crawler's status
