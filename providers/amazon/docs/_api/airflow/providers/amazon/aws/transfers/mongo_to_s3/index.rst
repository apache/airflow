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

:py:mod:`airflow.providers.amazon.aws.transfers.mongo_to_s3`
============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.mongo_to_s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.mongo_to_s3.MongoToS3Operator




.. py:class:: MongoToS3Operator(*, mongo_conn_id = 'mongo_default', aws_conn_id = 'aws_default', mongo_collection, mongo_query, s3_bucket, s3_key, mongo_db = None, mongo_projection = None, replace = False, allow_disk_use = False, compression = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Move data from MongoDB to S3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:MongoToS3Operator`

   :param mongo_conn_id: reference to a specific mongo connection
   :param aws_conn_id: reference to a specific S3 connection
   :param mongo_collection: reference to a specific collection in your mongo db
   :param mongo_query: query to execute. A list including a dict of the query
   :param mongo_projection: optional parameter to filter the returned fields by
       the query. It can be a list of fields names to include or a dictionary
       for excluding fields (e.g ``projection={"_id": 0}`` )
   :param s3_bucket: reference to a specific S3 bucket to store the data
   :param s3_key: in which S3 key the file will be stored
   :param mongo_db: reference to a specific mongo database
   :param replace: whether or not to replace the file in S3 if it previously existed
   :param allow_disk_use: enables writing to temporary files in the case you are handling large dataset.
       This only takes effect when `mongo_query` is a list - running an aggregate pipeline
   :param compression: type of compression to use for output file in S3. Currently only gzip is supported.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('s3_bucket', 's3_key', 'mongo_query', 'mongo_collection')



   .. py:attribute:: ui_color
      :value: '#589636'



   .. py:attribute:: template_fields_renderers



   .. py:method:: execute(context)

      Is written to depend on transform method.


   .. py:method:: transform(docs)
      :staticmethod:

      Transform the data for transfer.

      This method is meant to be extended by child classes to perform
      transformations unique to those operators needs. Processes pyMongo
      cursor and returns an iterable with each element being a JSON
      serializable dictionary

      The default implementation assumes no processing is needed, i.e. input
      is a pyMongo cursor of documents and just needs to be passed through.

      Override this method for custom transformations.
