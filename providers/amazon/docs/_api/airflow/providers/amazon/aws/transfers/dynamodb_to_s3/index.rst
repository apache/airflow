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

:py:mod:`airflow.providers.amazon.aws.transfers.dynamodb_to_s3`
===============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.dynamodb_to_s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.dynamodb_to_s3.JSONEncoder
   airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBToS3Operator




.. py:class:: JSONEncoder(*, skipkeys=False, ensure_ascii=True, check_circular=True, allow_nan=True, sort_keys=False, indent=None, separators=None, default=None)


   Bases: :py:obj:`json.JSONEncoder`

   Custom json encoder implementation.

   .. py:method:: default(obj)

      Convert decimal objects in a json serializable format.



.. py:class:: DynamoDBToS3Operator(*, dynamodb_table_name, s3_bucket_name, file_size, dynamodb_scan_kwargs = None, s3_key_prefix = '', process_func = _convert_item_to_json_bytes, export_time = None, export_format = 'DYNAMODB_JSON', **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.transfers.base.AwsToAwsBaseOperator`

   Replicates records from a DynamoDB table to S3.

   It scans a DynamoDB table and writes the received records to a file
   on the local filesystem. It flushes the file to S3 once the file size
   exceeds the file size limit specified by the user.

   Users can also specify a filtering criteria using dynamodb_scan_kwargs
   to only replicate records that satisfy the criteria.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/transfer:DynamoDBToS3Operator`

   :param dynamodb_table_name: Dynamodb table to replicate data from
   :param s3_bucket_name: S3 bucket to replicate data to
   :param file_size: Flush file to s3 if file size >= file_size
   :param dynamodb_scan_kwargs: kwargs pass to
       <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan>
   :param s3_key_prefix: Prefix of s3 object key
   :param process_func: How we transform a dynamodb item to bytes. By default, we dump the json
   :param export_time: Time in the past from which to export table data, counted in seconds from the start of
    the Unix epoch. The table export will be a snapshot of the table's state at this point in time.
   :param export_format: The format for the exported data. Valid values for ExportFormat are DYNAMODB_JSON
    or ION.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: template_fields_renderers



   .. py:method:: hook()

      Create DynamoDBHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
