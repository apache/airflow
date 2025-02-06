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

:py:mod:`airflow.providers.amazon.aws.transfers.salesforce_to_s3`
=================================================================

.. py:module:: airflow.providers.amazon.aws.transfers.salesforce_to_s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.salesforce_to_s3.SalesforceToS3Operator




.. py:class:: SalesforceToS3Operator(*, salesforce_query, s3_bucket_name, s3_key, salesforce_conn_id, export_format = 'csv', query_params = None, include_deleted = False, coerce_to_timestamp = False, record_time_added = False, aws_conn_id = 'aws_default', replace = False, encrypt = False, gzip = False, acl_policy = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Submits a Salesforce query and uploads the results to AWS S3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SalesforceToS3Operator`

   :param salesforce_query: The query to send to Salesforce.
   :param s3_bucket_name: The bucket name to upload to.
   :param s3_key: The object name to set when uploading the file.
   :param salesforce_conn_id: The name of the connection that has the parameters needed
       to connect to Salesforce.
   :param export_format: Desired format of files to be exported.
   :param query_params: Additional optional arguments to be passed to the HTTP request querying Salesforce.
   :param include_deleted: True if the query should include deleted records.
   :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
       False if you want them to be left in the same format as they were in Salesforce.
       Leaving the value as False will result in datetimes being strings. Default: False
   :param record_time_added: True if you want to add a Unix timestamp field
       to the resulting data that marks when the data was fetched from Salesforce. Default: False
   :param aws_conn_id: The name of the connection that has the parameters we need to connect to S3.
   :param replace: A flag to decide whether or not to overwrite the S3 key if it already exists. If set to
       False and the key exists an error will be raised.
   :param encrypt: If True, the file will be encrypted on the server-side by S3 and will
       be stored in an encrypted form while at rest in S3.
   :param gzip: If True, the file will be compressed locally.
   :param acl_policy: String specifying the canned ACL policy for the file being uploaded
       to the S3 bucket.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('salesforce_query', 's3_bucket_name', 's3_key')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ('.sql',)



   .. py:attribute:: template_fields_renderers



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
