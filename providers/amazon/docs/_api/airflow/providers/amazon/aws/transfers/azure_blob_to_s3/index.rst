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

:py:mod:`airflow.providers.amazon.aws.transfers.azure_blob_to_s3`
=================================================================

.. py:module:: airflow.providers.amazon.aws.transfers.azure_blob_to_s3


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.azure_blob_to_s3.AzureBlobStorageToS3Operator




.. py:class:: AzureBlobStorageToS3Operator(*, wasb_conn_id = 'wasb_default', container_name, prefix = None, delimiter = '', aws_conn_id = 'aws_default', dest_s3_key, dest_verify = None, dest_s3_extra_args = None, replace = False, s3_acl_policy = None, wasb_extra_args = {}, s3_extra_args = {}, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Operator transfers data from Azure Blob Storage to specified bucket in Amazon S3.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:AzureBlobStorageToGCSOperator`

   :param wasb_conn_id: Reference to the wasb connection.
   :param container_name: Name of the container
   :param prefix: Prefix string which filters objects whose name begin with
       this prefix. (templated)
   :param delimiter: The delimiter by which you want to filter the objects. (templated)
       For e.g to lists the CSV files from in a directory in GCS you would use
       delimiter='.csv'.
   :param aws_conn_id: Connection id of the S3 connection to use
   :param dest_s3_key: The base S3 key to be used to store the files. (templated)
   :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
       By default SSL certificates are verified.
       You can provide the following values:

       - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be
                verified.
       - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
   :param dest_s3_extra_args: Extra arguments that may be passed to the download/upload operations.
   :param replace: Whether or not to verify the existence of the files in the
       destination bucket.
       By default is set to False
       If set to True, will upload all the files replacing the existing ones in
       the destination bucket.
       If set to False, will upload only the files that are in the origin but not
       in the destination bucket.
   :param s3_acl_policy: Optional The string to specify the canned ACL policy for the
       object to be uploaded in S3
   :param wasb_extra_kargs: kwargs to pass to WasbHook
   :param s3_extra_kargs: kwargs to pass to S3Hook

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('container_name', 'prefix', 'delimiter', 'dest_s3_key')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
