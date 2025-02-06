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

:py:mod:`airflow.providers.amazon.aws.transfers.gcs_to_s3`
==========================================================

.. py:module:: airflow.providers.amazon.aws.transfers.gcs_to_s3

.. autoapi-nested-parse::

   This module contains Google Cloud Storage to S3 operator.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSToS3Operator




.. py:class:: GCSToS3Operator(*, gcs_bucket = None, bucket = None, prefix = None, delimiter = None, gcp_conn_id = 'google_cloud_default', dest_aws_conn_id = 'aws_default', dest_s3_key, dest_verify = None, replace = False, google_impersonation_chain = None, dest_s3_extra_args = None, s3_acl_policy = None, keep_directory_structure = True, match_glob = None, gcp_user_project = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Synchronizes a Google Cloud Storage bucket with an S3 bucket.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GCSToS3Operator`

   :param gcs_bucket: The Google Cloud Storage bucket to find the objects. (templated)
   :param bucket: (Deprecated) Use ``gcs_bucket`` instead.
   :param prefix: Prefix string which filters objects whose name begin with
       this prefix. (templated)
   :param delimiter: (Deprecated) The delimiter by which you want to filter the objects. (templated)
       For e.g to lists the CSV files from in a directory in GCS you would use
       delimiter='.csv'.
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :param dest_aws_conn_id: The destination S3 connection
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

   :param replace: Whether or not to verify the existence of the files in the
       destination bucket.
       By default is set to False
       If set to True, will upload all the files replacing the existing ones in
       the destination bucket.
       If set to False, will upload only the files that are in the origin but not
       in the destination bucket.
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :param s3_acl_policy: Optional The string to specify the canned ACL policy for the
       object to be uploaded in S3
   :param keep_directory_structure: (Optional) When set to False the path of the file
        on the bucket is recreated within path passed in dest_s3_key.
   :param match_glob: (Optional) filters objects based on the glob pattern given by the string
       (e.g, ``'**/*/.json'``)
   :param gcp_user_project: (Optional) The identifier of the Google Cloud project to bill for this request.
       Required for Requester Pays buckets.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('gcs_bucket', 'prefix', 'delimiter', 'dest_s3_key', 'google_impersonation_chain', 'gcp_user_project')



   .. py:attribute:: ui_color
      :value: '#f0eee4'



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
