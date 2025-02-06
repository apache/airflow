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

:py:mod:`airflow.providers.amazon.aws.transfers.glacier_to_gcs`
===============================================================

.. py:module:: airflow.providers.amazon.aws.transfers.glacier_to_gcs


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.transfers.glacier_to_gcs.GlacierToGCSOperator




.. py:class:: GlacierToGCSOperator(*, aws_conn_id = 'aws_default', gcp_conn_id = 'google_cloud_default', vault_name, bucket_name, object_name, gzip, chunk_size = 1024, google_impersonation_chain = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Transfers data from Amazon Glacier to Google Cloud Storage.

   .. note::
       Please be warn that GlacierToGCSOperator may depends on memory usage.
       Transferring big files may not working well.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GlacierToGCSOperator`

   :param aws_conn_id: The reference to the AWS connection details
   :param gcp_conn_id: The reference to the GCP connection details
   :param vault_name: the Glacier vault on which job is executed
   :param bucket_name: the Google Cloud Storage bucket where the data will be transferred
   :param object_name: the name of the object to check in the Google cloud
       storage bucket.
   :param gzip: option to compress local file or file data for upload
   :param chunk_size: size of chunk in bytes the that will be downloaded from Glacier vault
   :param google_impersonation_chain: Optional Google service account to impersonate using
       short-term credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('vault_name', 'bucket_name', 'object_name')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
