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

:py:mod:`airflow.providers.amazon.aws.operators.glacier`
========================================================

.. py:module:: airflow.providers.amazon.aws.operators.glacier


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.glacier.GlacierCreateJobOperator
   airflow.providers.amazon.aws.operators.glacier.GlacierUploadArchiveOperator




.. py:class:: GlacierCreateJobOperator(*, aws_conn_id='aws_default', vault_name, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Initiate an Amazon Glacier inventory-retrieval job.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GlacierCreateJobOperator`

   :param aws_conn_id: The reference to the AWS connection details
   :param vault_name: the Glacier vault on which job is executed

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('vault_name',)



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: GlacierUploadArchiveOperator(*, vault_name, body, checksum = None, archive_description = None, account_id = None, aws_conn_id='aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   This operator add an archive to an Amazon S3 Glacier vault.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:GlacierUploadArchiveOperator`

   :param vault_name: The name of the vault
   :param body: A bytes or seekable file-like object. The data to upload.
   :param checksum: The SHA256 tree hash of the data being uploaded.
       This parameter is automatically populated if it is not provided
   :param archive_description: The description of the archive you are uploading
   :param account_id: (Optional) AWS account ID of the account that owns the vault.
       Defaults to the credentials used to sign the request
   :param aws_conn_id: The reference to the AWS connection details

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('vault_name',)



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
