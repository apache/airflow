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

:py:mod:`airflow.providers.amazon.aws.utils.redshift`
=====================================================

.. py:module:: airflow.providers.amazon.aws.utils.redshift


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.redshift.build_credentials_block



Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.redshift.log


.. py:data:: log



.. py:function:: build_credentials_block(credentials)

   Generate AWS credentials block for Redshift COPY and UNLOAD commands.

   See AWS docs for details:
   https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html#copy-credentials

   :param credentials: ReadOnlyCredentials object from `botocore`
