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

.. _AWS:

AWS: Amazon Web Services
------------------------

Airflow has support for `Amazon Web Services <https://aws.amazon.com/>`__.

All hooks are based on :mod:`airflow.providers.amazon.aws.hooks.base_aws`.

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within the Amazon Web Services.

.. operators-hooks-ref::
   :tags: aws
   :header-separator: "

Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data from/to Amazon Web Services.

.. transfers-ref::
   :tags: aws
   :header-separator: "
