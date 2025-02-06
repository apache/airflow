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

:py:mod:`tests.system.providers.amazon.aws.example_local_to_s3`
===============================================================

.. py:module:: tests.system.providers.amazon.aws.example_local_to_s3


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_local_to_s3.create_temp_file
   tests.system.providers.amazon.aws.example_local_to_s3.delete_temp_file



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_local_to_s3.sys_test_context_task
   tests.system.providers.amazon.aws.example_local_to_s3.DAG_ID
   tests.system.providers.amazon.aws.example_local_to_s3.TEMP_FILE_PATH
   tests.system.providers.amazon.aws.example_local_to_s3.SAMPLE_TEXT
   tests.system.providers.amazon.aws.example_local_to_s3.test_context
   tests.system.providers.amazon.aws.example_local_to_s3.test_run


.. py:data:: sys_test_context_task



.. py:data:: DAG_ID
   :value: 'example_local_to_s3'



.. py:data:: TEMP_FILE_PATH
   :value: '/tmp/sample-txt.txt'



.. py:data:: SAMPLE_TEXT
   :value: 'This is some sample text.'



.. py:function:: create_temp_file()


.. py:function:: delete_temp_file()


.. py:data:: test_context



.. py:data:: test_run
