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

:py:mod:`tests.system.providers.amazon.aws.example_athena`
==========================================================

.. py:module:: tests.system.providers.amazon.aws.example_athena


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_athena.await_bucket
   tests.system.providers.amazon.aws.example_athena.read_results_from_s3



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_athena.sys_test_context_task
   tests.system.providers.amazon.aws.example_athena.DAG_ID
   tests.system.providers.amazon.aws.example_athena.SAMPLE_DATA
   tests.system.providers.amazon.aws.example_athena.SAMPLE_FILENAME
   tests.system.providers.amazon.aws.example_athena.test_context
   tests.system.providers.amazon.aws.example_athena.test_run


.. py:data:: sys_test_context_task



.. py:data:: DAG_ID
   :value: 'example_athena'



.. py:data:: SAMPLE_DATA
   :value: Multiline-String

    .. raw:: html

        <details><summary>Show Value</summary>

    .. code-block:: python

        """"Alice",20
            "Bob",25
            "Charlie",30
            """

    .. raw:: html

        </details>



.. py:data:: SAMPLE_FILENAME
   :value: 'airflow_sample.csv'



.. py:function:: await_bucket(bucket_name)


.. py:function:: read_results_from_s3(bucket_name, query_execution_id)


.. py:data:: test_context



.. py:data:: test_run
