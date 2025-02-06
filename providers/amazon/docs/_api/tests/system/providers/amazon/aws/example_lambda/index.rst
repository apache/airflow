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

:py:mod:`tests.system.providers.amazon.aws.example_lambda`
==========================================================

.. py:module:: tests.system.providers.amazon.aws.example_lambda


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_lambda.create_zip
   tests.system.providers.amazon.aws.example_lambda.delete_lambda



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_lambda.DAG_ID
   tests.system.providers.amazon.aws.example_lambda.ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_lambda.sys_test_context_task
   tests.system.providers.amazon.aws.example_lambda.CODE_CONTENT
   tests.system.providers.amazon.aws.example_lambda.test_context
   tests.system.providers.amazon.aws.example_lambda.test_run


.. py:data:: DAG_ID
   :value: 'example_lambda'



.. py:data:: ROLE_ARN_KEY
   :value: 'ROLE_ARN'



.. py:data:: sys_test_context_task



.. py:data:: CODE_CONTENT
   :value: Multiline-String

    .. raw:: html

        <details><summary>Show Value</summary>

    .. code-block:: python

        """
        def test(*args):
            print('Hello')
        """

    .. raw:: html

        </details>



.. py:function:: create_zip(content)


.. py:function:: delete_lambda(function_name)


.. py:data:: test_context



.. py:data:: test_run
