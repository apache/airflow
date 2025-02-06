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

:py:mod:`tests.system.providers.amazon.aws.example_glue`
========================================================

.. py:module:: tests.system.providers.amazon.aws.example_glue


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_glue.get_role_name
   tests.system.providers.amazon.aws.example_glue.glue_cleanup



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_glue.DAG_ID
   tests.system.providers.amazon.aws.example_glue.ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_glue.sys_test_context_task
   tests.system.providers.amazon.aws.example_glue.EXAMPLE_CSV
   tests.system.providers.amazon.aws.example_glue.EXAMPLE_SCRIPT
   tests.system.providers.amazon.aws.example_glue.test_context
   tests.system.providers.amazon.aws.example_glue.test_run


.. py:data:: DAG_ID
   :value: 'example_glue'



.. py:data:: ROLE_ARN_KEY
   :value: 'ROLE_ARN'



.. py:data:: sys_test_context_task



.. py:data:: EXAMPLE_CSV
   :value: Multiline-String

    .. raw:: html

        <details><summary>Show Value</summary>

    .. code-block:: python

        """
        apple,0.5
        milk,2.5
        bread,4.0
        """

    .. raw:: html

        </details>



.. py:data:: EXAMPLE_SCRIPT
   :value: Multiline-String

    .. raw:: html

        <details><summary>Show Value</summary>

    .. code-block:: python

        """
        from pyspark.context import SparkContext
        from awsglue.context import GlueContext

        glueContext = GlueContext(SparkContext.getOrCreate())
        datasource = glueContext.create_dynamic_frame.from_catalog(
                     database='{db_name}', table_name='input')
        print('There are %s items in the table' % datasource.count())

        datasource.toDF().write.format('csv').mode("append").save('s3://{bucket_name}/output')
        """

    .. raw:: html

        </details>



.. py:function:: get_role_name(arn)


.. py:function:: glue_cleanup(crawler_name, job_name, db_name)


.. py:data:: test_context



.. py:data:: test_run
