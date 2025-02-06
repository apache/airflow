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

:py:mod:`tests.system.providers.amazon.aws.example_quicksight`
==============================================================

.. py:module:: tests.system.providers.amazon.aws.example_quicksight


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_quicksight.get_aws_account_id
   tests.system.providers.amazon.aws.example_quicksight.create_quicksight_data_source
   tests.system.providers.amazon.aws.example_quicksight.create_quicksight_dataset
   tests.system.providers.amazon.aws.example_quicksight.delete_quicksight_data_source
   tests.system.providers.amazon.aws.example_quicksight.delete_dataset
   tests.system.providers.amazon.aws.example_quicksight.delete_ingestion



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_quicksight.DAG_ID
   tests.system.providers.amazon.aws.example_quicksight.sys_test_context_task
   tests.system.providers.amazon.aws.example_quicksight.SAMPLE_DATA_COLUMNS
   tests.system.providers.amazon.aws.example_quicksight.SAMPLE_DATA
   tests.system.providers.amazon.aws.example_quicksight.test_context
   tests.system.providers.amazon.aws.example_quicksight.test_run


.. py:data:: DAG_ID
   :value: 'example_quicksight'



.. py:data:: sys_test_context_task



.. py:data:: SAMPLE_DATA_COLUMNS
   :value: ['Project', 'Year']



.. py:data:: SAMPLE_DATA
   :value: Multiline-String

    .. raw:: html

        <details><summary>Show Value</summary>

    .. code-block:: python

        """'Airflow','2015'
            'OpenOffice','2012'
            'Subversion','2000'
            'NiFi','2006'
        """

    .. raw:: html

        </details>



.. py:function:: get_aws_account_id()


.. py:function:: create_quicksight_data_source(aws_account_id, datasource_name, bucket, manifest_key)


.. py:function:: create_quicksight_dataset(aws_account_id, dataset_name, data_source_arn)


.. py:function:: delete_quicksight_data_source(aws_account_id, datasource_name)


.. py:function:: delete_dataset(aws_account_id, dataset_name)


.. py:function:: delete_ingestion(aws_account_id, dataset_name, ingestion_name)


.. py:data:: test_context



.. py:data:: test_run
