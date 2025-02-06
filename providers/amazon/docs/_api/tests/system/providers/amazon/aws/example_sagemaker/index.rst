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

:py:mod:`tests.system.providers.amazon.aws.example_sagemaker`
=============================================================

.. py:module:: tests.system.providers.amazon.aws.example_sagemaker


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_sagemaker.generate_data
   tests.system.providers.amazon.aws.example_sagemaker.set_up
   tests.system.providers.amazon.aws.example_sagemaker.delete_ecr_repository
   tests.system.providers.amazon.aws.example_sagemaker.delete_model_group
   tests.system.providers.amazon.aws.example_sagemaker.delete_experiment
   tests.system.providers.amazon.aws.example_sagemaker.delete_docker_image



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_sagemaker.DAG_ID
   tests.system.providers.amazon.aws.example_sagemaker.ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_sagemaker.sys_test_context_task
   tests.system.providers.amazon.aws.example_sagemaker.KNN_IMAGES_BY_REGION
   tests.system.providers.amazon.aws.example_sagemaker.SAMPLE_SIZE
   tests.system.providers.amazon.aws.example_sagemaker.PREPROCESS_SCRIPT_TEMPLATE
   tests.system.providers.amazon.aws.example_sagemaker.test_context
   tests.system.providers.amazon.aws.example_sagemaker.test_run


.. py:data:: DAG_ID
   :value: 'example_sagemaker'



.. py:data:: ROLE_ARN_KEY
   :value: 'ROLE_ARN'



.. py:data:: sys_test_context_task



.. py:data:: KNN_IMAGES_BY_REGION



.. py:data:: SAMPLE_SIZE
   :value: 600



.. py:data:: PREPROCESS_SCRIPT_TEMPLATE
   :value: Multiline-String

    .. raw:: html

        <details><summary>Show Value</summary>

    .. code-block:: python

        """
        import boto3
        import numpy as np
        import pandas as pd

        def main():
            # Load the dataset from {input_path}/input.csv, split it into train/test
            # subsets, and write them to {output_path}/ for the Processing Operator.

            data = pd.read_csv('{input_path}/input.csv')

            # Split into test and train data
            data_train, data_test = np.split(
                data.sample(frac=1, random_state=np.random.RandomState()), [int(0.7 * len(data))]
            )

            # Remove the "answers" from the test set
            data_test.drop(['class'], axis=1, inplace=True)

            # Write the splits to disk
            data_train.to_csv('{output_path}/train.csv', index=False, header=False)
            data_test.to_csv('{output_path}/test.csv', index=False, header=False)

            print('Preprocessing Done.')

        if __name__ == "__main__":
            main()
        """

    .. raw:: html

        </details>



.. py:function:: generate_data()

   generates a very simple csv dataset with headers


.. py:function:: set_up(env_id, role_arn)


.. py:function:: delete_ecr_repository(repository_name)


.. py:function:: delete_model_group(group_name, model_version_arn)


.. py:function:: delete_experiment(name)


.. py:function:: delete_docker_image(image_name)


.. py:data:: test_context



.. py:data:: test_run
