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

:py:mod:`tests.system.providers.amazon.aws.example_emr_eks`
===========================================================

.. py:module:: tests.system.providers.amazon.aws.example_emr_eks


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_emr_eks.create_launch_template
   tests.system.providers.amazon.aws.example_emr_eks.delete_launch_template
   tests.system.providers.amazon.aws.example_emr_eks.enable_access_emr_on_eks
   tests.system.providers.amazon.aws.example_emr_eks.create_iam_oidc_identity_provider
   tests.system.providers.amazon.aws.example_emr_eks.delete_iam_oidc_identity_provider
   tests.system.providers.amazon.aws.example_emr_eks.update_trust_policy_execution_role
   tests.system.providers.amazon.aws.example_emr_eks.delete_virtual_cluster



Attributes
~~~~~~~~~~

.. autoapisummary::

   tests.system.providers.amazon.aws.example_emr_eks.DAG_ID
   tests.system.providers.amazon.aws.example_emr_eks.ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_emr_eks.JOB_ROLE_ARN_KEY
   tests.system.providers.amazon.aws.example_emr_eks.JOB_ROLE_NAME_KEY
   tests.system.providers.amazon.aws.example_emr_eks.SUBNETS_KEY
   tests.system.providers.amazon.aws.example_emr_eks.sys_test_context_task
   tests.system.providers.amazon.aws.example_emr_eks.S3_FILE_NAME
   tests.system.providers.amazon.aws.example_emr_eks.S3_FILE_CONTENT
   tests.system.providers.amazon.aws.example_emr_eks.test_context
   tests.system.providers.amazon.aws.example_emr_eks.test_run


.. py:data:: DAG_ID
   :value: 'example_emr_eks'



.. py:data:: ROLE_ARN_KEY
   :value: 'ROLE_ARN'



.. py:data:: JOB_ROLE_ARN_KEY
   :value: 'JOB_ROLE_ARN'



.. py:data:: JOB_ROLE_NAME_KEY
   :value: 'JOB_ROLE_NAME'



.. py:data:: SUBNETS_KEY
   :value: 'SUBNETS'



.. py:data:: sys_test_context_task



.. py:data:: S3_FILE_NAME
   :value: 'pi.py'



.. py:data:: S3_FILE_CONTENT
   :value: Multiline-String

    .. raw:: html

        <details><summary>Show Value</summary>

    .. code-block:: python

        """
        k = 1
        s = 0

        for i in range(1000000):
            if i % 2 == 0:
                s += 4/k
            else:
                s -= 4/k

            k += 2

        print(s)
        """

    .. raw:: html

        </details>



.. py:function:: create_launch_template(template_name)


.. py:function:: delete_launch_template(template_name)


.. py:function:: enable_access_emr_on_eks(cluster, ns)


.. py:function:: create_iam_oidc_identity_provider(cluster_name)


.. py:function:: delete_iam_oidc_identity_provider(cluster_name)


.. py:function:: update_trust_policy_execution_role(cluster_name, cluster_namespace, role_name)


.. py:function:: delete_virtual_cluster(virtual_cluster_id)


.. py:data:: test_context



.. py:data:: test_run
