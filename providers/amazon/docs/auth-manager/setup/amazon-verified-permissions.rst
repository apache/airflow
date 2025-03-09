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

=====================================
Configure Amazon Verified Permissions
=====================================

`Amazon Verified Permissions <https://aws.amazon.com/verified-permissions/>`_ is used by the AWS auth manager to make
all user authorization decisions. All user permission policies need to be defined in Amazon Verified Permissions by
the Airflow environment admin.

Create the policy store
=======================

The AWS auth manager needs one resource in AWS IAM Identity Center: a policy store. You can create it either through
the vended CLI command or manually.

With CLI
--------

.. note::
  In order to create all resources needed by the AWS auth manager, you can use the CLI command vended as part of the AWS auth manager.
  In order to use it, you first need to set the AWS auth manager as auth manager in Airflow config.
  See :doc:`how to set AWS auth manager as auth manager </auth-manager/setup/config>`.

To create the policy store, please run the following command:

.. code-block:: bash

   airflow aws-auth-manager init-avp

The CLI command should exit successfully. If the error message below is raised, it means you already have created a
policy store for Airflow. In that case you might need to :ref:`update its schema manually <avp_update_schema>`. ::

  Since an existing policy store with description ... has been found in Amazon Verified Permissions,
  the CLI made no changes to this policy store for security reasons.
  Any modification to this policy store must be done manually.

.. _avp_create_policy_store_manually:

Manually
--------

Please follow the instructions below to create the Amazon Verified Permissions policy store.

1. Open the `Amazon Verified Permissions console <https://console.aws.amazon.com/verifiedpermissions>`_.
2. Choose **Create policy store**.
3. In the **Configuration method** section, choose **Empty policy store**.
4. In the **Details** section, type ``Airflow`` as description.
5. Choose **Create policy store**. The policy store is now created.

You now need to define the schema of the policy store you just created.

.. _avp_update_schema:

Update the policy store schema
==============================

.. note::

  You only need to update the policy store schema in some special cases. If your situation matches one of the case
  below, you should update it, if not, you can skip this part.

  * You :ref:`created the policy store manually <avp_create_policy_store_manually>` and no schema is yet defined in
    the policy store.
  * You have an existing policy store used for Airflow and you made some modifications to its schema you want to revert.
  * You have an existing policy store used for Airflow and you want to update its schema to the latest version.
    This is only needed if your policy store schema and `the latest schema version <https://github.com/apache/airflow/blob/main/providers/amazon/aws/src/airflow/providers/amazon/aws/auth_manager/avp/schema.json>`_
    are different. If so, there should be a warning message when Airflow is starting.

With CLI
--------

To update the policy store schema to its latest version, please run the following command:

.. code-block:: bash

   airflow aws-auth-manager update-avp-schema

Manually
--------

Please follow the instructions below to update the Amazon Verified Permissions policy store schema to its latest version.

1. Open the `Amazon Verified Permissions console <https://console.aws.amazon.com/verifiedpermissions>`_.
2. Choose the policy store used by Airflow (by default its description is ``Airflow``).
3. In the navigation pane on the left, choose **Schema**.
4. Choose **Edit schema** and then choose **JSON mode**.
5. Enter the content of `the latest schema version <https://github.com/apache/airflow/blob/main/providers/amazon/aws/src/airflow/providers/amazon/aws/auth_manager/avp/schema.json>`_
   in the **Contents** field.
6. Choose **Save changes**.

Configure Airflow
=================

You need to set in Airflow configuration the Amazon Verified Permissions policy store ID file created previously.

.. code-block:: ini

    [aws_auth_manager]
    avp_policy_store_id = <avp_policy_store_id>

or

.. code-block:: bash

   export AIRFLOW__AWS_AUTH_MANAGER__AVP_POLICY_STORE_ID='<avp_policy_store_id>'

The AWS auth manager is now configured and ready to be used. See :doc:`/auth-manager/manage/index` to learn how to
manage users and permissions through AWS IAM Identity Center and Amazon Verified Permissions.
