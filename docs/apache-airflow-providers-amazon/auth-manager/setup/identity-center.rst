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

=================================
Configure AWS IAM Identity Center
=================================

In order to use the AWS auth manager, you first need to configure `AWS IAM Identity Center <https://aws.amazon.com/iam/identity-center/>`_.
AWS IAM Identity Center is used by the AWS auth manager for authentication purposes (login and logout).
Following configuration, the Airflow environment administrator can manage users and groups with Identity Center service.

Create resources
================

The AWS auth manager needs two resources in AWS IAM Identity Center: an instance and an application.
You can create them either through the provided CLI command or manually.

Create resources with CLI
-------------------------

.. note::
  The CLI command is not compatible with AWS accounts that are managed through AWS organizations.
  If your AWS account is managed through an AWS organization, please follow the
  :ref:`manual configuration <identity_center_manual_configuration>`.

.. note::
  To create all necessary resources for the AWS Auth Manager, you can utilize the CLI command provided as part of the
  AWS auth manager. Before executing the command, ensure the AWS auth manager is configured as the auth manager
  for the Airflow instance. See :doc:`/auth-manager/setup/config`.

To create the resources, please run the following command:

.. code-block:: bash

  airflow aws-auth-manager init-identity-center

The CLI command should exit successfully with the message: ::

  AWS IAM Identity Center resources created successfully.

If the CLI command exited with an error, please look carefully at the CLI command output to understand which resource(s)
have or have not been created successfully. The resource(s) which have not been successfully created need to be
:ref:`created manually <identity_center_manual_configuration>`.

If the error message below is raised, please create the AWS IAM Identity Center application through the console
following :ref:`these instructions <identity_center_manual_configuration_application>`: ::

  Creation of SAML applications is only supported in AWS console today. Please create the application through the console.

.. _identity_center_manual_configuration:

Create resources manually
-------------------------

Create the instance
~~~~~~~~~~~~~~~~~~~

Please follow `AWS documentation <https://docs.aws.amazon.com/singlesignon/latest/userguide/identity-center-instances.html>`_
to create the AWS IAM Identity Center instance.

.. _identity_center_manual_configuration_application:

Create the application
~~~~~~~~~~~~~~~~~~~~~~

Please follow the instructions below to create the AWS IAM Identity Center application.

1. Open the `IAM Identity Center console <https://console.aws.amazon.com/singlesignon>`_.
2. Choose **Applications**.
3. Choose the **Customer managed** tab.
4. Choose **Add application**.
5. On the **Select application type** page, under **Setup preference**, choose **I have an application I want to set up**.
6. Under **Application type**, choose **SAML 2.0**.
7. Choose **Next**.
8. On the **Configure application** page, under **Configure application**, enter a **Display name** for the application, such as ``Airflow``. Then, enter a Description.
9. Under **IAM Identity Center metadata**, copy the address of the **IAM Identity Center SAML metadata file**.

.. note::
  You will need to set this address in Airflow configuration later.

10. Under **Application metadata**, choose **Manually type your metadata values**. Then, provide the **Application ACS URL** and **Application SAML audience** as follows.

    .. important::
      Replace ``<base_url>`` by the base URL of your Airflow UI. It should be defined in ``AIRFLOW__WEBSERVER__BASE_URL``
      (e.g. ``localhost:8080`` if Airflow is running locally).

   * **Application ACS URL**: ``<base_url>/login_callback``
   * **Application SAML audience**: ``<base_url>/login_metadata``

11. Choose **Submit**. The application is now created.

Attribute mappings configuration
================================

Once the application is created, you need to configure the attribute mappings.

1. Go to the details page of the application that you just created.
2. Choose **Actions**.
3. Under **Actions**, choose **Edit attribute mappings**.
4. On the **Attribute mappings** page, you need to configure the different attribute mappings between your identity
   provider and AWS IAM Identity Center. For more information on attribute mappings, see the
   `IAM Identity Center documentation <https://docs.aws.amazon.com/singlesignon/latest/userguide/attributemappingsconcept.html>`_.
   The AWS auth manager needs two attributes: **id** and **groups**.
   If you use the default Identity Center directory as identity source, you can use the configuration below:

   * **id**

     * **User attribute in the application**: ``id``
     * **Maps to this string value or user attribute in IAM Identity Center**: ``${user:AD_GUID}``
     * **Format**: ``basic``
   * **groups**

     * **User attribute in the application**: ``groups``
     * **Maps to this string value or user attribute in IAM Identity Center**: ``${user:groups}``
     * **Format**: ``basic``

5. Once both attributes **id** and **groups** are defined, choose **Save changes**.

Configure Airflow
=================

You need to set in Airflow configuration the IAM Identity Center SAML metadata file created previously.

.. code-block:: ini

    [aws_auth_manager]
    saml_metadata_url = <saml_metadata_file_url>

or

.. code-block:: bash

   export AIRFLOW__AWS_AUTH_MANAGER__SAML_METADATA_URL='<saml_metadata_file_url>'
