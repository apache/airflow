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

====================================
Set AWS auth manager as auth manager
====================================

In order to use the AWS auth manager as auth manager in your Airflow environment, you need to set it in your Airflow configuration.

.. note::
   Configuration options must be consistent across all the hosts/environments running the Airflow components (Scheduler, Webserver, ECS Task containers, etc). See `here <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html>`__ for more details on setting configurations.

.. code-block:: ini

    [core]
    auth_manager = airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager

or

.. code-block:: bash

   export AIRFLOW__CORE__AUTH_MANAGER='airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager'
