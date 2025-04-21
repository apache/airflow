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

Generate JWT token with AWS auth manager
========================================

.. note::
    This guide only applies if your environment is configured with AWS auth manager.

In order to use the :doc:`Airflow public API <apache-airflow:stable-rest-api-ref>`, you need a JWT token for authentication.
You can then include this token in your Airflow public API requests.
To generate a JWT token, please follow the following steps.

1. Go to ``${ENDPOINT_URL}/auth/login/token``, you'll be redirected to AWS IAM Identity Center login page
2. Enter your AWS IAM Identity Center user credentials and submit

This process will return a token that you can use in the Airflow public API requests.
