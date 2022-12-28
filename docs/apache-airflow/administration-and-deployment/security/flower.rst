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

Flower
======

Flower is a web based tool for monitoring and administrating Celery clusters. This topic describes how
to configure Airflow to secure your flower instance.

This is an optional component that is disabled by default in Community deployments and you need to
configure it on your own if you want to use it.

Flower Authentication
---------------------

Basic authentication for Celery Flower is supported.

You can specify the details either as an optional argument in the Flower process launching
command, or as a configuration item in your ``airflow.cfg``. For both cases, please provide
``user:password`` pairs separated by a comma.

.. code-block:: bash

    airflow celery flower --basic-auth=user1:password1,user2:password2

.. code-block:: ini

    [celery]
    flower_basic_auth = user1:password1,user2:password2

Flower URL Prefix
-----------------

Enables deploying Celery Flower on non-root URL

For example to access Flower on http://example.com/flower run it with:

.. code-block:: bash

  airflow celery flower --url-prefix=flower

.. code-block:: ini

  [celery]
  flower_url_prefix = flower


NOTE: The old nginx rewrite is no longer needed
