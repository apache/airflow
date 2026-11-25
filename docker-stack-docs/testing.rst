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

.. _entrypoint:test-options:

Working with the image
======================

The options below are mostly used for quick testing the image - for example with
quick-start docker-compose or when you want to perform a local test with new packages
added. They are not supposed to be run in the production environment as they add additional
overhead for execution of additional commands. Those options in production should be realized
either as maintenance operations on the database or should be embedded in the custom image used
(when you want to add new packages).

Upgrading Airflow DB
--------------------

If you set :envvar:`_AIRFLOW_DB_MIGRATE` variable to a non-empty value, the entrypoint will run
the ``airflow db migrate`` command right after verifying the connection. You can also use this
when you are running Airflow with internal SQLite database (default) to upgrade the db and create
admin users at entrypoint, so that you can start the webserver immediately. If no command is
provided to the container and :envvar:`_AIRFLOW_DB_MIGRATE` is set, the container will exit
cleanly after completing the database migration. This allows one-off init containers
(such as ``airflow-init``) to perform setup without requiring a placeholder command to suppress
CLI errors. Note - using SQLite is intended only for testing purpose, never use SQLite in
production as it has severe limitations when it comes to concurrency.

Creating admin user
-------------------

The entrypoint can also create webserver user automatically when you enter it. you need to set
:envvar:`_AIRFLOW_WWW_USER_CREATE` to a non-empty value in order to do that. This is not intended for
production, it is only useful if you would like to run a quick test with the production image.
You need to pass at least password to create such user via ``_AIRFLOW_WWW_USER_PASSWORD`` or
:envvar:`_AIRFLOW_WWW_USER_PASSWORD_CMD` similarly like for other ``*_CMD`` variables, the content of
the ``*_CMD`` will be evaluated as shell command and its output will be set as password.

User creation will fail if none of the ``PASSWORD`` variables are set - there is no default for
password for security reasons.

+-----------+--------------------------+----------------------------------------------------------------------+
| Parameter | Default                  | Environment variable                                                 |
+===========+==========================+======================================================================+
| username  | admin                    | ``_AIRFLOW_WWW_USER_USERNAME``                                       |
+-----------+--------------------------+----------------------------------------------------------------------+
| password  |                          | ``_AIRFLOW_WWW_USER_PASSWORD_CMD`` or ``_AIRFLOW_WWW_USER_PASSWORD`` |
+-----------+--------------------------+----------------------------------------------------------------------+
| firstname | Airflow                  | ``_AIRFLOW_WWW_USER_FIRSTNAME``                                      |
+-----------+--------------------------+----------------------------------------------------------------------+
| lastname  | Admin                    | ``_AIRFLOW_WWW_USER_LASTNAME``                                       |
+-----------+--------------------------+----------------------------------------------------------------------+
| email     | airflowadmin@example.com | ``_AIRFLOW_WWW_USER_EMAIL``                                          |
+-----------+--------------------------+----------------------------------------------------------------------+
| role      | Admin                    | ``_AIRFLOW_WWW_USER_ROLE``                                           |
+-----------+--------------------------+----------------------------------------------------------------------+

In case the password is specified, the user will be attempted to be created, but the entrypoint will
not fail if the attempt fails (this accounts for the case that the user is already created).

You can, for example start the webserver in the production image with initializing the internal SQLite
database and creating an ``admin/admin`` Admin user with the following command:

.. code-block:: bash

  docker run -it -p 8080:8080 \
    --env "_AIRFLOW_DB_MIGRATE=true" \
    --env "_AIRFLOW_WWW_USER_CREATE=true" \
    --env "_AIRFLOW_WWW_USER_PASSWORD=admin" \
      apache/airflow:3.5.0-python3.11 webserver


.. code-block:: bash

  docker run -it -p 8080:8080 \
    --env "_AIRFLOW_DB_MIGRATE=true" \
    --env "_AIRFLOW_WWW_USER_CREATE=true" \
    --env "_AIRFLOW_WWW_USER_PASSWORD_CMD=echo admin" \
      apache/airflow:3.5.0-python3.11 webserver

The commands above perform initialization of the SQLite database, create admin user with admin password
and Admin role. They also forward local port ``8080`` to the webserver port and finally start the webserver.

Installing additional requirements
----------------------------------

.. warning:: Installing requirements this way is a very convenient method of running Airflow, very useful for
    testing and debugging. However, do not be tricked by its convenience. You should never, ever use it in
    production environment. We have deliberately chose to make it a development/test dependency and we print
    a warning, whenever it is used. There is an inherent security-related issue with using this method in
    production. Installing the requirements this way can happen at literally any time - when your containers
    get restarted, when your machines in K8S cluster get restarted. In a K8S Cluster those events can happen
    literally any time. This opens you up to a serious vulnerability where your production environment
    might be brought down by a single dependency being removed from PyPI - or even dependency of your
    dependency. This means that you put your production service availability in hands of 3rd-party developers.
    At any time, any moment including weekends and holidays those 3rd party developers might bring your
    production Airflow instance down, without you even knowing it. This is a serious vulnerability that
    is similar to the infamous
    `leftpad <https://qz.com/646467/how-one-programmer-broke-the-internet-by-deleting-a-tiny-piece-of-code/>`_
    problem. You can fully protect against this case by building your own, immutable custom image, where the
    dependencies are baked in. You have been warned.

Installing additional requirements can be done by specifying ``_PIP_ADDITIONAL_REQUIREMENTS`` variable.
The variable should contain a list of requirements that should be installed additionally when entering
the containers. Note that this option slows down starting of Airflow as every time any container starts
it must install new packages and it opens up huge potential security vulnerability when used in production
(see below). Therefore this option should only be used for testing. When testing is finished,
you should create your custom image with dependencies baked in.

Example:

.. code-block:: bash

  docker run -it -p 8080:8080 \
    --env "_PIP_ADDITIONAL_REQUIREMENTS=lxml==4.6.3 charset-normalizer==1.4.1" \
    --env "_AIRFLOW_DB_MIGRATE=true" \
    --env "_AIRFLOW_WWW_USER_CREATE=true" \
    --env "_AIRFLOW_WWW_USER_PASSWORD_CMD=echo admin" \
      apache/airflow:3.5.0-python3.11 webserver

This method is only available starting from Docker image of Airflow 2.1.1 and above.
