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

Entrypoint
==========

If you are using the default entrypoint of the production image,
there are a few actions that are automatically performed when the container starts.
In some cases, you can pass environment variables to the image to trigger some of that behaviour.

The variables that control the "execution" behaviour start with ``_AIRFLOW`` to distinguish them
from the variables used to build the image starting with ``AIRFLOW``.

.. _arbitrary-docker-user:

Allowing arbitrary user to run the container
--------------------------------------------

Airflow image is Open-Shift compatible, which means that you can start it with random user ID and the
group id ``0`` (``root``). If you want to run the image with user different than Airflow, you MUST set
GID of the user to ``0``. In case you try to use different group, the entrypoint exits with error.

OpenShift randomly assigns UID when it starts the container, but you can utilise this flexible UID
also in case of running the image manually. This might be useful for example in case you want to
mount ``dag`` and ``logs`` folders from host system on Linux, in which case the UID should be set
the same ID as your host user.

This can be achieved in various ways - you can change USER when you extend or customize the image or
you can dynamically pass the user to  ``docker run`` command, by adding ``--user`` flag in one of
those formats (See `Docker Run reference <https://docs.docker.com/engine/reference/run/#user>`_ for details):

```
[ user | user:group | uid | uid:gid | user:gid | uid:group ]
```

In case of Docker Compose environment it can be changed via ``user:`` entry in the ``docker-compose.yaml``.
See `Docker compose reference <https://docs.docker.com/compose/compose-file/compose-file-v3/#domainname-hostname-ipc-mac_address-privileged-read_only-shm_size-stdin_open-tty-user-working_dir>`_
for details. In our Quickstart Guide using Docker-Compose, the UID and GID can be passed via
``AIRFLOW_UID`` and ``AIRFLOW_GID`` variables as described in
:ref:`Initializing docker compose environment <initializing_docker_compose_environment>`.

In case ``GID`` is set to ``0``, the user can be any UID, but in case UID is different than the default
``airflow`` (UID=50000), the user will be automatically created when entering the container.

In order to accommodate a number of external libraries and projects, Airflow will automatically create
such an arbitrary user in (`/etc/passwd`) and make it's home directory point to ``/home/airflow``.
Many of 3rd-party libraries and packages require home directory of the user to be present, because they
need to write some cache information there, so such a dynamic creation of a user is necessary.

Such arbitrary user has to be able to write to certain directories that needs write access, and since
it is not advised to allow write access to "other" for security reasons, the OpenShift
guidelines introduced the concept of making all such folders have the ``0`` (``root``) group id (GID).
All the directories that need write access in the Airflow production image have GID set to 0 (and
they are writable for the group). We are following that concept and all the directories that need
write access follow that.

The GID=0 is set as default for the ``airflow`` user, so any directories it creates have GID set to 0
by default. The entrypoint sets ``umask`` to be ``0002`` - this means that any directories created by
the user have also "group write" access for group ``0`` - they will be writable by other users with
``root`` group. Also whenever any "arbitrary" user creates a folder (for example in a mounted volume), that
folder will have a "group write" access and ``GID=0``, so that execution with another, arbitrary user
will still continue to work, even if such directory is mounted by another arbitrary user later.

The ``umask`` setting however only works for runtime of the container - it is not used during building of
the image. If you would like to extend the image and add your own packages, you should remember to add
``umask 0002`` in front of your docker command - this way the directories created by any installation
that need group access will also be writable for the group. This can be done for example this way:

  .. code-block:: docker

      RUN umask 0002; \
          do_something; \
          do_otherthing;


You can read more about it in the "Support arbitrary user ids" chapter in the
`Openshift best practices <https://docs.openshift.com/container-platform/4.7/openshift_images/create-images.html#images-create-guide-openshift_create-images>`_.


Waits for Airflow DB connection
-------------------------------

The entrypoint is waiting for a connection to the database independent of the database engine. This allows us to increase
the stability of the environment.

Waiting for connection involves executing ``airflow db check`` command, which means that a ``select 1 as is_alive;`` statement
is executed. Then it loops until the the command will be successful.
It tries :envvar:`CONNECTION_CHECK_MAX_COUNT` times and sleeps :envvar:`CONNECTION_CHECK_SLEEP_TIME` between checks
To disable check, set ``CONNECTION_CHECK_MAX_COUNT=0``.

Waits for celery broker connection
----------------------------------

In case CeleryExecutor is used, and one of the ``scheduler``, ``celery``
commands are used the entrypoint will wait until the celery broker DB connection is available.

The script detects backend type depending on the URL schema and assigns default port numbers if not specified
in the URL. Then it loops until connection to the host/port specified can be established
It tries :envvar:`CONNECTION_CHECK_MAX_COUNT` times and sleeps :envvar:`CONNECTION_CHECK_SLEEP_TIME` between checks.
To disable check, set ``CONNECTION_CHECK_MAX_COUNT=0``.

Supported schemes:

* ``amqp(s)://``  (rabbitmq) - default port 5672
* ``redis://``               - default port 6379
* ``postgres://``            - default port 5432
* ``mysql://``               - default port 3306

Waiting for connection involves checking if a matching port is open. The host information is derived from the Airflow configuration.

.. _entrypoint:commands:

Executing commands
------------------

If first argument equals to "bash" - you are dropped to a bash shell or you can executes bash command
if you specify extra arguments. For example:

.. code-block:: bash

  docker run -it apache/airflow:2.1.2-python3.6 bash -c "ls -la"
  total 16
  drwxr-xr-x 4 airflow root 4096 Jun  5 18:12 .
  drwxr-xr-x 1 root    root 4096 Jun  5 18:12 ..
  drwxr-xr-x 2 airflow root 4096 Jun  5 18:12 dags
  drwxr-xr-x 2 airflow root 4096 Jun  5 18:12 logs

If first argument is equal to ``python`` - you are dropped in python shell or python commands are executed if
you pass extra parameters. For example:

.. code-block:: bash

  > docker run -it apache/airflow:2.1.2-python3.6 python -c "print('test')"
  test

If first argument equals to "airflow" - the rest of the arguments is treated as an airflow command
to execute. Example:

.. code-block:: bash

   docker run -it apache/airflow:2.1.2-python3.6 airflow webserver

If there are any other arguments - they are simply passed to the "airflow" command

.. code-block:: bash

  > docker run -it apache/airflow:2.1.2-python3.6 version
  2.1.2

Additional quick test options
-----------------------------

The options below are mostly used for quick testing the image - for example with
quick-start docker-compose or when you want to perform a local test with new packages
added. They are not supposed to be run in the production environment as they add additional
overhead for execution of additional commands. Those options in production should be realized
either as maintenance operations on the database or should be embedded in the custom image used
(when you want to add new packages).

Upgrading Airflow DB
....................

If you set :envvar:`_AIRFLOW_DB_UPGRADE` variable to a non-empty value, the entrypoint will run
the ``airflow db upgrade`` command right after verifying the connection. You can also use this
when you are running airflow with internal SQLite database (default) to upgrade the db and create
admin users at entrypoint, so that you can start the webserver immediately. Note - using SQLite is
intended only for testing purpose, never use SQLite in production as it has severe limitations when it
comes to concurrency.

Creating admin user
...................

The entrypoint can also create webserver user automatically when you enter it. you need to set
:envvar:`_AIRFLOW_WWW_USER_CREATE` to a non-empty value in order to do that. This is not intended for
production, it is only useful if you would like to run a quick test with the production image.
You need to pass at least password to create such user via ``_AIRFLOW_WWW_USER_PASSWORD`` or
:envvar:`_AIRFLOW_WWW_USER_PASSWORD_CMD` similarly like for other ``*_CMD`` variables, the content of
the ``*_CMD`` will be evaluated as shell command and it's output will be set as password.

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
    --env "_AIRFLOW_DB_UPGRADE=true" \
    --env "_AIRFLOW_WWW_USER_CREATE=true" \
    --env "_AIRFLOW_WWW_USER_PASSWORD=admin" \
      apache/airflow:main-python3.8 webserver


.. code-block:: bash

  docker run -it -p 8080:8080 \
    --env "_AIRFLOW_DB_UPGRADE=true" \
    --env "_AIRFLOW_WWW_USER_CREATE=true" \
    --env "_AIRFLOW_WWW_USER_PASSWORD_CMD=echo admin" \
      apache/airflow:main-python3.8 webserver

The commands above perform initialization of the SQLite database, create admin user with admin password
and Admin role. They also forward local port ``8080`` to the webserver port and finally start the webserver.

Installing additional requirements
..................................

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
    --env "_AIRFLOW_DB_UPGRADE=true" \
    --env "_AIRFLOW_WWW_USER_CREATE=true" \
    --env "_AIRFLOW_WWW_USER_PASSWORD_CMD=echo admin" \
      apache/airflow:master-python3.8 webserver

This method is only available starting from Docker image of Airflow 2.1.1 and above.
