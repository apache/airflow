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

Development Environments
========================

There are two environments, available on Linux and macOS, that you can use to
develop Apache Airflow.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Local virtualenv Development Environment
----------------------------------------

All details about using and running local virtualenv environment for Airflow can be found
in `07_local_virtualenv.rst <07_local_virtualenv.rst>`__.

Benefits:

-   Packages are installed locally. No container environment is required.
-   You can benefit from local debugging within your IDE. You can follow the `Local and remote debugging in IDE <07_local_virtualenv.rst#local-and-remote-debugging-in-ide>`__
    to set up your local virtualenv and connect your IDE with the environment.
-   With the virtualenv in your IDE, you can benefit from auto completion and running tests directly from the IDE.

Limitations:

-   You have to maintain your dependencies and local environment consistent with
    other development environments that you have on your local machine.

-   You cannot run tests that require external components, such as mysql,
    postgres database, hadoop, mongo, cassandra, redis, etc.

    The tests in Airflow are a mixture of unit and integration tests and some of
    them require these components to be set up. Local virtualenv supports only
    real unit tests. Technically, to run integration tests, you can configure
    and install the dependencies on your own, but it is usually complex.
    Instead, you are recommended to use
    `Breeze development environment <../dev/breeze/doc/README.rst>`__ with all required packages
    pre-installed.

-   You need to make sure that your local environment is consistent with other
    developer environments. This often leads to a "works for me" syndrome. The
    Breeze container-based solution provides a reproducible environment that is
    consistent with other developers.

-   You are **STRONGLY** encouraged to also install and use `prek hooks <08_static_code_checks.rst#prek-hooks>`_
    for your local virtualenv development environment.
    Prek hooks can speed up your development cycle a lot.

Typically you can connect your local virtualenv environments easily with your IDE
and use it for development:

- `PyCharm/IntelliJ <quick-start-ide/contributors_quick_start_pycharm.rst>`__ quick start instructions
- `VSCode <quick-start-ide/contributors_quick_start_vscode.rst>`__ quick start instructions

Breeze Development Environment
------------------------------

All details about using and running Airflow Breeze can be found in
`Breeze <../dev/breeze/doc/README.rst>`__.

The Airflow Breeze solution is intended to ease your local development as "*It's
a Breeze to develop Airflow*".

Benefits:

-   Breeze is a complete environment that includes external components, such as
    mysql database, hadoop, mongo, cassandra, redis, etc., required by some of
    Airflow tests. Breeze provides a pre-configured Docker Compose environment
    where all these services are available and can be used by tests
    automatically.

-   Breeze environment is almost the same as used in the CI automated builds.
    So, if the tests run in your Breeze environment, they will work in the CI as well.
    See `Airflow CI design <../dev/breeze/doc/ci/README.md>`__ for details about Airflow CI.

Limitations:

-   Breeze environment takes significant space in your local Docker cache. There
    are separate environments for different Python and Airflow versions, and
    each of the images takes around 3GB in total.

-   Though Airflow Breeze setup is automated, it takes time. The Breeze
    environment uses pre-built images from DockerHub and it takes time to
    download and extract those images. Building the environment for a particular
    Python version takes less than 10 minutes.

-   Breeze environment runs in the background taking resources, such as disk space, CPU and memory.
    You can stop the environment manually after you use it
    or even use a ``bare`` environment to decrease resource usage.

.. note::

   Breeze CI images are not supposed to be used in production environments.
   They are optimized for repeatability of tests, maintainability and speed of building rather
   than production performance. For production purposes you should use DockerHub published
   `PROD images <https://hub.docker.com/r/apache/airflow/>`__ and customize/extend them as needed.

Remote development environments
-------------------------------

There are also remote development environments that you can use to develop Airflow:

- `CodeSpaces <quick-start-ide/contributors_quick_start_codespaces.rst>`_ - a browser-based development
   environment that you can use to develop Airflow in a browser. It is based on GitHub CodeSpaces and
   is available for all GitHub users (free version has number of hours/month limitations).

- `GitPod <quick-start-ide/contributors_quick_start_gitpod.rst>`_ - a browser-based development
   environment that you can use to develop Airflow in a browser. It is based on GitPod and
   is a paid service.


When to use which environment
-----------------------------

The table below summarizes differences between the environments:

+--------------------------+----------------------------------+---------------------------------------+----------------------------------------+
| **Property**             | **Local virtualenv**             | **Breeze environment**                | **Remote environments**                |
+==========================+==================================+=======================================+========================================+
| Dev machine needed       | - (-) You need a dev PC          | - (-) You need a dev PC               | (+) Works with remote setup            |
+--------------------------+----------------------------------+---------------------------------------+----------------------------------------+
| Test coverage            | - (-) unit tests only            | - (+) integration and unit tests      | (*/-) integration tests (extra config) |
+--------------------------+----------------------------------+---------------------------------------+----------------------------------------+
| Setup                    | - (+) automated with breeze cmd  | - (+) automated with breeze cmd       | (+) automated with CodeSpaces/GitPod   |
+--------------------------+----------------------------------+---------------------------------------+----------------------------------------+
| Installation difficulty  | - (-) depends on the OS setup    | - (+) works whenever Docker works     | (+) works in a modern browser/VSCode   |
+--------------------------+----------------------------------+---------------------------------------+----------------------------------------+
| Team synchronization     | - (-) difficult to achieve       | - (+) reproducible within team        | (+) reproducible within team           |
+--------------------------+----------------------------------+---------------------------------------+----------------------------------------+
| Reproducing CI failures  | - (-) not possible in many cases | - (+) fully reproducible              | (+) reproduce CI failures              |
+--------------------------+----------------------------------+---------------------------------------+----------------------------------------+
| Ability to update        | - (-) requires manual updates    | - (+) automated update via breeze cmd | (+/-) can be rebuild on demand         |
+--------------------------+----------------------------------+---------------------------------------+----------------------------------------+
| Disk space and CPU usage | - (+) relatively lightweight     | - (-) uses GBs of disk and many CPUs  | (-) integration tests (extra config)   |
+--------------------------+----------------------------------+---------------------------------------+----------------------------------------+
| IDE integration          | - (+) straightforward            | - (-) via remote debugging only       | (-) integration tests (extra config)   |
+--------------------------+----------------------------------+---------------------------------------+----------------------------------------+

Typically, you are recommended to use multiple of these environments depending on your needs.


-----------

If you want to learn more details about setting up your local virtualenv, follow to the
`Local virtualenv <07_local_virtualenv.rst>`__ document.

For detailed information about debugging Airflow components using Breeze, see the
`Debugging Airflow Components <20_debugging_airflow_components.rst>`__ guide.
