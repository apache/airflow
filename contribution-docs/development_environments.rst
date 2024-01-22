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
develop Apache Airflow:

-   `Local virtualenv development environment <local_virtualenv.rst>`_
    that supports running unit tests and can be used in your IDE.

-   `Breeze Docker-based development environment <dev/breeze/doc/breeze.rst>`_ that provides
    an end-to-end CI solution with all software dependencies covered.

The table below summarizes differences between the environments:

========================= ================================ ===================================== ========================================
**Property**              **Local virtualenv**             **Breeze environment**                 **GitHub Codespaces**
========================= ================================ ===================================== ========================================
Dev machine needed        - (-) You need a dev PC          - (-) You need a dev PC                (+) Works with remote setup
------------------------- -------------------------------- ------------------------------------- ----------------------------------------
Test coverage             - (-) unit tests only            - (+) integration and unit tests       (*/-) integration tests (extra config)
------------------------- -------------------------------- ------------------------------------- ----------------------------------------
Setup                     - (+) automated with breeze cmd  - (+) automated with breeze cmd        (+) automated with VSCode
------------------------- -------------------------------- ------------------------------------- ----------------------------------------
Installation difficulty   - (-) depends on the OS setup    - (+) works whenever Docker works      (+) works in a modern browser/VSCode
------------------------- -------------------------------- ------------------------------------- ----------------------------------------
Team synchronization      - (-) difficult to achieve       - (+) reproducible within team         (+) reproducible within team
------------------------- -------------------------------- ------------------------------------- ----------------------------------------
Reproducing CI failures   - (-) not possible in many cases - (+) fully reproducible               (+) reproduce CI failures
------------------------- -------------------------------- ------------------------------------- ----------------------------------------
Ability to update         - (-) requires manual updates    - (+) automated update via breeze cmd  (+/-) can be rebuild on demand
------------------------- -------------------------------- ------------------------------------- ----------------------------------------
Disk space and CPU usage  - (+) relatively lightweight     - (-) uses GBs of disk and many CPUs   (-) integration tests (extra config)
------------------------- -------------------------------- ------------------------------------- ----------------------------------------
IDE integration           - (+) straightforward            - (-) via remote debugging only        (-) integration tests (extra config)
========================= ================================ ===================================== ----------------------------------------


Typically, you are recommended to use both of these environments depending on your needs.

Local virtualenv Development Environment
----------------------------------------

All details about using and running local virtualenv environment for Airflow can be found
in `local_virtualenv.rst <local_virtualenv.rst>`__.

Benefits:

-   Packages are installed locally. No container environment is required.

-   You can benefit from local debugging within your IDE.

-   With the virtualenv in your IDE, you can benefit from autocompletion and running tests directly from the IDE.

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
    `Breeze development environment <dev/breeze/doc/breeze.rst>`__ with all required packages
    pre-installed.

-   You need to make sure that your local environment is consistent with other
    developer environments. This often leads to a "works for me" syndrome. The
    Breeze container-based solution provides a reproducible environment that is
    consistent with other developers.

-   You are **STRONGLY** encouraged to also install and use `pre-commit hooks <static_code_checks.rst#pre-commit-hooks>`_
    for your local virtualenv development environment.
    Pre-commit hooks can speed up your development cycle a lot.

Breeze Development Environment
------------------------------

All details about using and running Airflow Breeze can be found in
`Breeze <dev/breeze/doc/breeze.rst>`__.

The Airflow Breeze solution is intended to ease your local development as "*It's
a Breeze to develop Airflow*".

Benefits:

-   Breeze is a complete environment that includes external components, such as
    mysql database, hadoop, mongo, cassandra, redis, etc., required by some of
    Airflow tests. Breeze provides a preconfigured Docker Compose environment
    where all these services are available and can be used by tests
    automatically.

-   Breeze environment is almost the same as used in the CI automated builds.
    So, if the tests run in your Breeze environment, they will work in the CI as well.
    See `<CI.rst>`_ for details about Airflow CI.

Limitations:

-   Breeze environment takes significant space in your local Docker cache. There
    are separate environments for different Python and Airflow versions, and
    each of the images takes around 3GB in total.

-   Though Airflow Breeze setup is automated, it takes time. The Breeze
    environment uses pre-built images from DockerHub and it takes time to
    download and extract those images. Building the environment for a particular
    Python version takes less than 10 minutes.

-   Breeze environment runs in the background taking precious resources, such as
    disk space and CPU. You can stop the environment manually after you use it
    or even use a ``bare`` environment to decrease resource usage.



.. note::

   Breeze CI images are not supposed to be used in production environments.
   They are optimized for repeatability of tests, maintainability and speed of building rather
   than production performance. The production images are not yet officially published.
