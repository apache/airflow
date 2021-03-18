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

Image build arguments reference
-------------------------------

The following build arguments (``--build-arg`` in docker build command) can be used for production images:

+------------------------------------------+------------------------------------------+------------------------------------------+
| Build argument                           | Default value                            | Description                              |
+==========================================+==========================================+==========================================+
| ``PYTHON_BASE_IMAGE``                    | ``python:3.6-slim-buster``               | Base python image.                       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PYTHON_MAJOR_MINOR_VERSION``           | ``3.6``                                  | major/minor version of Python (should    |
|                                          |                                          | match base image).                       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_VERSION``                      | ``2.0.1.dev0``                           | version of Airflow.                      |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_REPO``                         | ``apache/airflow``                       | the repository from which PIP            |
|                                          |                                          | dependencies are pre-installed.          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_BRANCH``                       | ``master``                               | the branch from which PIP dependencies   |
|                                          |                                          | are pre-installed initially.             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_LOCATION``         |                                          | If not empty, it will override the       |
|                                          |                                          | source of the constraints with the       |
|                                          |                                          | specified URL or file. Note that the     |
|                                          |                                          | file has to be in docker context so      |
|                                          |                                          | it's best to place such file in          |
|                                          |                                          | one of the folders included in           |
|                                          |                                          | ``.dockerignore`` file.                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_REFERENCE``        | ``constraints-master``                   | Reference (branch or tag) from GitHub    |
|                                          |                                          | where constraints file is taken from     |
|                                          |                                          | It can be ``constraints-master`` but     |
|                                          |                                          | also can be ``constraints-1-10`` for     |
|                                          |                                          | 1.10.* installation. In case of building |
|                                          |                                          | specific version you want to point it    |
|                                          |                                          | to specific tag, for example             |
|                                          |                                          | ``constraints-1.10.15``.                 |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_PROVIDERS_FROM_SOURCES``       | ``false``                                | If set to ``true`` and image is built    |
|                                          |                                          | from sources, all provider packages are  |
|                                          |                                          | installed from sources rather than from  |
|                                          |                                          | packages. It has no effect when          |
|                                          |                                          | installing from PyPI or GitHub repo.     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_EXTRAS``                       | (see Dockerfile)                         | Default extras with which airflow is     |
|                                          |                                          | installed.                               |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_FROM_PYPI``                    | ``true``                                 | If set to true, Airflow is installed     |
|                                          |                                          | from PyPI. if you want to install        |
|                                          |                                          | Airflow from self-build package          |
|                                          |                                          | you can set it to false, put package in  |
|                                          |                                          | ``docker-context-files`` and set         |
|                                          |                                          | ``INSTALL_FROM_DOCKER_CONTEXT_FILES`` to |
|                                          |                                          | ``true``. For this you have to also keep |
|                                          |                                          | ``AIRFLOW_PRE_CACHED_PIP_PACKAGES`` flag |
|                                          |                                          | set to ``false``.                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_PRE_CACHED_PIP_PACKAGES``      | ``false``                                | Allows to pre-cache airflow PIP packages |
|                                          |                                          | from the GitHub of Apache Airflow        |
|                                          |                                          | This allows to optimize iterations for   |
|                                          |                                          | Image builds and speeds up CI builds.    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_FROM_DOCKER_CONTEXT_FILES``    | ``false``                                | If set to true, Airflow, providers and   |
|                                          |                                          | all dependencies are installed from      |
|                                          |                                          | from locally built/downloaded            |
|                                          |                                          | .whl and .tar.gz files placed in the     |
|                                          |                                          | ``docker-context-files``. In certain     |
|                                          |                                          | corporate environments, this is required |
|                                          |                                          | to install airflow from such pre-vetted  |
|                                          |                                          | packages rather than from PyPI. For this |
|                                          |                                          | to work, also set ``INSTALL_FROM_PYPI``. |
|                                          |                                          | Note that packages starting with         |
|                                          |                                          | ``apache?airflow`` glob are treated      |
|                                          |                                          | differently than other packages. All     |
|                                          |                                          | ``apache?airflow`` packages are          |
|                                          |                                          | installed with dependencies limited by   |
|                                          |                                          | airflow constraints. All other packages  |
|                                          |                                          | are installed without dependencies       |
|                                          |                                          | 'as-is'. If you wish to install airflow  |
|                                          |                                          | via 'pip download' with all dependencies |
|                                          |                                          | downloaded, you have to rename the       |
|                                          |                                          | apache airflow and provider packages to  |
|                                          |                                          | not start with ``apache?airflow`` glob.  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``UPGRADE_TO_NEWER_DEPENDENCIES``        | ``false``                                | If set to true, the dependencies are     |
|                                          |                                          | upgraded to newer versions matching      |
|                                          |                                          | setup.py before installation.            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``CONTINUE_ON_PIP_CHECK_FAILURE``        | ``false``                                | By default the image build fails if pip  |
|                                          |                                          | check fails for it. This is good for     |
|                                          |                                          | interactive building but on CI the       |
|                                          |                                          | image should be built regardless - we    |
|                                          |                                          | have a separate step to verify image.    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_AIRFLOW_EXTRAS``            |                                          | Optional additional extras with which    |
|                                          |                                          | airflow is installed.                    |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_PYTHON_DEPS``               |                                          | Optional python packages to extend       |
|                                          |                                          | the image with some extra dependencies.  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_COMMAND``                      | (see Dockerfile)                         | Dev apt command executed before dev deps |
|                                          |                                          | are installed in the Build image.        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_COMMAND``           |                                          | Additional Dev apt command executed      |
|                                          |                                          | before dev dep are installed             |
|                                          |                                          | in the Build image. Should start with    |
|                                          |                                          | ``&&``.                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_DEPS``                         | (see Dockerfile)                         | Dev APT dependencies installed           |
|                                          |                                          | in the Build image.                      |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_DEPS``              |                                          | Additional apt dev dependencies          |
|                                          |                                          | installed in the Build image.            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_ENV``               |                                          | Additional env variables defined         |
|                                          |                                          | when installing dev deps.                |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``RUNTIME_APT_COMMAND``                  | (see Dockerfile)                         | Runtime apt command executed before deps |
|                                          |                                          | are installed in the Main image.         |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_COMMAND``       |                                          | Additional Runtime apt command executed  |
|                                          |                                          | before runtime dep are installed         |
|                                          |                                          | in the Main image. Should start with     |
|                                          |                                          | ``&&``.                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``RUNTIME_APT_DEPS``                     | (see Dockerfile)                         | Runtime APT dependencies installed       |
|                                          |                                          | in the Main image.                       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_DEPS``          |                                          | Additional apt runtime dependencies      |
|                                          |                                          | installed in the Main image.             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_ENV``           |                                          | Additional env variables defined         |
|                                          |                                          | when installing runtime deps.            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_HOME``                         | ``/opt/airflow``                         | Airflow’s HOME (that’s where logs and    |
|                                          |                                          | SQLite databases are stored).            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_UID``                          | ``50000``                                | Airflow user UID.                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_GID``                          | ``50000``                                | Airflow group GID. Note that most files  |
|                                          |                                          | created on behalf of airflow user belong |
|                                          |                                          | to the ``root`` group (0) to keep        |
|                                          |                                          | OpenShift Guidelines compatibility.      |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_USER_HOME_DIR``                | ``/home/airflow``                        | Home directory of the Airflow user.      |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``CASS_DRIVER_BUILD_CONCURRENCY``        | ``8``                                    | Number of processors to use for          |
|                                          |                                          | cassandra PIP install (speeds up         |
|                                          |                                          | installing in case cassandra extra is    |
|                                          |                                          | used).                                   |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_MYSQL_CLIENT``                 | ``true``                                 | Whether MySQL client should be installed |
|                                          |                                          | The mysql extra is removed from extras   |
|                                          |                                          | if the client is not installed.          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_PIP_VERSION``                  | ``20.2.4``                               | PIP version used.                        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``PIP_PROGRESS_BAR``                     | ``on``                                   | Progress bar for PIP installation        |
+------------------------------------------+------------------------------------------+------------------------------------------+

There are build arguments that determine the installation mechanism of Apache Airflow for the
production image. There are three types of build:

* From local sources (by default for example when you use ``docker build .``)
* You can build the image from released PyPI airflow package (used to build the official Docker image)
* You can build the image from any version in GitHub repository(this is used mostly for system testing).

+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| Build argument                    | Default                | What to specify                                                                   |
+===================================+========================+===================================================================================+
| ``AIRFLOW_INSTALLATION_METHOD``   | ``apache-airflow``     | Should point to the installation method of Apache Airflow. It can be              |
|                                   |                        | ``apache-airflow`` for installation from packages and URL to installation from    |
|                                   |                        | GitHub repository tag or branch or "." to install from sources.                   |
|                                   |                        | Note that installing from local sources requires appropriate values of the        |
|                                   |                        | ``AIRFLOW_SOURCES_FROM`` and ``AIRFLOW_SOURCES_TO`` variables as described below. |
|                                   |                        | Only used when ``INSTALL_FROM_PYPI`` is set to ``true``.                          |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| ``AIRFLOW_VERSION_SPECIFICATION`` |                        | Optional - might be used for package installation of different Airflow version    |
|                                   |                        | for example"==2.0.1". For consistency, you should also set``AIRFLOW_VERSION``     |
|                                   |                        | to the same value AIRFLOW_VERSION is resolved as label in the image created.      |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_REFERENCE`` | ``constraints-master`` | Reference (branch or tag) from GitHub where constraints file is taken from.       |
|                                   |                        | It can be ``constraints-master`` but also can be``constraints-1-10`` for          |
|                                   |                        | 1.10.*  installations. In case of building specific version                       |
|                                   |                        | you want to point it to specific tag, for example ``constraints-2.0.1``           |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| ``AIRFLOW_WWW``                   | ``www``                | In case of Airflow 2.0 it should be "www", in case of Airflow 1.10                |
|                                   |                        | series it should be "www_rbac".                                                   |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| ``AIRFLOW_SOURCES_FROM``          | ``empty``              | Sources of Airflow. Set it to "." when you install airflow from                   |
|                                   |                        | local sources.                                                                    |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
| ``AIRFLOW_SOURCES_TO``            | ``/empty``             | Target for Airflow sources. Set to "/opt/airflow" when                            |
|                                   |                        | you want to install airflow from local sources.                                   |
+-----------------------------------+------------------------+-----------------------------------------------------------------------------------+
