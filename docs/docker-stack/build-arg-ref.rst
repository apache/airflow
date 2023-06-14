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

The following build arguments (``--build-arg`` in docker build command) can be used for production images.
Those arguments are used when you want to customize the image. You can see some examples of it in
:ref:`Building from PyPI packages<image-build-pypi>`.

Basic arguments
...............

Those are the most common arguments that you use when you want to build a custom image.

+------------------------------------------+------------------------------------------+---------------------------------------------+
| Build argument                           | Default value                            | Description                                 |
+==========================================+==========================================+=============================================+
| ``PYTHON_BASE_IMAGE``                    | ``python:3.8-slim-bullseye``             | Base python image.                          |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``AIRFLOW_VERSION``                      | :subst-code:`|airflow-version|`          | version of Airflow.                         |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``AIRFLOW_EXTRAS``                       | (see below the table)                    | Default extras with which airflow is        |
|                                          |                                          | installed.                                  |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``ADDITIONAL_AIRFLOW_EXTRAS``            |                                          | Optional additional extras with which       |
|                                          |                                          | airflow is installed.                       |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``AIRFLOW_HOME``                         | ``/opt/airflow``                         | Airflow's HOME (that's where logs and       |
|                                          |                                          | SQLite databases are stored).               |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``AIRFLOW_USER_HOME_DIR``                | ``/home/airflow``                        | Home directory of the Airflow user.         |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``AIRFLOW_PIP_VERSION``                  | ``23.1.2``                               |  PIP version used.                          |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``ADDITIONAL_PIP_INSTALL_FLAGS``         |                                          | additional ``pip`` flags passed to the      |
|                                          |                                          | installation commands (except when          |
|                                          |                                          | reinstalling ``pip`` itself)                |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``PIP_PROGRESS_BAR``                     | ``on``                                   | Progress bar for PIP installation           |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``AIRFLOW_UID``                          | ``50000``                                | Airflow user UID.                           |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``AIRFLOW_CONSTRAINTS``                  | ``constraints``                          | Type of constraints to build the image.     |
|                                          |                                          | This can be ``constraints`` for regular     |
|                                          |                                          | images or ``constraints-no-providers`` for  |
|                                          |                                          | slim images.                                |
+------------------------------------------+------------------------------------------+---------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_REFERENCE``        |                                          | Reference (branch or tag) from GitHub       |
|                                          |                                          | where constraints file is taken from        |
|                                          |                                          | It can be ``constraints-main`` or           |
|                                          |                                          | ``constraints-2-0`` for                     |
|                                          |                                          | 2.0.* installation. In case of building     |
|                                          |                                          | specific version you want to point it       |
|                                          |                                          | to specific tag, for example                |
|                                          |                                          | :subst-code:`constraints-|airflow-version|`.|
|                                          |                                          | Auto-detected if empty.                     |
+------------------------------------------+------------------------------------------+---------------------------------------------+

.. note::

    Before Airflow 2.2, the image also had ``AIRFLOW_GID`` parameter, but it did not provide any additional
    functionality - only added confusion - so it has been removed.

List of default extras in the production Dockerfile:

.. BEGINNING OF EXTRAS LIST UPDATED BY PRE COMMIT

* aiobotocore
* amazon
* async
* celery
* cncf.kubernetes
* dask
* docker
* elasticsearch
* ftp
* google
* google_auth
* grpc
* hashicorp
* http
* ldap
* microsoft.azure
* mysql
* odbc
* pandas
* postgres
* redis
* sendgrid
* sftp
* slack
* snowflake
* ssh
* statsd
* virtualenv

.. END OF EXTRAS LIST UPDATED BY PRE COMMIT

Image optimization options
..........................

The main advantage of Customization method of building Airflow image, is that it allows to build highly optimized image because
the final image (RUNTIME) might not contain all the dependencies that are needed to build and install all other dependencies
(DEV). Those arguments allow to control what is installed in the DEV image and what is installed in RUNTIME one, thus
allowing to produce much more optimized images. See :ref:`Building optimized images<image-build-optimized>`.
for examples of using those arguments.

+------------------------------------------+------------------------------------------+------------------------------------------+
| Build argument                           | Default value                            | Description                              |
+==========================================+==========================================+==========================================+
| ``UPGRADE_TO_NEWER_DEPENDENCIES``        | ``false``                                | If set to a value different than "false" |
|                                          |                                          | the dependencies are upgraded to newer   |
|                                          |                                          | versions. In CI it is set to build id    |
|                                          |                                          | to make sure subsequent builds are not   |
|                                          |                                          | reusing cached images with same value.   |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_PYTHON_DEPS``               |                                          | Optional python packages to extend       |
|                                          |                                          | the image with some extra dependencies.  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_COMMAND``                      |                                          | Dev apt command executed before dev deps |
|                                          |                                          | are installed in the Build image.        |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_COMMAND``           |                                          | Additional Dev apt command executed      |
|                                          |                                          | before dev dep are installed             |
|                                          |                                          | in the Build image. Should start with    |
|                                          |                                          | ``&&``.                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``DEV_APT_DEPS``                         | Empty - install default dependencies     | Dev APT dependencies installed           |
|                                          | (see ``install_os_dependencies.sh``)     | in the Build image.                      |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_DEPS``              |                                          | Additional apt dev dependencies          |
|                                          |                                          | installed in the Build image.            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_DEV_APT_ENV``               |                                          | Additional env variables defined         |
|                                          |                                          | when installing dev deps.                |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``RUNTIME_APT_COMMAND``                  |                                          | Runtime apt command executed before deps |
|                                          |                                          | are installed in the ``main`` stage.     |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_COMMAND``       |                                          | Additional Runtime apt command executed  |
|                                          |                                          | before runtime dep are installed         |
|                                          |                                          | in the ``main`` stage. Should start with |
|                                          |                                          | ``&&``.                                  |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``RUNTIME_APT_DEPS``                     | Empty - install default dependencies     | Runtime APT dependencies installed       |
|                                          | (see ``install_os_dependencies.sh``)     | in the Main image.                       |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_DEPS``          |                                          | Additional apt runtime dependencies      |
|                                          |                                          | installed in the Main image.             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``ADDITIONAL_RUNTIME_APT_ENV``           |                                          | Additional env variables defined         |
|                                          |                                          | when installing runtime deps.            |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_MYSQL_CLIENT``                 | ``true``                                 | Whether MySQL client should be installed |
|                                          |                                          | The mysql extra is removed from extras   |
|                                          |                                          | if the client is not installed.          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_MSSQL_CLIENT``                 | ``true``                                 | Whether MsSQL client should be installed |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_POSTGRES_CLIENT``              | ``true``                                 | Whether Postgres client should be        |
|                                          |                                          | installed                                |
+------------------------------------------+------------------------------------------+------------------------------------------+

Installing Airflow using different methods
..........................................

Those parameters are useful only if you want to install Airflow using different installation methods than the default
(installing from PyPI packages).

This is usually only useful if you have your own fork of Airflow and want to build the images locally from
those sources - either locally or directly from GitHub sources. This way you do not need to release your
Airflow and Providers via PyPI - they can be installed directly from sources or from GitHub repository.
Another option of installation is to build Airflow from previously prepared binary Python packages which might
be useful if you need to build Airflow in environments that require high levels of security.

You can see some examples of those in:
  * :ref:`Building from GitHub<image-build-github>`,
  * :ref:`Using custom installation sources<image-build-custom>`,
  * :ref:`Build images in security restricted environments<image-build-secure-environments>`

+------------------------------------+------------------------------------------+------------------------------------------+
| Build argument                     | Default value                            | Description                              |
+====================================+==========================================+==========================================+
| ``AIRFLOW_INSTALLATION_METHOD``    | ``apache-airflow``                       | Installation method of Apache Airflow.   |
|                                    |                                          | ``apache-airflow`` for installation from |
|                                    |                                          | PyPI. It can be GitHub repository URL    |
|                                    |                                          | including branch or tag to install from  |
|                                    |                                          | that repository or "." to install from   |
|                                    |                                          | local sources. Installing from sources   |
|                                    |                                          | requires appropriate values of the       |
|                                    |                                          | ``AIRFLOW_SOURCES_FROM`` and             |
|                                    |                                          | ``AIRFLOW_SOURCES_TO`` variables (see    |
|                                    |                                          | below)                                   |
+------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_SOURCES_FROM``           | ``Dockerfile``                           | Sources of Airflow. Set it to "." when   |
|                                    |                                          | you install Airflow from local sources   |
+------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_SOURCES_TO``             | ``/Dockerfile``                          | Target for Airflow sources. Set to       |
|                                    |                                          | "/opt/airflow" when you install Airflow  |
|                                    |                                          | from local sources.                      |
+------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_VERSION_SPECIFICATION``  |                                          | Optional - might be used for using limit |
|                                    |                                          | for Airflow version installation - for   |
|                                    |                                          | example ``<2.0.2`` for automated builds. |
+------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_PROVIDERS_FROM_SOURCES`` | ``false``                                | If set to ``true`` and image is built    |
|                                    |                                          | from sources, all provider packages are  |
|                                    |                                          | installed from sources rather than from  |
|                                    |                                          | packages. It has no effect when          |
|                                    |                                          | installing from PyPI or GitHub repo.     |
+------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_CONSTRAINTS_LOCATION``   |                                          | If not empty, it will override the       |
|                                    |                                          | source of the constraints with the       |
|                                    |                                          | specified URL or file. Note that the     |
|                                    |                                          | file has to be in Docker context so      |
|                                    |                                          | it's best to place such file in          |
|                                    |                                          | one of the folders included in           |
|                                    |                                          | ``.dockerignore`` file.                  |
+------------------------------------+------------------------------------------+------------------------------------------+
| ``DOCKER_CONTEXT_FILES``           | ``Dockerfile``                           | If set to a folder (for example to       |
|                                    |                                          | ``docker-context-files`` folder), then   |
|                                    |                                          | this folder will be copied to the        |
|                                    |                                          | ``docker-context-files`` inside the      |
|                                    |                                          | context of docker and you will be able   |
|                                    |                                          | to install from binary files present     |
|                                    |                                          | there. By default we set it to           |
|                                    |                                          | Dockerfile as we know the file is there, |
|                                    |                                          | otherwise the COPY instruction fails.    |
+------------------------------------+------------------------------------------+------------------------------------------+
| ``INSTALL_PACKAGES_FROM_CONTEXT``  | ``false``                                | If set to true, Airflow, providers and   |
|                                    |                                          | all dependencies are installed from      |
|                                    |                                          | from locally built/downloaded            |
|                                    |                                          | .whl and .tar.gz files placed in the     |
|                                    |                                          | ``docker-context-files``.                |
+------------------------------------+------------------------------------------+------------------------------------------+

Pre-caching PIP dependencies
............................

When image is build from PIP, by default pre-caching of PIP dependencies is used. This is in order to speed-up incremental
builds during development. When pre-cached PIP dependencies are used and ``setup.py`` or ``setup.cfg`` changes, the
PIP dependencies are already pre-installed, thus resulting in much faster image rebuild. This is purely an optimization
of time needed to build the images and should be disabled if you want to install Airflow from
Docker context files.

+------------------------------------------+------------------------------------------+------------------------------------------+
| Build argument                           | Default value                            | Description                              |
+==========================================+==========================================+==========================================+
| ``AIRFLOW_BRANCH``                       | ``main``                                 | the branch from which PIP dependencies   |
|                                          |                                          | are pre-installed initially.             |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_REPO``                         | ``apache/airflow``                       | the repository from which PIP            |
|                                          |                                          | dependencies are pre-installed.          |
+------------------------------------------+------------------------------------------+------------------------------------------+
| ``AIRFLOW_PRE_CACHED_PIP_PACKAGES``      | ``false``                                | Allows to pre-cache airflow PIP packages |
|                                          |                                          | from the GitHub of Apache Airflow        |
|                                          |                                          | This allows to optimize iterations for   |
|                                          |                                          | Image builds and speeds up CI builds.    |
+------------------------------------------+------------------------------------------+------------------------------------------+
