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

Dockerfile Changelog
====================

The ``Dockerfile`` does not strictly follow the `SemVer <https://semver.org/>`_ approach of
Apache Airflow when it comes to features and backwards compatibility. While Airflow code strictly
follows it, the ``Dockerfile`` is really a way to give users a conveniently packaged Airflow
using standard container approach, so occasionally there are some changes in the building process
or in the entrypoint of the image that require slight adaptation of how it is used or built.

The Changelog below describes the changes introduced in each version of the docker images released by
the Airflow team.

:note: The Changelog below concerns only the convenience production images released at
       `Airflow DockerHub <https://hub.docker.com/r/apache/airflow>`_ . The images that are released
       there, are usually built using the ``Dockerfile`` released together with Airflow. However You are
       free to take latest released ``Dockerfile`` from Airflow and use it to build an image for
       any Airflow version from the ``Airflow 2`` line. There is no guarantee that it works, but if it does,
       then you can use latest features from that image to build the previous Airflow versions.

Changes after publishing the images
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Occasionally our images need to be regenerated using newer ``Dockerfiles`` or constraints.
This happens when an issue is found or a breaking change is released by our dependencies
that invalidates the already released image, and regenerating the image makes it usable again.
While we cannot assure 100% backwards compatibility when it happens, we at least document it
here so that users affected can find the reason for the changes.

+--------------+---------------------+-----------------------------------------+------------------------+----------------------------------------------+
| Date         | Affected images     | Potentially breaking change             | Reason                 | Link to Pull Request                         |
+==============+=====================+=========================================+========================+==============================================+
| 17 June 2022 | 2.2.5               | * The ``Authlib`` library downgraded    | Flask App Builder      | https://github.com/apache/airflow/pull/24516 |
|              |                     |   from 1.0.1 to 0.15.5 version          | not compatible with    |                                              |
|              | 2.3.0-2.3.2         |                                         | Authlib >= 1.0.0       |                                              |
+--------------+---------------------+-----------------------------------------+------------------------+----------------------------------------------+
| 18 Jan 2022  | All 2.2.\*, 2.1.\*  | * The AIRFLOW_GID 500 was removed       | MySQL changed keys     | https://github.com/apache/airflow/pull/20912 |
|              |                     | * MySQL ``apt`` repository key changed. | to sign their packages |                                              |
|              |                     |                                         | on 17 Jan 2022         |                                              |
+--------------+---------------------+-----------------------------------------+------------------------+----------------------------------------------+


Airflow 2.3
~~~~~~~~~~~

* 2.4.0

  * You can specify additional ``pip install`` flags when you build the image via ``ADDITIONAL_PIP_INSTALL_FLAGS``
    build arg.

* 2.3.0

  * Airflow 2.3 ``Dockerfile`` is now better optimized for caching and "standalone" which means that you
    can copy **just** the ``Dockerfile`` to any folder and start building custom images. This
    however requires `Buildkit <https://docs.docker.com/develop/develop-images/build_enhancements/>`_
    to build the image because we started using features that are only available in ``Buildkit``.
    This can be done by setting ``DOCKER_BUILDKIT=1`` as an environment variable
    or by installing `the buildx plugin <https://docs.docker.com/buildx/working-with-buildx/>`_
    and running ``docker buildx build`` command.
  * Add Python 3.10 support
  * Add support for Bullseye Debian release (Debian Buster is deprecated)
  * Add Multi-Platform support (AMD64/ARM64) in order to accommodate MacOS M1 users
  * Build parameters which control if packages and Airflow should be installed from context file were
    unified
  * The ``INSTALL_FROM_PYPI`` arg was removed - it is automatically detected now.
  * The ``INSTALL_FROM_DOCKER_CONTEXT_FILES`` arg changed to ``INSTALL_PACKAGES_FROM_CONTEXT``

Airflow 2.2
~~~~~~~~~~~

* 2.2.4
  * Add support for both ``.piprc`` and ``pip.conf`` customizations
  * Add ArtifactHub labels for better discovery of the images
  * Update default Python image to be 3.7
  * Build images with ``Buildkit`` (optional)
  * Fix building the image on Azure with ``text file busy`` error

* 2.2.3
  * No changes

* 2.2.2
  * No changes

* 2.2.1
  * Workaround the problem with ``libstdcpp`` TLS error

* 2.2.0
  * Remove AIRFLOW_GID (5000) from Airflow images (potentially breaking change for users using it)
  * Added warnings for Quick-start docker compose
  * Fix warm shutdown for celery worker (signal propagation)
  * Add Oauth libraries to PROD images
  * Add Python 3.9 support

Airflow 2.1
~~~~~~~~~~~

* MySQL changed the keys to sign their packages on 17 Feb 2022. This caused all released images
  to fail when being extended. As result, on 18 Feb 2021 we re-released all
  the ``2.2`` and ``2.1`` images with latest versions of ``Dockerfile``
  containing the new signing key.

  There were subtle changes in the behaviour of some 2.1 images due to that (more details below)
  Detailed `issue here <https://github.com/apache/airflow/issues/20911>`_

:note: that the changes below were valid before image refreshing on 18 Feb 2022.
  Since all the images were refreshed on 18 Feb with the same ``Dockerfile``
  as 2.1.4, the changes 2.1.1 -> 2.1.3 are
  effectively applied to all the images in 2.1.* line.
  The images refreshed have also those fixes added:

* All 2.1.* image versions refreshed on 18 Feb 2022 have those fixes applied:
  * Fix building the image on Azure with ``text file busy`` error
  * Workaround the problem with ``libstdcpp`` TLS error
  * Remove AIRFLOW_GID (5000) from Airflow images (potentially breaking change for users using it)
  * Added warnings for Quick-start docker compose
  * Add Oauth libraries to PROD images

Original image Changelog (before the refresh on 18 Feb 2022):

* 2.1.4
   * Workaround the problem with ``libstdcpp`` TLS error
   * fixed detection of port number in connection URL
   * Improve warnings for quick-start-docker compose
   * Fix warm shutdown for celery worker (signal propagation)

* 2.1.3
   * fixed auto-creation of user to use non-deprecated ``create user`` rather than ``user_create``
   * remove waiting for celery backend for ``worker`` and ``flower`` commands rather than ``scheduler`` and ``celery`` only
   * remove deprecated ``airflow upgradedb`` command from Airflow 1.10 in case upgrade is requested
   * Add Python 3.9 support

* 2.1.2
   * No changes

* 2.1.1
   * Fix failure of lack of default commands (failed when no commands were passed)
   * Added ``_PIP_ADDITIONAL_REQUIREMENTS`` development feature

* 2.1.0
   * Unset default ``PIP_USER`` variable - which caused PythonVirtualEnv to fail


Airflow 2.0
~~~~~~~~~~~

* MySQL changed the keys to sign their packages on 17 Feb 2022. This caused all released images
  to fail when being extended. As result, on 18 Feb 2021 we re-released all
  the ``2.2`` and ``2.1`` images with latest versions of ``Dockerfile``
  containing the new signing key.

  There were no changes in the behaviour of 2.0.2 image due to that
  Detailed `issue here <https://github.com/apache/airflow/issues/20911>`_ .
  Only 2.0.2 image was regenerated, as 2.0.1 and 2.0.0 versions are hardly used and it is unlikely someone
  would like to extend those images. Extending 2.0.1 and 2.0.0 images will lead to failures of "missing key".

* 2.0.2
   * Set correct PYTHONPATH for ``root`` user. Allows to run the image as root
   * Warn if the deprecated 5000 group ID was used for airflow user when running the image
     (should be 0 for the OpenShift compatibility). Fails if the group 5000 was used with any other user
     (it would not work anyway but with cryptic errors)
   * Set umask as 002 by default, so that you can actually change the user id used to run the image
     (required for OpenShift compatibility)
   * Skip checking the DB and celery backend if CONNECTION_CHECK_MAX_COUNT is equal to 0

* 2.0.1
   * Avoid reverse IP lookup when checking DB availability. This helped to solve long delays on misconfigured
     docker engines
   * Add auto-detection of redis and amqp broker ports
   * Fixed detection of all user/password combinations in URLs - helps in auto-detecting ports and testing
     connectivity
   * Add possibility to create Admin user automatically when entering the image
   * Automatically create system user when different user than ``airflow`` is used. Needed for OpenShift
     compatibility
   * Allows to exec to ``bash`` or ``python`` if specified as parameters
   * Remove ``airflow`` command if it is specified as first parameter of the ``run`` command

* 2.0.0
   * Initial release of the image based on Debian Buster
