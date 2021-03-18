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

Building the image
==================

Before you dive-deeply in the way how the Airflow Image is build, named and why we are doing it the
way we do, you might want to know very quickly how you can extend or customize the existing image
for Apache Airflow. This chapter gives you a short answer to those questions.

Airflow Summit 2020's `Production Docker Image <https://youtu.be/wDr3Y7q2XoI>`_ talk provides more
details about the context, architecture and customization/extension methods for the Production Image.

Extending the image
-------------------

Extending the image is easiest if you just need to add some dependencies that do not require
compiling. The compilation framework of Linux (so called ``build-essential``) is pretty big, and
for the production images, size is really important factor to optimize for, so our Production Image
does not contain ``build-essential``. If you need compiler like gcc or g++ or make/cmake etc. - those
are not found in the image and it is recommended that you follow the "customize" route instead.

How to extend the image - it is something you are most likely familiar with - simply
build a new image using Dockerfile's ``FROM`` directive and add whatever you need. Then you can add your
Debian dependencies with ``apt`` or PyPI dependencies with ``pip install`` or any other stuff you need.

You should be aware, about a few things:

* The production image of airflow uses "airflow" user, so if you want to add some of the tools
  as ``root`` user, you need to switch to it with ``USER`` directive of the Dockerfile. Also you
  should remember about following the
  `best practises of Dockerfiles <https://docs.docker.com/develop/develop-images/dockerfile_best-practices/>`_
  to make sure your image is lean and small.

  .. code-block:: dockerfile

    FROM apache/airflow:2.0.1
    USER root
    RUN apt-get update \
      && apt-get install -y --no-install-recommends \
             my-awesome-apt-dependency-to-add \
      && apt-get autoremove -yqq --purge \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*
    USER airflow


* PyPI dependencies in Apache Airflow are installed in the user library, of the "airflow" user, so
  you need to install them with the ``--user`` flag and WITHOUT switching to airflow user. Note also
  that using --no-cache-dir is a good idea that can help to make your image smaller.

  .. code-block:: dockerfile

    FROM apache/airflow:2.0.1
    RUN pip install --no-cache-dir --user my-awesome-pip-dependency-to-add

* As of 2.0.1 image the ``--user`` flag is turned on by default by setting ``PIP_USER`` environment variable
  to ``true``. This can be disabled by un-setting the variable or by setting it to ``false``.


* If your apt, or PyPI dependencies require some of the build-essentials, then your best choice is
  to follow the "Customize the image" route. However it requires to checkout sources of Apache Airflow,
  so you might still want to choose to add build essentials to your image, even if your image will
  be significantly bigger.

  .. code-block:: dockerfile

    FROM apache/airflow:2.0.1
    USER root
    RUN apt-get update \
      && apt-get install -y --no-install-recommends \
             build-essential my-awesome-apt-dependency-to-add \
      && apt-get autoremove -yqq --purge \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*
    USER airflow
    RUN pip install --no-cache-dir --user my-awesome-pip-dependency-to-add

* You can also embed your dags in the image by simply adding them with COPY directive of Airflow.
  The DAGs in production image are in ``/opt/airflow/dags`` folder.

Customizing the image
---------------------

Customizing the image is an alternative way of adding your own dependencies to the image - better
suited to prepare optimized production images.

The advantage of this method is that it produces optimized image even if you need some compile-time
dependencies that are not needed in the final image. You need to use Airflow Sources to build such images
from the `official distribution folder of Apache Airflow <https://downloads.apache.org/airflow/>`_ for the
released versions, or checked out from the GitHub project if you happen to do it from git sources.

The easiest way to build the image is to use ``breeze`` script, but you can also build such customized
image by running appropriately crafted docker build in which you specify all the ``build-args``
that you need to add to customize it. You can read about all the args and ways you can build the image
in :doc:`build-arg-ref`.

Here just a few examples are presented which should give you general understanding of what you can customize.

This builds production image in version 3.6 with default extras from the local sources (master version
of 2.0 currently):

.. code-block:: bash

  docker build .

This builds the production image in version 3.7 with default extras from 2.0.1 tag and
constraints taken from constraints-2-0 branch in GitHub.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="https://github.com/apache/airflow/archive/2.0.1.tar.gz#egg=apache-airflow" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2-0" \
    --build-arg AIRFLOW_BRANCH="v1-10-test" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty"

This builds the production image in version 3.7 with default extras from 2.0.1 PyPI package and
constraints taken from 2.0.1 tag in GitHub and pre-installed pip dependencies from the top
of ``v1-10-test`` branch.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.1" \
    --build-arg AIRFLOW_VERSION_SPECIFICATION="==2.0.1" \
    --build-arg AIRFLOW_BRANCH="v1-10-test" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2.0.1" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty"

This builds the production image in version 3.7 with additional airflow extras from 2.0.1 PyPI package and
additional python dependencies and pre-installed pip dependencies from 2.0.1 tagged constraints.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.1" \
    --build-arg AIRFLOW_VERSION_SPECIFICATION="==2.0.1" \
    --build-arg AIRFLOW_BRANCH="v1-10-test" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2.0.1" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="mssql,hdfs" \
    --build-arg ADDITIONAL_PYTHON_DEPS="sshtunnel oauth2client"

This builds the production image in version 3.7 with additional airflow extras from 2.0.1 PyPI package and
additional apt dev and runtime dependencies.

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.1" \
    --build-arg AIRFLOW_VERSION_SPECIFICATION="==2.0.1" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2-0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="jdbc" \
    --build-arg ADDITIONAL_PYTHON_DEPS="pandas" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="gcc g++" \
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="default-jre-headless" \
    --tag my-image


The same image can be built using ``breeze`` (it supports auto-completion of the options):

.. code-block:: bash

  ./breeze build-image \
      --production-image  --python 3.7 --install-airflow-version=2.0.1 \
      --additional-extras=jdbc --additional-python-deps="pandas" \
      --additional-dev-apt-deps="gcc g++" --additional-runtime-apt-deps="default-jre-headless"


You can customize more aspects of the image - such as additional commands executed before apt dependencies
are installed, or adding extra sources to install your dependencies from. You can see all the arguments
described below but here is an example of rather complex command to customize the image
based on example in `this comment <https://github.com/apache/airflow/issues/8605#issuecomment-690065621>`_:

.. code-block:: bash

  docker build . -f Dockerfile \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.1" \
    --build-arg AIRFLOW_VERSION_SPECIFICATION="==2.0.1" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2-0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg ADDITIONAL_AIRFLOW_EXTRAS="slack" \
    --build-arg ADDITIONAL_PYTHON_DEPS=" \
        apache-airflow-providers-odbc \
        azure-storage-blob \
        sshtunnel \
        google-api-python-client \
        oauth2client \
        beautifulsoup4 \
        dateparser \
        rocketchat_API \
        typeform" \
    --build-arg ADDITIONAL_DEV_APT_DEPS="msodbcsql17 unixodbc-dev g++" \
    --build-arg ADDITIONAL_DEV_APT_COMMAND="curl https://packages.microsoft.com/keys/microsoft.asc | \
    apt-key add --no-tty - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
    --build-arg ADDITIONAL_DEV_ENV_VARS="ACCEPT_EULA=Y" \
    --build-arg ADDITIONAL_RUNTIME_APT_COMMAND="curl https://packages.microsoft.com/keys/microsoft.asc | \
    apt-key add --no-tty - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list" \
    --build-arg ADDITIONAL_RUNTIME_APT_DEPS="msodbcsql17 unixodbc git procps vim" \
    --build-arg ADDITIONAL_RUNTIME_ENV_VARS="ACCEPT_EULA=Y" \
    --tag my-image

Customizing images in high security restricted environments
...........................................................

You can also make sure your image is only build using local constraint file and locally downloaded
wheel files. This is often useful in Enterprise environments where the binary files are verified and
vetted by the security teams.

This builds below builds the production image in version 3.7 with packages and constraints used from the local
``docker-context-files`` rather than installed from PyPI or GitHub. It also disables MySQL client
installation as it is using external installation method.

Note that as a prerequisite - you need to have downloaded wheel files. In the example below we
first download such constraint file locally and then use ``pip download`` to get the ``.whl`` files needed
but in most likely scenario, those wheel files should be copied from an internal repository of such .whl
files. Note that ``AIRFLOW_VERSION_SPECIFICATION`` is only there for reference, the apache airflow ``.whl`` file
in the right version is part of the ``.whl`` files downloaded.

Note that 'pip download' will only works on Linux host as some of the packages need to be compiled from
sources and you cannot install them providing ``--platform`` switch. They also need to be downloaded using
the same python version as the target image.

The ``pip download`` might happen in a separate environment. The files can be committed to a separate
binary repository and vetted/verified by the security team and used subsequently to build images
of Airflow when needed on an air-gaped system.

Preparing the constraint files and wheel files:

.. code-block:: bash

  rm docker-context-files/*.whl docker-context-files/*.txt

  curl -Lo "docker-context-files/constraints-2-0.txt" \
    https://raw.githubusercontent.com/apache/airflow/constraints-2-0/constraints-3.7.txt

  pip download --dest docker-context-files \
    --constraint docker-context-files/constraints-2-0.txt  \
    apache-airflow[async,aws,azure,celery,dask,elasticsearch,gcp,kubernetes,mysql,postgres,redis,slack,ssh,statsd,virtualenv]==2.0.1

Since apache-airflow .whl packages are treated differently by the docker image, you need to rename the
downloaded apache-airflow* files, for example:

.. code-block:: bash

   pushd docker-context-files
   for file in apache?airflow*
   do
     mv ${file} _${file}
   done
   popd

Building the image:

.. code-block:: bash

  ./breeze build-image \
      --production-image --python 3.7 --install-airflow-version=2.0.1 \
      --disable-mysql-client-installation --disable-pip-cache --install-from-local-files-when-building \
      --constraints-location="/docker-context-files/constraints-2-0.txt"

or

.. code-block:: bash

  docker build . \
    --build-arg PYTHON_BASE_IMAGE="python:3.7-slim-buster" \
    --build-arg PYTHON_MAJOR_MINOR_VERSION=3.7 \
    --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
    --build-arg AIRFLOW_VERSION="2.0.1" \
    --build-arg AIRFLOW_VERSION_SPECIFICATION="==2.0.1" \
    --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2-0" \
    --build-arg AIRFLOW_SOURCES_FROM="empty" \
    --build-arg AIRFLOW_SOURCES_TO="/empty" \
    --build-arg INSTALL_MYSQL_CLIENT="false" \
    --build-arg AIRFLOW_PRE_CACHED_PIP_PACKAGES="false" \
    --build-arg INSTALL_FROM_DOCKER_CONTEXT_FILES="true" \
    --build-arg AIRFLOW_CONSTRAINTS_LOCATION="/docker-context-files/constraints-2-0.txt"


Customizing & extending the image together
..........................................

You can combine both - customizing & extending the image. You can build the image first using
``customize`` method (either with docker command or with ``breeze`` and then you can ``extend``
the resulting image using ``FROM`` any dependencies you want.

Customizing PYPI installation
.............................

You can customize PYPI sources used during image build by adding a ``docker-context-files``/``.pypirc`` file
This ``.pypirc`` will never be committed to the repository and will not be present in the final production image.
It is added and used only in the build segment of the image so it is never copied to the final image.

External sources for dependencies
.................................

In corporate environments, there is often the need to build your Container images using
other than default sources of dependencies. The docker file uses standard sources (such as
Debian apt repositories or PyPI repository. However, in corporate environments, the dependencies
are often only possible to be installed from internal, vetted repositories that are reviewed and
approved by the internal security teams. In those cases, you might need to use those different
sources.

This is rather easy if you extend the image - you simply write your extension commands
using the right sources - either by adding/replacing the sources in apt configuration or
specifying the source repository in pip install command.

It's a bit more involved in the case of customizing the image. We do not have yet (but we are working
on it) a capability of changing the sources via build args. However, since the builds use
Dockerfile that is a source file, you can rather easily simply modify the file manually and
specify different sources to be used by either of the commands.


Comparing extending and customizing the image
---------------------------------------------

Here is the comparison of the two types of building images.

+----------------------------------------------------+---------------------+-----------------------+
|                                                    | Extending the image | Customizing the image |
+====================================================+=====================+=======================+
| Produces optimized image                           | No                  | Yes                   |
+----------------------------------------------------+---------------------+-----------------------+
| Use Airflow Dockerfile sources to build the image  | No                  | Yes                   |
+----------------------------------------------------+---------------------+-----------------------+
| Requires Airflow sources                           | No                  | Yes                   |
+----------------------------------------------------+---------------------+-----------------------+
| You can build it with Breeze                       | No                  | Yes                   |
+----------------------------------------------------+---------------------+-----------------------+
| Allows to use non-default sources for dependencies | Yes                 | No [1]                |
+----------------------------------------------------+---------------------+-----------------------+

[1] When you combine customizing and extending the image, you can use external sources
in the "extend" part. There are plans to add functionality to add external sources
option to image customization. You can also modify Dockerfile manually if you want to
use non-default sources for dependencies.

More details about the images
-----------------------------

You can read more details about the images - the context, their parameters and internal structure in the
`IMAGES.rst <https://github.com/apache/airflow/blob/master/IMAGES.rst>`_ document.
