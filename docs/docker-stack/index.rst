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

 .. WARNING:
    IF YOU ARE UPDATING THIS FILE, CONSIDER UPDATING README.MD TOO.

.. image:: /img/docker-logo.png
    :width: 100

Docker Image for Apache Airflow
===============================

.. toctree::
    :hidden:

    Home <self>
    build
    entrypoint
    changelog
    recipes

.. toctree::
    :hidden:
    :caption: References

    build-arg-ref

For the ease of deployment in production, the community releases a production-ready reference container
image.


The Apache Airflow community, releases Docker Images which are ``reference images`` for Apache Airflow.
Every time a new version of Airflow is released, the images are prepared in the
`apache/airflow DockerHub <https://hub.docker.com/r/apache/airflow>`_
for all the supported Python versions.

You can find the following images there (Assuming Airflow version :subst-code:`|airflow-version|`):

* :subst-code:`apache/airflow:latest`              - the latest released Airflow image with default Python version (3.8 currently)
* :subst-code:`apache/airflow:latest-pythonX.Y`    - the latest released Airflow image with specific Python version
* :subst-code:`apache/airflow:|airflow-version|`           - the versioned Airflow image with default Python version (3.8 currently)
* :subst-code:`apache/airflow:|airflow-version|-pythonX.Y` - the versioned Airflow image with specific Python version

Those are "reference" regular images. They contain the most common set of extras, dependencies and providers that are
often used by the users and they are good to "try-things-out" when you want to just take Airflow for a spin,

You can also use "slim" images that contain only core airflow and are about half the size of the "regular" images
but you need to add all the :doc:`apache-airflow:extra-packages-ref` and providers that you need separately
via :ref:`Building the image <build:build_image>`.

* :subst-code:`apache/airflow:slim-latest`              - the latest released Airflow image with default Python version (3.8 currently)
* :subst-code:`apache/airflow:slim-latest-pythonX.Y`    - the latest released Airflow image with specific Python version
* :subst-code:`apache/airflow:slim-|airflow-version|`           - the versioned Airflow image with default Python version (3.8 currently)
* :subst-code:`apache/airflow:slim-|airflow-version|-pythonX.Y` - the versioned Airflow image with specific Python version

The Apache Airflow image provided as convenience package is optimized for size, and
it provides just a bare minimal set of the extras and dependencies installed and in most cases
you want to either extend or customize the image. You can see all possible extras in :doc:`apache-airflow:extra-packages-ref`.
The set of extras used in Airflow Production image are available in the
`Dockerfile <https://github.com/apache/airflow/blob/2c6c7fdb2308de98e142618836bdf414df9768c8/Dockerfile#L37>`_.

However, Airflow has more than 60 community-managed providers (installable via extras) and some of the
default extras/providers installed are not used by everyone, sometimes others extras/providers
are needed, sometimes (very often actually) you need to add your own custom dependencies,
packages or even custom providers. You can learn how to do it in :ref:`Building the image <build:build_image>`.

The production images are build in DockerHub from released version and release candidates. There
are also images published from branches but they are used mainly for development and testing purpose.
See `Airflow Git Branching <https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#airflow-git-branches>`_
for details.

Fixing images at release time
=============================

The released "versioned" reference images are mostly ``fixed`` when we release Airflow version and we only
update them in exceptional circumstances. For example when we find out that there are dependency errors
that might prevent important Airflow or embedded provider's functionalities working. In normal circumstances,
the images are not going to change after release, even if new version of Airflow dependencies are released -
not even when those versions contain critical security fixes. The process of Airflow releases is designed
around upgrading dependencies automatically where applicable but only when we release a new version of Airflow,
not for already released versions.

If you want to make sure that Airflow dependencies are upgraded to the latest released versions containing
latest security fixes in the image you use, you should implement your own process to upgrade
those yourself when you build custom image based on the Airflow reference one. Airflow usually does not
upper-bound versions of its dependencies via requirements, so you should be able to upgrade them to the
latest versions - usually without any problems. And you can follow the process described in
:ref:`Building the image <build:build_image>` to do it (even in automated way).

Obviously - since we have no control over what gets released in new versions of the dependencies, we
cannot give any guarantees that tests and functionality of those dependencies will be compatible with
Airflow after you upgrade them - testing if Airflow still works with those is in your hands,
and in case of any problems, you should raise issue with the authors of the dependencies that are problematic.
You can also - in such cases - look at the `Airflow issues <https://github.com/apache/airflow/issues>`_
`Airflow Pull Requests <https://github.com/apache/airflow/pulls>`_ and
`Airflow Discussions <https://github.com/apache/airflow/discussions>`_, searching for similar
problems to see if there are any fixes or workarounds found in the ``main`` version of Airflow and apply them
to your custom image.

The easiest way to keep-up with the latest released dependencies is however, to upgrade to the latest released
Airflow version via switching to newly released images as base for your images, when a new version of
Airflow is released. Whenever we release a new version of Airflow, we upgrade all dependencies to the latest
applicable versions and test them together, so if you want to keep up with those tests - staying up-to-date
with latest version of Airflow is the easiest way to update those dependencies.


Support
=======

The reference Docker Image supports the following platforms and database:


* Intel platform (x86_64)
  * Postgres Client
  * MySQL Client
  * MSSQL Client

* ARM platform (aarch64) - experimental support, might change any time
  * Postgres Client
  * MSSQL Client

Usage
=====

The :envvar:`AIRFLOW_HOME` is set by default to ``/opt/airflow/`` - this means that DAGs
are by default in the ``/opt/airflow/dags`` folder and logs are in the ``/opt/airflow/logs``

The working directory is ``/opt/airflow`` by default.

If no :envvar:`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` variable is set then SQLite database is created in
``${AIRFLOW_HOME}/airflow.db``.

For example commands that start Airflow see: :ref:`entrypoint:commands`.

Airflow requires many components to function as it is a distributed application. You may therefore also be interested
in launching Airflow in the Docker Compose environment, see: :doc:`apache-airflow:howto/docker-compose/index`.

You can use this image in :doc:`Helm Chart <helm-chart:index>` as well.
