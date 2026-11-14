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

.. _image-legacy-flavor:

Legacy images
=============

.. warning::

   The ``legacy`` image flavor is deprecated and will be removed in Airflow 3.5.0. Use the default
   images described in :doc:`build` instead.

Since Airflow 3.4.0 the images are built on the Docker Hardened Image for Python on Debian 13 "trixie"
(see :ref:`image-hardened-base`). For the 3.4.x line the ``Dockerfile`` can still build ``legacy``
images, the way they were built before 3.4.0: Python downloaded from python.org, verified with its
sigstore signature and compiled from sources on ``debian:bookworm-slim``.

.. note::

   The community does not publish ``legacy`` images - there are no ``-legacy`` tags in the
   `DockerHub of Apache Airflow <https://hub.docker.com/r/apache/airflow>`_. If you need one,
   build it yourself from the ``Dockerfile`` of your Airflow version.

You can build one with the ``AIRFLOW_IMAGE_FLAVOR`` build argument:

.. code-block:: bash

    docker build . \
      --build-arg AIRFLOW_IMAGE_FLAVOR="legacy" \
      --build-arg BASE_IMAGE="debian:bookworm-slim" \
      --build-arg AIRFLOW_PYTHON_VERSION="3.13" \
      --tag my-image:my-tag

A ``MAJOR.MINOR`` ``AIRFLOW_PYTHON_VERSION`` builds the latest python.org release of that Python line;
pass ``MAJOR.MINOR.PATCH`` to build a specific one.

The flavor an image was built with is recorded in its ``org.apache.airflow.image.flavor`` label.

If your image relies on something that changed with the hardened base or with Debian trixie, see
:ref:`image-hardened-base` and :ref:`image-debian-trixie` for what to adapt, so that you can move to
the default images before the ``legacy`` flavor is removed.
