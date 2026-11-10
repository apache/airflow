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

.. _build:publishing:

Publishing the image
====================

Once your ``Dockerfile`` or ``docker build`` command is ready - see :doc:`build` - build the image,
test it and make it available to your deployment.

1. Build the image with ``docker`` (or another tool such as ``podman`` or ``kaniko``), tagging it with
   your own name and version. If you publish it to a registry, the name usually has the form
   ``registry/image-name``:

   .. code-block:: shell

      docker build . -f Dockerfile --pull --tag my-image:0.0.1

2. Optionally, test the image. If you have Airflow sources checked out, run this from their root folder;
   the output tells you whether the image is good to go:

   .. code-block:: shell

      ./scripts/ci/tools/verify_docker_image.sh PROD my-image:0.0.1

3. Make the image available to your deployment:

   * With :doc:`apache-airflow:howto/docker-compose/index`, set the ``AIRFLOW_IMAGE_NAME`` variable to
     your image, or let Docker Compose build it: in ``docker-compose.yaml``, search for "In order to add
     custom dependencies", comment the ``image`` line out, uncomment the ``build`` line and run
     ``docker compose build``.
   * Development Kubernetes clusters such as ``kind`` or ``minikube`` can load a local image directly
     with their ``load`` commands.
   * In all other cases, push the image to a registry and set its name in your deployment - for example
     in the :doc:`helm-chart:index`.
