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

.. _build:build_image:

Why build your own image
========================

The reference images that the community publishes are a good way to try Airflow out, but in
production you should run your own image: one that contains exactly the providers and dependencies
your Dags need, built once and versioned. Installing packages every time a container starts is
fragile and slow - every container pays for the installation again, and a new release of a package can
break the start of your deployment. Keep dynamic installation for quick experiments only.

Airflow has more than 100 community-managed providers (installable via extras) and some of the
default extras/providers installed are not used by everyone, sometimes others extras/providers
are needed, sometimes (very often actually) you need to add your own custom dependencies,
packages or even custom providers. You can see all possible extras in :doc:`apache-airflow:extra-packages-ref`.
The set of extras used in Airflow Production image are available in the
`Dockerfile <https://github.com/apache/airflow/blob/|airflow-version|/Dockerfile>`__.

The standard ways of building your own image need nothing but ``docker`` and a short ``Dockerfile`` or
``docker build`` command, and they produce small images suitable for production:

* :doc:`build` - choose the right approach and build a small production image in a few steps.
* :doc:`build-extending` - the rules and more examples for extending the published images.
* :doc:`build-customizing` - building the image with the ``Dockerfile`` and its build arguments.
* :doc:`build-advanced` - custom package sources, restricted, air-gapped and FIPS environments, forks
  of Airflow and the details of the base image, starting with a guide to which approach to follow.
* :doc:`build-publishing` - building, testing and using your image in your deployment.
* :doc:`legacy-images` - the deprecated ``legacy`` images, built the way the images were built before
  Airflow 3.4.0. For the 3.4.x line they are not published, but you can build them yourself.

.. note::
   Creating custom images means that you also need to maintain the automation that re-creates them
   when the packages you install or Airflow itself are upgraded. In cases when you run pure Python
   tasks, you can also use the
   `Python Virtualenv functions <https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#pythonvirtualenvoperator>`_
   which install Python dependencies dynamically at runtime.
