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

Manually building and testing release candidate packages
========================================================

Breeze can be used to test new release candidates of packages - both Airflow and providers. You can easily
turn the CI image of Breeze to install and start Airflow for both Airflow and provider packages - both,
packages that are built from sources and packages that are downloaded from PyPI when they are released
there as release candidates.

.. contents:: :local:

Prerequisites
-------------

The way to test it is rather straightforward:

1) Make sure that the packages - both ``airflow`` and ``providers`` are placed in the ``dist`` folder
   of your Airflow source tree. You can either build them there or download from PyPI (see the next chapter)

2) You can run ```breeze shell`` or ``breeze start-airflow`` commands with adding the following flags -
   ``--mount-sources remove`` and ``--use-packages-from-dist``. The first one removes the ``airflow``
   source tree from the container when starting it, the second one installs ``airflow`` and ``providers``
   packages from the ``dist`` folder when entering breeze.

Testing pre-release packages
----------------------------

There are two ways how you can get Airflow packages in ``dist`` folder - by building them from sources or
downloading them from PyPI.

.. note ::

    Make sure you run ``rm dist/*`` before you start building packages or downloading them from PyPI because
    the packages built there already are not removed manually.

In order to build apache-airflow from sources, you need to run the following command:

.. code-block:: bash

    breeze release-management prepare-airflow-package

In order to build providers from sources, you need to run the following command:

.. code-block:: bash

    breeze release-management prepare-provider-packages <PROVIDER_1> <PROVIDER_2> ... <PROVIDER_N>

The packages are built in ``dist`` folder and the command will summarise what packages are available in the
``dist`` folder after it finishes.

If you want to download the packages from PyPI, you need to run the following command:

.. code-block:: bash

    pip download apache-airflow-providers-<PROVIDER_NAME>==X.Y.Zrc1 --dest dist --no-deps

You can use it for both release and pre-release packages.

Examples of testing pre-release packages
----------------------------------------

Few examples below explain how you can test pre-release packages, and combine them with locally build
and released packages.

The following example downloads ``apache-airflow`` and ``celery`` and ``kubernetes`` provider packages from PyPI and
eventually starts Airflow with the Celery Executor. It also loads example dags and default connections:

.. code:: bash

    rm dist/*
    pip download apache-airflow==2.7.0rc1 --dest dist --no-deps
    pip download apache-airflow-providers-cncf-kubernetes==7.4.0rc1 --dest dist --no-deps
    pip download apache-airflow-providers-cncf-kubernetes==3.3.0rc1 --dest dist --no-deps
    breeze start-airflow --mount-sources remove --use-packages-from-dist --executor CeleryExecutor --load-default-connections --load-example-dags


The following example downloads ``celery`` and ``kubernetes`` provider packages from PyPI, builds
``apache-airflow`` package from the main sources and eventually starts Airflow with the Celery Executor.
It also loads example dags and default connections:

.. code:: bash

    rm dist/*
    breeze release-management prepare-airflow-package
    pip download apache-airflow-providers-cncf-kubernetes==7.4.0rc1 --dest dist --no-deps
    pip download apache-airflow-providers-cncf-kubernetes==3.3.0rc1 --dest dist --no-deps
    breeze start-airflow --mount-sources remove --use-packages-from-dist --executor CeleryExecutor --load-default-connections --load-example-dags

The following example builds ``celery``, ``kubernetes`` provider packages from PyPI, downloads 2.6.3 version
of ``apache-airflow`` package from PyPI and eventually starts Airflow using default executor
for the backend chosen (no example dags, no default connections):

.. code:: bash

    rm dist/*
    pip download apache-airflow==2.6.3 --dest dist --no-deps
    breeze release-management prepare-provider-packages celery cncf.kubernetes
    breeze start-airflow --mount-sources remove --use-packages-from-dist

You can mix and match packages from PyPI (final or pre-release candidates) with locally build packages. You
can also choose which providers to install this way since the ``--remove-sources`` flag makes sure that Airflow
installed does not contain all the providers - only those that you explicitly downloaded or built in the
``dist`` folder. This way you can test all the combinations of Airflow + Providers you might need.

-----

For other kinds of tests look at `Testing document <../09_testing.rst>`__
