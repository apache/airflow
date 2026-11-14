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

Image variants
==============

The following images are published in the `DockerHub of Apache Airflow <https://hub.docker.com/r/apache/airflow>`_ (assuming Airflow version :subst-code:`|airflow-version|`):

* :subst-code:`apache/airflow:latest`              - the latest released Airflow image with default Python version (3.14 currently)
* :subst-code:`apache/airflow:latest-pythonX.Y`    - the latest released Airflow image with specific Python version
* :subst-code:`apache/airflow:|airflow-version|`           - the versioned Airflow image with default Python version (3.14 currently)
* :subst-code:`apache/airflow:|airflow-version|-pythonX.Y` - the versioned Airflow image with specific Python version

Those are "reference" regular images. They contain the most common set of extras, dependencies and providers that are
often used by the users and they are good to "try-things-out" when you want to just take Airflow for a spin,

You can also use "slim" images that contain only core Airflow and are about half the size of the "regular" images.
They are the recommended starting point for production: you add only the
:doc:`apache-airflow:extra-packages-ref` and providers that you need, as shown in
:ref:`a small production image <build:small-production-image>`.

* :subst-code:`apache/airflow:slim-latest`              - the latest released Airflow image with default Python version (3.14 currently)
* :subst-code:`apache/airflow:slim-latest-pythonX.Y`    - the latest released Airflow image with specific Python version
* :subst-code:`apache/airflow:slim-|airflow-version|`           - the versioned Airflow image with default Python version (3.14 currently)
* :subst-code:`apache/airflow:slim-|airflow-version|-pythonX.Y` - the versioned Airflow image with specific Python version

.. note:: Database providers in slim images

    Slim images come with the database clients preinstalled, but the default providers do not include
    any database provider. Install the provider for your database (for example
    ``apache-airflow-providers-postgres``) yourself.

Naming conventions for the images:

+----------------+--------------------------+---------------------------------+--------------------------------------+
| Image          | Python                   | Standard image                  | Slim image                           |
+================+==========================+=================================+======================================+
| Latest default | 3.14                     | apache/airflow:latest           | apache/airflow:slim-latest           |
+----------------+--------------------------+---------------------------------+--------------------------------------+
| Default        | 3.14                     | apache/airflow:X.Y.Z            | apache/airflow:slim-X.Y.Z            |
+----------------+--------------------------+---------------------------------+--------------------------------------+
| Latest         | 3.11,3.12,3.13,3.14      | apache/airflow:latest-pythonN.M | apache/airflow:slim-latest-pythonN.M |
+----------------+--------------------------+---------------------------------+--------------------------------------+
| Specific       | 3.11,3.12,3.13,3.14      | apache/airflow:X.Y.Z-pythonN.M  | apache/airflow:slim-X.Y.Z-pythonN.M  |
+----------------+--------------------------+---------------------------------+--------------------------------------+

The "latest" image is always the latest released stable version. Pin a specific ``X.Y.Z`` version in
your own images, so that rebuilding them does not upgrade Airflow by surprise.

.. spelling:word-list::

     pythonN
