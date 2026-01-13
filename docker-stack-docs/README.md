<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->
<!--
This file was created for the purpose of publishing an image to ArtifactHub.
Please try to keep it in sync with index.rst
-->

# Docker Image for Apache Airflow

For the ease of deployment in production, the community releases a production-ready reference container
image.

The Apache Airflow community, releases Docker Images which are `reference images` for Apache Airflow.
Every time a new version of Airflow is released, the images are prepared in the
[apache/airflow DockerHub](https://hub.docker.com/r/apache/airflow)
for all the supported Python versions.

You can find the following images there (Assuming Airflow version `3.2.0`):

* `apache/airflow:latest` - the latest released Airflow image with default Python version (3.12 currently)
* `apache/airflow:latest-pythonX.Y` - the latest released Airflow image with specific Python version
* `apache/airflow:3.2.0` - the versioned Airflow image with default Python version (3.12 currently)
* `apache/airflow:3.2.0-pythonX.Y` - the versioned Airflow image with specific Python version

Those are "reference" regular images. They contain the most common set of extras, dependencies and providers that are
often used by the users and they are good to "try-things-out" when you want to just take Airflow for a spin,

You can also use "slim" images that contain only core Airflow and are about half the size of the "regular" images
but you need to add all the [Reference for package extras](https://airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html) and providers that you need separately
via [Building the image](https://airflow.apache.org/docs/docker-stack/build.html#build-build-image).

* `apache/airflow:slim-latest`              - the latest released Airflow image with default Python version (3.12 currently)
* `apache/airflow:slim-latest-pythonX.Y`    - the latest released Airflow image with specific Python version
* `apache/airflow:slim-3.2.0`           - the versioned Airflow image with default Python version (3.12 currently)
* `apache/airflow:slim-3.2.0-pythonX.Y` - the versioned Airflow image with specific Python version

The Apache Airflow image provided as convenience package is optimized for size, and
it provides just a bare minimal set of the extras and dependencies installed and in most cases
you want to either extend or customize the image. You can see all possible extras in [Reference for package extras](https://airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html).
The set of extras used in Airflow Production image are available in the
[Dockerfile](https://github.com/apache/airflow/blob/2c6c7fdb2308de98e142618836bdf414df9768c8/Dockerfile#L37).

However, Airflow has more than 60 community-managed providers (installable via extras) and some of the
default extras/providers installed are not used by everyone, sometimes others extras/providers
are needed, sometimes (very often actually) you need to add your own custom dependencies,
packages or even custom providers. You can learn how to do it in [Building the image](https://airflow.apache.org/docs/docker-stack/build.html#build-build-image).

The production images are built in DockerHub from released version and release candidates. There
are also images published from branches but they are used mainly for development and testing purpose.
See [Airflow Git Branching](https://github.com/apache/airflow/blob/main/contributing-docs/10_working_with_git.rst#airflow-git-branches)
for details.

## Usage

The [`AIRFLOW_HOME`](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#envvar-AIRFLOW_HOME) is set by default to ``/opt/airflow/`` - this means that dags
are in default in the ``/opt/airflow/dags`` folder and logs are in the ``/opt/airflow/logs``

The working directory is ``/opt/airflow`` by default.

If no `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` variable is set then SQLite database is created in
``${AIRFLOW_HOME}/airflow.db``.

For example commands that start Airflow see: [Executing commands](https://airflow.apache.org/docs/docker-stack/entrypoint.html#entrypoint-commands).

Airflow requires many components to function as it is a distributed application. You may therefore also be interested
in launching Airflow in the Docker Compose environment, see: [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

You can use this image in [Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html) as well.
