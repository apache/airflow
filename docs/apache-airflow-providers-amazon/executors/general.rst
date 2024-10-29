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

.. |executorName| replace:: executor name
.. |dockerfileLink| replace:: Dockerfile link
.. |configKwargs| replace:: config keyword arguments
.. BEGIN CONFIG_OPTIONS_PRECEDENCE
.. note::
   Configuration options must be consistent across all the hosts/environments running the Airflow components (Scheduler, Webserver, Executor managed resources, etc). See `here <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html>`__ for more details on setting configurations.

In the case of conflicts, the order of precedence from lowest to highest is:

1. Load default values for options which have defaults.
2. Load any values explicitly provided through airflow.cfg or
   environment variables. These are checked with Airflow's config
   precedence.
3. Load any values provided in the |configKwargs| option if one is
   provided.

.. END CONFIG_OPTIONS_PRECEDENCE

.. BEGIN DOCKERFILE

Dockerfile for AWS |executorName| Executor
------------------------------------------

An example Dockerfile can be found |dockerfileLink|, it creates an
image that can be used by AWS |executorName| to run Airflow tasks using
the AWS |executorName| Executor in Apache Airflow. The image supports AWS CLI/API
integration, allowing you to interact with AWS services within your
Airflow environment. It also includes options to load Dags (Directed
Acyclic Graphs) from either an S3 bucket or a local folder.


Prerequisites
~~~~~~~~~~~~~

Docker must be installed on your system. Instructions for installing
Docker can be found `here <https://docs.docker.com/get-docker/>`__.



Building an Image
~~~~~~~~~~~~~~~~~

The `AWS CLI <https://aws.amazon.com/cli/>`__ will be installed within the
image, and there are multiple ways to pass AWS authentication
information to the container and thus multiple ways to build the image.
This guide will cover 2 methods.
.. END DOCKERFILE



.. BEGIN DOCKERFILE_AUTH_SECOND_METHOD

Then you can build your image by ``cd``-ing to the directory with the Dockerfile and running:

.. code-block:: bash

   docker build -t my-airflow-image \
    --build-arg aws_default_region=YOUR_DEFAULT_REGION .

Note: It is important that images are built and run under the same architecture. For example,
for users on Apple Silicon, you may want to specify the arch using ``docker buildx``:

.. code-block:: bash

  docker buildx build --platform=linux/amd64 -t my-airflow-image \
    --build-arg aws_default_region=YOUR_DEFAULT_REGION .

See
`here <https://docs.docker.com/reference/cli/docker/buildx/>`__ for more information
about using ``docker buildx``.

The second method is to use the build-time arguments
(``aws_access_key_id``, ``aws_secret_access_key``,
``aws_default_region``, and ``aws_session_token``).

Note: This method is not recommended for use in production environments,
because user credentials are stored in the container, which may be a
security vulnerability.

To pass AWS authentication information using these arguments, use the
``--build-arg`` option during the Docker build process. For example:

.. code-block:: bash

   docker build -t my-airflow-image \
    --build-arg aws_access_key_id=YOUR_ACCESS_KEY \
    --build-arg aws_secret_access_key=YOUR_SECRET_KEY \
    --build-arg aws_default_region=YOUR_DEFAULT_REGION \
    --build-arg aws_session_token=YOUR_SESSION_TOKEN .

Replace ``YOUR_ACCESS_KEY``, ``YOUR_SECRET_KEY``,
``YOUR_SESSION_TOKEN``, and ``YOUR_DEFAULT_REGION`` with valid AWS
credentials.

Base Image
~~~~~~~~~~

The Docker image is built upon the ``apache/airflow:latest`` image. See
`here <https://hub.docker.com/r/apache/airflow>`__ for more information
about the image.

Important note: The Airflow and python versions in this image must align
with the Airflow and python versions on the host/container which is
running the Airflow scheduler process (which in turn runs the executor).
The Airflow version of the image can be verified by running the
container locally with the following command:

.. code-block:: bash

   docker run <image_name> version

Similarly, the python version of the image can be verified the following
command:

.. code-block:: bash

   docker run <image_name> python --version

Ensure that these versions match the versions on the host/container
which is running the Airflow scheduler process (and thus, the |executorName|
executor.) Apache Airflow images with specific python versions can be
downloaded from the Dockerhub registry, and filtering tags by the
`python
version <https://hub.docker.com/r/apache/airflow/tags?page=1&name=3.8>`__.
For example, the tag ``latest-python3.8`` specifies that the image will
have python 3.8 installed.


Loading Dags
~~~~~~~~~~~~

There are many ways to load Dags on a container managed by |executorName|. This Dockerfile
is preconfigured with two possible ways: copying from a local folder, or
downloading from an S3 bucket. Other methods of loading Dags are
possible as well.

From S3 Bucket
^^^^^^^^^^^^^^

To load Dags from an S3 bucket, uncomment the entrypoint line in the
Dockerfile to synchronize the Dags from the specified S3 bucket to the
``/opt/airflow/dags`` directory inside the container. You can optionally
provide ``container_dag_path`` as a build argument if you want to store
the Dags in a directory other than ``/opt/airflow/dags``.

Add ``--build-arg s3_uri=YOUR_S3_URI`` in the docker build command.
Replace ``YOUR_S3_URI`` with the URI of your S3 bucket. Make sure you
have the appropriate permissions to read from the bucket.

Note that the following command is also passing in AWS credentials as
build arguments.

.. code-block:: bash

   docker build -t my-airflow-image \
    --build-arg aws_access_key_id=YOUR_ACCESS_KEY \
    --build-arg aws_secret_access_key=YOUR_SECRET_KEY \
    --build-arg aws_default_region=YOUR_DEFAULT_REGION \
    --build-arg aws_session_token=YOUR_SESSION_TOKEN \
    --build-arg s3_uri=YOUR_S3_URI .

From Local Folder
^^^^^^^^^^^^^^^^^

To load Dags from a local folder, place your DAG files in a folder
within the docker build context on your host machine, and provide the
location of the folder using the ``host_dag_path`` build argument. By
default, the Dags will be copied to ``/opt/airflow/dags``, but this can
be changed by passing the ``container_dag_path`` build-time argument
during the Docker build process:

.. code-block:: bash

   docker build -t my-airflow-image --build-arg host_dag_path=./dags_on_host --build-arg container_dag_path=/path/on/container .

If choosing to load Dags onto a different path than
``/opt/airflow/dags``, then the new path will need to be updated in the
Airflow config.

Installing Python Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This Dockerfile supports installing Python dependencies via ``pip`` from
a ``requirements.txt`` file. Place your ``requirements.txt`` file in the
same directory as the Dockerfile. If it is in a different location, it
can be specified using the ``requirements_path`` build-argument. Keep in
mind the Docker context when copying the ``requirements.txt`` file.
Uncomment the two appropriate lines in the Dockerfile that copy the
``requirements.txt`` file to the container, and run ``pip install`` to
install the dependencies on the container.

Building Image for AWS |executorName| Executor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Detailed instructions on how to use the Docker image, that you have
created via this readme, with the |executorName| Executor can be found
:ref:`here <setup_guide>`.

.. END DOCKERFILE_AUTH_SECOND_METHOD

.. BEGIN LOGGING

Logging
-------

Airflow tasks executed via this executor run in containers within
the configured VPC. This means that logs are not directly accessible to
the Airflow Webserver and when containers are stopped, after task
completion, the logs would be permanently lost.

Remote logging should be employed when using the |executorName| executor to persist
your Airflow Task logs and make them viewable from the Airflow
Webserver.

Configuring Remote Logging
~~~~~~~~~~~~~~~~~~~~~~~~~~

There are many ways to configure remote logging and several supported
destinations. A general overview of Airflow Task logging can be found
`here <https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/logging-tasks.html>`__.
Instructions for configuring S3 remote logging can be found
`here <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/logging/s3-task-handler.html>`__
and Cloudwatch remote logging
`here <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/logging/cloud-watch-task-handlers.html>`__.
Some important things to point out for remote logging in the context of
the |executorName| executor:
.. END LOGGING


.. BEGIN DATABASE_CONNECTION

Setting up an RDS DB Instance for AWS |executorName| Executor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create the RDS DB Instance
~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Log in to your AWS Management Console and navigate to the RDS service.

2. Click "Create database" to start creating a new RDS instance.

3. Choose the "Standard create" option, and select PostreSQL.

4. Select the appropriate template, availability and durability.

   - NOTE: At the time of this writing, the "Multi-AZ DB **Cluster**" option does not support setting the database name, which is a required step below.
5. Set the DB Instance name, the username and password.

6. Choose the instance configuration, and storage parameters.

7. In the Connectivity section, select Don't connect to an EC2 compute resource

8. Select or create a VPC and subnet, and allow public access to the DB. Select or create security group and select the Availability Zone.

9.  Open the Additional Configuration tab and set the database name to ``airflow_db``.

10. Select other settings as required, and create the database by clicking Create database.


Test Connectivity
~~~~~~~~~~~~~~~~~

In order to be able to connect to the new RDS instance, you need to allow inbound traffic to the database from your IP address.


1. Under the "Security" heading in the "Connectivity & security" tab of the RDS instance, find the link to the VPC security group for your new RDS DB instance.

2. Create an inbound rule that allows traffic from your IP address(es) on TCP port 5432 (PostgreSQL).

3. Confirm that you can connect to the DB after modifying the security group. This will require having ``psql`` installed. Instructions for installing ``psql`` can be found `here <https://www.postgresql.org/download/>`__.

**NOTE**: Be sure that the status of your DB is Available before testing connectivity

.. code-block:: bash

   psql -h <endpoint> -p 5432 -U <username> <db_name>

The endpoint can be found on the "Connectivity and Security" tab, the username (and password) are the credentials used when creating the database.

The db_name should be ``airflow_db`` (unless a different one was used when creating the database.)

You will be prompted to enter the password if the connection is successful.


.. END DATABASE_CONNECTION


.. BEGIN ECR_STEPS

Create an ECR Repository
~~~~~~~~~~~~~~~~~~~~~~~~

1. Log in to your AWS Management Console and navigate to the ECR service.

2. Click Create repository.

3. Name the repository and fill out other information as required.

4. Click Create Repository.

5. Once the repository has been created, click on the repository. Click on the "View push commands" button on the top right.

6. Follow the instructions to push the Docker image, replacing image names as appropriate. Ensure the image is uploaded by refreshing the page once the image is pushed.

.. END ECR_STEPS


.. BEGIN INIT_DB

This script should be run on the host(s) running the Airflow Scheduler and Webserver, before those processes are started.

The script sets environment variables that configure Airflow to use the Batch Executor and provide necessary information for task execution. Any other configuration changes made (such as for remote logging) should be added to this example script to keep configuration consistent across the Airflow environment.

Initialize the Airflow DB
~~~~~~~~~~~~~~~~~~~~~~~~~

The Airflow DB needs to be initialized before it can be used and a user needs to be added for you to log in. The below command adds an admin user (the command will also initialize the DB if it hasn't been already):

.. code-block:: bash

   airflow users create --username admin --password admin --firstname <your first name> --lastname <your last name> --email <your email> --role Admin

.. END INIT_DB
