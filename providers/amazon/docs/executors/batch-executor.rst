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


.. |executorName| replace:: Batch
.. |dockerfileLink| replace:: `here <https://github.com/apache/airflow/blob/main/providers/amazon/src/airflow/providers/amazon/aws/executors/Dockerfile>`__
.. |configKwargs| replace:: SUBMIT_JOB_KWARGS

==================
AWS Batch Executor
==================

This is an Airflow executor powered by Amazon Batch. Each task scheduled by Airflow is run inside a
separate container, scheduled by Batch. Some benefits of an executor like this include:

1. Scalability and Lower Costs: AWS Batch allows the ability to dynamically provision the resources needed to execute tasks.
   Depending on the resources allocated, AWS Batch can autoscale up or down based on the workload,
   ensuring efficient resource utilization and reducing costs.
2. Job Queues and Priority: AWS Batch provides the concept of job queues, allowing
   the ability to prioritize and manage the execution of tasks. This ensures that when multiple
   tasks are scheduled simultaneously, they are executed in the desired order of priority.
3. Flexibility: AWS Batch supports Fargate (ECS), EC2 and EKS compute environments. This range of
   compute environments, as well as the ability to finely define the resources allocated to
   the compute environments gives a lot of flexibility to users in choosing the most suitable
   execution environment for their workloads.
4. Rapid Task Execution: By maintaining an active worker within AWS Batch, tasks submitted to
   the service can be executed swiftly. With a ready-to-go worker, there's minimal startup delay,
   ensuring tasks commence immediately upon submission. This feature is particularly advantageous
   for time-sensitive workloads or applications requiring near-real-time processing,
   enhancing overall workflow efficiency and responsiveness.

For a quick start guide please see :ref:`here <batch_setup_guide>`, it will
get you up and running with a basic configuration.

The below sections provide more generic details about configuration, the
provided example Dockerfile and logging.

.. _batch_config_options:

Config Options
--------------

There are a number of configuration options available, which can either
be set directly in the airflow.cfg file under an "aws_batch_executor"
section or via environment variables using the
``AIRFLOW__AWS_BATCH_EXECUTOR__<OPTION_NAME>`` format, for example
``AIRFLOW__AWS_BATCH_EXECUTOR__JOB_QUEUE = "myJobQueue"``. For
more information on how to set these options, see `Setting Configuration
Options <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html>`__

.. include:: general.rst
   :start-after: .. BEGIN CONFIG_OPTIONS_PRECEDENCE
   :end-before: .. END CONFIG_OPTIONS_PRECEDENCE

.. note::
   ``executor_config`` is an optional parameter that can be provided to operators. It is a dictionary type and in the context of the Batch Executor it represents a ``submit_job_kwargs`` configuration which is then updated over-top of the ``submit_job_kwargs`` specified in Airflow config above (if present). It is a recursive update which essentially applies Python update to each nested dictionary in the configuration. Loosely approximated as: ``submit_job_kwargs.update(executor_config)``

Required config options:
~~~~~~~~~~~~~~~~~~~~~~~~

-  JOB_QUEUE - The job queue where the job is submitted. Required.
-  JOB_DEFINITION - The job definition used by this job. Required.
-  JOB_NAME - The name of the AWS Batch Job. Required.
-  REGION_NAME - The name of the AWS Region where Amazon Batch is configured.
   Required.

Optional config options:
~~~~~~~~~~~~~~~~~~~~~~~~

-  AWS_CONN_ID - The Airflow connection (i.e. credentials) used by the Batch
   executor to make API calls to AWS Batch. Defaults to "aws_default".
-  SUBMIT_JOB_KWARGS - A JSON string containing arguments to provide the
   Batch ``submit_job`` API.
-  MAX_SUBMIT_JOB_ATTEMPTS - The maximum number of times the Batch Executor
   should attempt to submit a job. This refers to instances where the job
   fails to start (i.e. API failures, container failures etc.)
-  CHECK_HEALTH_ON_STARTUP - Whether or not to check the Batch Executor
   health on startup

For a more detailed description of available options, including type
hints and examples, see the ``config_templates`` folder in the Amazon
provider package.

.. note::
   ``executor_config`` is an optional parameter that can be provided to operators. It is a dictionary type and in the context of the Batch Executor it represents a ``submit_job_kwargs`` configuration which is then updated over-top of the ``submit_job_kwargs`` specified in Airflow config above (if present). It is a recursive update which essentially applies Python update to each nested dictionary in the configuration. Loosely approximated as: ``submit_job_kwargs.update(executor_config)``

.. _dockerfile_for_batch_executor:

.. include:: general.rst
   :start-after: .. BEGIN DOCKERFILE
   :end-before: .. END DOCKERFILE


The most secure method is to use IAM roles. When creating an AWS Batch Job
Definition, you are able to select a Job Role and an Execution
Role. The Execution Role is the role that is used by the container
agent to make AWS API requests on your behalf. Depending on what compute is being
used by the Batch Executor, the appropriate policy needs to be attached to the Execution Role.
Additionally, the role also needs to have at least the ``CloudWatchLogsFullAccess``
(or ``CloudWatchLogsFullAccessV2``) policies. The Job Role is the role that is
used by the containers to make AWS API requests. This role needs to have
permissions based on the tasks that are described in the Dag being run.
If you are loading Dags via an S3 bucket, this role needs to have
permission to read the S3 bucket.

To create a new Job Role or Execution Role, follow the steps
below:

1. Navigate to the IAM page on the AWS console, and from the left hand
   tab, under Access Management, select Roles.
2. On the Roles page, click Create role on the top right hand corner.
3. Under Trusted entity type, select AWS Service.
4. Select applicable use case.
5. In the Permissions page, select the permissions the role will need,
   depending on whether the role is a Job Role or an Execution
   Role. Click Next after selecting all the required permissions.
6. Enter a name for the new role, and an optional description. Review
   the Trusted Entities, and the permissions for the role. Add any tags
   as necessary, and click Create role.

When creating the Job Definition for Batch (see the :ref:`setup guide <batch_setup_guide>` for more details), select the appropriate
newly created Job Role and  Execution role for the Job Definition.

.. include:: general.rst
   :start-after: .. BEGIN DOCKERFILE_AUTH_SECOND_METHOD
   :end-before: .. END DOCKERFILE_AUTH_SECOND_METHOD

.. include:: general.rst
  :start-after: .. BEGIN BASE_IMAGE
  :end-before: .. END BASE_IMAGE

.. include:: general.rst
  :start-after: .. BEGIN LOADING_DAGS_OVERVIEW
  :end-before: .. END LOADING_DAGS_OVERVIEW

.. include:: general.rst
  :start-after: .. BEGIN LOADING_DAGS_FROM_S3
  :end-before: .. END LOADING_DAGS_FROM_S3

.. include:: general.rst
  :start-after: .. BEGIN LOADING_DAGS_FROM_LOCAL
  :end-before: .. END LOADING_DAGS_FROM_LOCAL

.. include:: general.rst
  :start-after: .. BEGIN DEPENDENCIES
  :end-before: .. END DEPENDENCIES

Building Image for AWS Batch Executor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Detailed instructions on how to use the Docker image, that you have
created via this readme, with the Batch Executor can be found
:ref:`here <batch_setup_guide>`.

.. _batch_logging:

.. include:: general.rst
   :start-after: .. BEGIN LOGGING
   :end-before: .. END LOGGING


-  The configuration options for Airflow remote logging should be
   configured on all hosts and containers running Airflow. For example
   the Webserver requires this config so that it can fetch logs from
   the remote location and the containers run by |executorName| Executor require the config so that
   they can upload the logs to the remote location. See
   `here <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-config.html>`__
   to read more about how to set Airflow configuration via config file
   or environment variable exports.
-  Adding the Airflow remote logging config to the container can be done
   in many ways. Some examples include, but are not limited to:

   -  Exported as environment variables directly in the Dockerfile (see
      the Dockerfile section :ref:`above <dockerfile_for_batch_executor>`)
   -  Updating the ``airflow.cfg`` file or copy/mounting/downloading a
      custom ``airflow.cfg`` in the Dockerfile.
   -  Added in the Job Definition as an environment variable

-  You must have credentials configured within the container to be able
   to interact with the remote service for your logs (e.g. S3,
   CloudWatch Logs, etc). This can be done in many ways. Some examples
   include, but are not limited to:

   -  Export credentials into the Dockerfile directly (see the
      Dockerfile section :ref:`above <dockerfile_for_batch_executor>`)
   -  Configure an Airflow Connection and provide this as the `remote
      logging conn
      id <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#remote-log-conn-id>`__
      (exported into the container by any of the means listed above or
      your preferred method). Airflow will then use these credentials
      *specifically* for interacting with your chosen remote logging
      destination.

.. note::
   Configuration options must be consistent across all the hosts/environments running the Airflow components (Scheduler, Webserver, Executor managed resources, etc). See `here <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html>`__ for more details on setting configurations.

.. _batch_setup_guide:


Setting up a Batch Executor for Apache Airflow
----------------------------------------------

There are 3 steps involved in getting a Batch Executor to work in Apache Airflow:

1. Creating a database that Airflow and the tasks executed by Batch can connect to.

2. Creating and configuring Batch resources that can run tasks from Airflow.

3. Configuring Airflow to use the Batch Executor and the database.

There are different options for selecting a database backend. See `here <https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html>`_ for more information about the different options supported by Airflow. The following guide will explain how to set up a PostgreSQL RDS Instance on AWS.


.. include:: general.rst
   :start-after: .. BEGIN DATABASE_CONNECTION
   :end-before: .. END DATABASE_CONNECTION

Setting up AWS Batch
--------------------
AWS Batch can be configured in various ways, with differing orchestration types depending on the use case.
For simplicity, this guide will cover how to set up Batch with EC2.

In order to set up AWS Batch so that it will work with Apache Airflow, you will need a Docker image that is properly configured. See the :ref:`Dockerfile <dockerfile_for_batch_executor>` section for instructions on how to do that.

Once the image is built, it needs to be put in a repository where it can be pulled by a container. There are multiple ways to accomplish this. This guide will go over doing this using Amazon Elastic Container Registry (ECR).

.. include:: general.rst
   :start-after: .. BEGIN ECR_STEPS
   :end-before: .. END ECR_STEPS


Configuring AWS Batch
~~~~~~~~~~~~~~~~~~~~~

1. Log in to your AWS Management Console and navigate to the AWS Batch landing page.

2. On the left hand side bar, click Wizard. This Wizard will guide you to creating all the
   required resources to run Batch jobs.

3. Select the orchestration as Amazon EC2.

4. Click Next.

Create a Compute Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Choose a name for the compute environment, tags and any appropriate instance configuration.
   Here, you may select the minimum, maximum and desired number of vCPU's, as well as the type of
   EC2 instances you would like to use.

2. For Instance Role, choose to create a new instance profile or use an existing instance profile
   that has the required IAM permissions attached. This instance profile allows the Amazon ECS
   container instances that are created for your compute environment to make calls to the
   required AWS API operations on your behalf.

3. Select a VPC which allows access to internet, as well as a security group with the necessary permissions.

4. Click Next.

Create a Job Queue
~~~~~~~~~~~~~~~~~~

1. Select a name for the job queue, as well as a priority. The compute environment will be set to the one created in the previous step.



Create a Job Definition
~~~~~~~~~~~~~~~~~~~~~~~

1. Choose a name for the Job Definition.

2. Select the appropriate platform configurations. Ensure ``Assign public IP`` is enabled.

3. Select an Execution Role, and ensure the role has the required permissions to accomplish its tasks.

4. Enter the image URI of the image that was pushed in the previous section. Make sure the role being used has the required permissions to pull the image.

5. Select an appropriate Job role, keeping in mind the requirements of the tasks being run.

6. Configure the environment as required. You may specify the number of vCPU's, memory or GPUs available to the container.
   Additionally, add the following environment variables to the container:

- ``AIRFLOW__DATABASE__SQL_ALCHEMY_CONN``, with the value being the PostgreSQL connection string in the following format using the values set during the `Database section <#create-the-rds-db-instance>`_ above:

.. code-block:: bash

   postgresql+psycopg2://<username>:<password>@<endpoint>/<database_name>

7. Add other configuration as necessary for Airflow generally (see `here <https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html>`__), the Batch executor (see :ref:`here <config-options>`) or for remote logging (see :ref:`here <logging>`). Note that any configuration changes should be made across the entire Airflow environment to keep configuration consistent.

8. Click Next.

9. In Review and Create page, review all the selections, and once everything is correct, click Create Resources.

Allow Containers to Access RDS Database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As a final step, access to the database must be configured for the containers managed by Batch. Many different networking configurations are possible, but one possible approach is:

1. Log in to your AWS Management Console and navigate to the VPC Dashboard.

2. On the left hand, under the Security heading, click Security groups.

3. Select the security group associated with your RDS instance, and click Edit inbound rules.

4. Add a new rule that allows PostgreSQL type traffic to the CIDR of the subnet(s) associated with the Batch Compute Environment.

Configure Airflow
~~~~~~~~~~~~~~~~~

To configure Airflow to utilize the Batch Executor and leverage the resources we've set up, ensure the following environment variables are defined:

.. code-block:: bash

   AIRFLOW__CORE__EXECUTOR='airflow.providers.amazon.aws.executors.batch.batch_executor.AwsBatchExecutor'

   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=<postgres-connection-string>

   AIRFLOW__AWS_BATCH_EXECUTOR__REGION_NAME=<executor-region>

   AIRFLOW__AWS_BATCH_EXECUTOR__JOB_QUEUE=<batch-job-queue>

   AIRFLOW__AWS_BATCH_EXECUTOR__JOB_DEFINITION=<batch-job-definition>

   AIRFLOW__AWS_BATCH_EXECUTOR__JOB_NAME=<batch-job-name>

.. include:: general.rst
   :start-after: .. BEGIN INIT_DB
   :end-before: .. END INIT_DB
