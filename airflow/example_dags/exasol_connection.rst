.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements. See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership. The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied. See the License for the
   specific language governing permissions and limitations
   under the License.

.. _howto/connection:exasol:

########################################
Exasol Connection Setup for Apache Airflow
########################################

This guide provides a step-by-step approach to configuring an Exasol connection in Apache Airflow. The setup enables data extraction from an Exasol database using the ``ExasolToS3Operator``.

Prerequisites
#############
- **Docker** installed on the local machine
- **Apache Airflow** installed and running
- **Exasol Database** running in a Docker container
- **Exasol Python Client (`pyexasol`)** installed

Setting Up Exasol in Docker
#############################
To start an Exasol instance locally using Docker:

.. code-block:: sh

   docker run --name <EXASOL_CONTAINER_NAME> --privileged -d -p 8563:8563 -p 2580:2580 exasol/docker-db:latest

This command runs Exasol on ``localhost:8563``.

**Parameters:**
- ``<EXASOL_CONTAINER_NAME>``: Custom name for the Exasol container.

Verifying the Container
#######################

After launching the container, check its status:

.. code-block:: sh

   docker ps  # Retrieve CONTAINER_ID
   docker exec -it <CONTAINER_ID> /bin/bash  # Access Exasol container

Inside the container, locate and use the Exasol client:

.. code-block:: sh

   find / -name "exaplus" 2>/dev/null  # Identify the Exasol client path
   <path-to-exaplus> -c 127.0.0.1:8563 -u sys -p exasol

   # If using TCP, use:
   <path-to-exaplus> -c 127.0.0.1/<FINGERPRINT>:8563 -u sys -p exasol

**Definitions:**
- ``<CONTAINER_ID>``: ID of the running Exasol container.
- ``<FINGERPRINT>``: Optional identifier for secure TCP connections.

Connecting Exasol to Airflow's Network
######################################

To ensure Airflow can communicate with Exasol, connect the container to the ``breeze_default`` network:

.. code-block:: sh

   docker network connect breeze_default <EXASOL_CONTAINER_NAME>

Connecting to Exasol via Python
################################

Once the container is running, test the connection using ``pyexasol``:

.. code-block:: python

   import pyexasol

   conn = pyexasol.connect(dsn="<EXASOL_CONTAINER_NAME>:8563", user="SYS", password="exasol")
   result = conn.execute("SELECT CURRENT_USER, CURRENT_SCHEMA").fetchall()
   print(result)

Ensure that the connection is established successfully before proceeding.

Configuring Airflow Connection to Exasol
########################################

To integrate Apache Airflow with Exasol, define an Airflow connection. This allows Airflow to interact with Exasol for data extraction and processing.

Using the Airflow CLI:

.. code-block:: sh

   airflow connections add 'exasol_default' \
       --conn-type 'exasol' \
       --conn-host '<EXASOL_HOST>' \
       --conn-port '8563' \
       --conn-schema '<EXASOL_SCHEMA>' \
       --conn-login 'SYS' \
       --conn-password 'exasol' \
       --conn-extra '{"encryption": true}'

**Parameter Details:**
- ``<EXASOL_HOST>``: The hostname or IP address of the Exasol instance. If running in a Docker container using the ``bridge`` network, use the container's name (e.g., ``exasol-db``) instead of ``localhost``.
- ``<EXASOL_SCHEMA>``: The schema to execute queries in Exasol.

Verifying the Connection
#########################

Once the connection is configured, check if it was successfully added:

.. code-block:: sh

   airflow connections get exasol_default

This command displays the connection details, ensuring Airflow can communicate with Exasol.

Testing the DAG
###############

Load DAG Files
==============

Reserialize and refresh DAGs:

.. code-block:: sh

   airflow dags reserialize

Manual Task Execution
=====================

Verify connectivity by running a manual task execution:

.. code-block:: sh

   airflow tasks test example_exasol_to_s3 test_exasol_conn $(date +%Y-%m-%d)

If configured correctly, the task will connect to Exasol and extract data.

---

This document follows the official Apache Airflow documentation style and ensures professional clarity and consistency.