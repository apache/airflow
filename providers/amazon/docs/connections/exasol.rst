.. _exasol_airflow_setup:

########################################
Exasol Connection Setup for Apache Airflow
########################################

This document provides a concise guide to setting up an Exasol connection in Apache Airflow. The setup enables data extraction from an Exasol database and its export to an AWS S3 bucket using the ``ExasolToS3Operator``.

Prerequisites
#############
- **Docker** installed on the local machine
- **Apache Airflow** installed and running
- **Exasol Database** running in a Docker container
- **Exasol Python Client (`pyexasol`)** installed

Setting Up Exasol in Docker
#############################
To launch an Exasol instance locally using Docker:

.. code-block:: sh

   docker run --name <EXASOL_CONTAINER_NAME> --privileged -d -p 8563:8563 -p 2580:2580 exasol/docker-db:latest

This command runs Exasol on ``localhost:8563``.

**Definitions:**
- ``<EXASOL_CONTAINER_NAME>``: Custom name for the Exasol container.

Verifying the Container
#######################

If the container builds successfully, verify its status:

.. code-block:: sh

   docker ps  # Get CONTAINER_ID
   docker exec -it <CONTAINER_ID> /bin/bash  # Access Exasol container

Once inside the container, find and use the Exasol client:

.. code-block:: sh

   find / -name "exaplus" 2>/dev/null  # Path varies by version
   /opt/exasol/db-<EXASOL_VERSION>/bin/Console/exaplus -c 127.0.0.1:8563 -u sys -p exasol

   # If using TCP, use:
   /opt/exasol/db-<EXASOL_VERSION>/bin/Console/exaplus -c 127.0.0.1/<FINGERPRINT>:8563 -u sys -p exasol

**Definitions:**
- ``<CONTAINER_ID>``: ID of the running Exasol container.
- ``<EXASOL_VERSION>``: Exasol database version inside the container.
- ``<FINGERPRINT>``: Optional fingerprint for secure TCP connections.

Adding Exasol to ``breeze_default`` Network
############################################

To ensure communication between Airflow and Exasol, add the container to the ``breeze_default`` network:

.. code-block:: sh

   docker network connect breeze_default <EXASOL_CONTAINER_NAME>

**Definitions:**
- ``<EXASOL_CONTAINER_NAME>``: Name of the Exasol container.

Connecting to Exasol
######################

After starting the container, access the Breeze container and connect to Exasol using ``pyexasol``:

.. code-block:: python

   import pyexasol

   conn = pyexasol.connect(dsn="exasol-db:8563", user="SYS", password="exasol")
   
   result = conn.execute("SELECT CURRENT_USER, CURRENT_SCHEMA").fetchall()
   print(result)

Ensure that the connection is successful before proceeding.

Creating a Sample Dataset
###########################

To create a new schema and enable TPC-H benchmark data:

.. code-block:: python

   import pyexasol

   conn = pyexasol.connect(dsn="exasol-db", user="SYS", password="exasol")

   conn.execute("CREATE SCHEMA TEST;")
   conn.execute("CREATE TABLE TEST.TEST_TABLE (NUM_VALUE INTEGER);")
   conn.execute("INSERT INTO TEST.TEST_TABLE (NUM_VALUE) VALUES (1);")

   result = conn.execute("SELECT * FROM TEST.TEST_TABLE;").fetchall()
   print(result)  # Expected output: [(1,)]

This setup provides a sample dataset within the ``TEST`` schema.

Configuring Airflow Connection to Exasol
#########################################

Define an Airflow connection in the UI:

- **Conn Id**: ``exasol_default``
- **Conn Type**: ``Exasol``
- **Host**: ``localhost``
- **Port**: ``8563``
- **Schema**: ``TEST``
- **Login**: ``SYS``
- **Password**: ``exasol``
- **Extra** (JSON format):

.. code-block:: json

   {"encryption": true}

Alternatively, use the Airflow CLI:

.. code-block:: sh

   airflow connections add 'exasol_default' \
       --conn-type 'exasol' \
       --conn-host 'localhost' \
       --conn-port '8563' \
       --conn-schema 'TEST' \
       --conn-login 'SYS' \
       --conn-password 'exasol' \
       --conn-extra '{"encryption": true}'

**Definitions:**
- ``<EXASOL_HOST>``: The hostname or IP address of the Exasol instance.
- ``<EXASOL_PORT>``: The port on which Exasol is running (default: ``8563``).
- ``<EXASOL_SCHEMA>``: Schema to be used in Exasol.

Testing the DAG
################

Run the task manually to ensure connectivity:

.. code-block:: sh

   airflow tasks test <DAG_ID> <TASK_ID> $(date +%Y-%m-%d)

If properly configured, the task will extract data from Exasol and upload it to the specified S3 bucket.

**Definitions:**
- ``<DAG_ID>``: ID of the DAG to be tested.
- ``<TASK_ID>``: Specific task within the DAG to be executed.

Troubleshooting
################

### Connection Issues
- Ensure the Exasol container is running: ``docker ps``
- Test connectivity using ``ping()`` in ``pyexasol``
- Verify Airflow connection settings in the UI or with:

.. code-block:: sh

   airflow connections get exasol_default

### Exasol Authentication Errors
- Check if the schema exists:

.. code-block:: sql

   SELECT SCHEMA_NAME FROM EXA_ALL_SCHEMAS;

- If missing, create it manually as shown above.

This setup ensures seamless integration between Exasol and Apache Airflow for efficient data workflows.