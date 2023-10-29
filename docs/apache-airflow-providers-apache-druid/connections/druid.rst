.. _druid_integration:

Apache Druid Integration with Apache Airflow
============================================

Introduction
------------

Apache Druid, a high-performance, real-time analytics database, can be seamlessly integrated with Apache Airflow using the DruidHook and DruidDbApiHook. The DruidHook facilitates the submission of ingestion tasks to the Druid cluster, while the DruidDbApiHook enables querying of the Druid broker.

Establishing the Connection
---------------------------

To establish a connection between Apache Airflow and Apache Druid, follow these steps:

1. **Install the required packages**: Ensure that the necessary dependencies for integrating Apache Druid with Apache Airflow are installed.

2. **Configure the Druid connection**: Set up the connection details, including the Druid connection ID, host, port, and any authentication credentials required to access the Druid cluster.

3. **Initialize the DruidHook and DruidDbApiHook**: Create instances of the DruidHook and DruidDbApiHook in your Apache Airflow DAG (Directed Acyclic Graph) to facilitate communication with the Druid cluster and broker, respectively.

.. _druid_connection:

Druid Connection
================

The Druid Connection type enables integration with the Apache Druid database within Apache Airflow.

Default Connection IDs
----------------------

Hooks and operators related to Druid use the default connection ID `druid_default` by default.

Configuring the Connection
--------------------------

The following parameters can be configured for the Druid connection:

- **Host**: Host name or IP address of the Druid server.
- **Port**: Port number of the Druid server.
- **Schema**: Specify the schema, such as "http" or "https", for the Druid connection.
- **Username**: Username for authentication (if applicable).
- **Password**: Password for authentication (if applicable).
- **Endpoint**: Endpoint for the Druid API.
- **Optional Parameters**: Additional parameters that can be used in the Druid connection.

  - **param1**: Description of param1.
  - **param2**: Description of param2.
  - **param3**: Description of param3.

By providing these configuration details, users can effectively set up the Druid connection within Apache Airflow, enabling seamless integration with the Apache Druid database for data analysis and querying purposes.

Examples
--------

Below is an example of how to configure the Druid connection in Apache Airflow:

.. code-block:: python

    from airflow.providers.druid.hooks.druid import DruidHook

    # Set up the Druid connection
    druid_hook = DruidHook()
    druid_connection = druid_hook.get_connection("druid_default")

    # Configure the connection parameters
    druid_connection.host = "druid.example.com"
    druid_connection.port = 8082
    druid_connection.schema = "http"
    druid_connection.login = "username"
    druid_connection.password = "password"
    druid_connection.extra = {"param1": "value1", "param2": "value2", "param3": "value3"}

    # Execute queries and operations with the configured connection

For further details on advanced configurations and best practices, refer to the Apache Druid and Apache Airflow documentation.

Executing Operations
--------------------

Once the connection is established, you can perform various operations on the Druid cluster and broker:

1. **Submitting ingestion tasks**: Utilize the DruidHook to submit ingestion tasks, allowing you to load data into the Druid database for analysis and querying.

2. **Querying data**: Use the DruidDbApiHook to execute queries on the data stored in the Druid database, retrieving valuable insights and analytics for further processing.

3. **Managing the Druid database**: Leverage the capabilities of the DruidHook and DruidDbApiHook to manage and maintain the Druid database, ensuring smooth and efficient data operations within your Apache Airflow workflows.

For more detailed usage and configuration instructions, refer to the Apache Druid and Apache Airflow documentation.

Example
-------

Below is an example of how to use the DruidHook and DruidDbApiHook to interact with the Druid cluster and broker in Apache Airflow:

.. code-block:: python

    # Example code demonstrating the usage of the DruidHook and DruidDbApiHook
    # Import the necessary classes
    from airflow.providers.druid.hooks.druid import DruidHook, DruidDbApiHook

    # Initialize the DruidHook and DruidDbApiHook
    druid_hook = DruidHook()
    druid_db_api_hook = DruidDbApiHook()

    # Submit an ingestion task to the Druid cluster
    druid_hook.submit_indexing_job(json_index_spec)

    # Execute a query on the Druid broker
    result = druid_db_api_hook.run(sql_query)

    # Process the query results for further analysis and visualization
    process_results(result)

For additional usage examples and best practices, consult the Apache Druid and Apache Airflow documentation.

Conclusion
----------

By integrating Apache Druid with Apache Airflow, you can harness the power of real-time analytics and efficient data processing within your data workflows, enabling seamless data ingestion, querying, and analysis for your business needs.
