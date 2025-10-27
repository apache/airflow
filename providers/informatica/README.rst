
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

Apache Airflow Informatica Provider
===================================

This provider package contains integrations for `Informatica Enterprise Data Catalog (EDC) <https://www.informatica.com/products/data-governance/enterprise-data-catalog.html>`_ to work with Apache Airflow.


Features
--------

- **Airflow Integration**: Seamless integration with Airflow's lineage system using inlets and outlets.


Installation
------------

.. code-block:: bash

    pip install apache-airflow-providers-informatica



Connection Setup
~~~~~~~~~~~~~~~~

Create an Informatica EDC connection in Airflow:

    #. **Connection Type**: ``http``
    #. **Host**: Your EDC server hostname
    #. **Port**: EDC server port (typically 9087)
    #. **Schema**: ``https`` or ``http``
    #. **Login**: EDC username
    #. **Password**: EDC password
    #. **Extras**: Add the following JSON:

       .. code-block:: json

           {"security_domain": "your_security_domain"}

Configuration Options
~~~~~~~~~~~~~~~~~~~~~

Add to your ``airflow.cfg``:

.. code-block:: ini

    [informatica]
    # Disable sending events without uninstalling the Informatica Provider
    disabled = False
    # The connection ID to use when no connection ID is provided
    default_conn_id = informatica_edc_default



Complete DAG Example
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from airflow import DAG
   from airflow.operators.python import PythonOperator
   from datetime import datetime


   def my_python_task(**kwargs):
       print("Hello Informatica Lineage!")


   with DAG(
       dag_id="example_informatica_lineage_dag",
       start_date=datetime(2024, 1, 1),
       schedule_interval=None,
       catchup=False,
   ) as dag:
       python_task = PythonOperator(
           task_id="my_python_task",
           python_callable=my_python_task,
           inlets=[{"dataset_uri": "edc://object/source_table_abc123"}],
           outlets=[{"dataset_uri": "edc://object/target_table_xyz789"}],
       )
   python_task



EDC API Endpoints Used
~~~~~~~~~~~~~~~~~~~~~~


- ``/access/2/catalog/data/objects/{object_id}?includeRefObjects={true|false}`` - Retrieve catalog object details
- ``/access/1/catalog/data/objects`` (PATCH) - Create lineage relationship between objects


Testing
~~~~~~~

.. code-block:: bash

    # Run unit tests
    python -m pytest providers/informatica/tests/

    # Run specific test
    python -m pytest providers/informatica/tests/hooks/test_edc.py

    # Run with coverage
    python -m pytest providers/informatica/tests/ --cov=airflow.providers.informatica

Code Quality
~~~~~~~~~~~~

.. code-block:: bash

    # Type checking
    mypy providers/informatica/src/airflow/providers/informatica/

    # Code formatting
    black providers/informatica/src/airflow/providers/informatica/

    # Linting
    flake8 providers/informatica/src/airflow/providers/informatica/



Common Issues
~~~~~~~~~~~~~

1. **Authentication Failures**
    - Verify EDC credentials and server connectivity
    - Check firewall and network access to EDC server
    - Ensure EDC service is running and accessible

2. **No Lineage Found**
    - Verify table IDs exist in EDC catalog


Logging
~~~~~~~

Enable debug logging to troubleshoot issues:

.. code-block:: python

    import logging

    logging.getLogger("airflow.providers.informatica").setLevel(logging.DEBUG)


Compatibility
~~~~~~~~~~~~

- **Informatica EDC Version**: This provider is compatible with Informatica EDC version 10.5 and above.
- **Airflow Compatibility**: This provider is compatible with Apache Airflow 3.0 and above.



License
-------

Licensed under the Apache License, Version 2.0. See `LICENSE <LICENSE>`_ for details.

Support
-------

**Note:** This provider is not officially maintained or endorsed by Informatica. It is a community-developed integration for Apache Airflow.

- `Apache Airflow Documentation <https://airflow.apache.org/docs/>`_
- `GitHub Issues <https://github.com/apache/airflow/issues>`_
