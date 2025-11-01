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

Installing providers from sources
=================================

To install the MariaDB provider from sources, you can use the following commands:

.. code-block:: bash

    # Clone the Apache Airflow repository
    git clone https://github.com/apache/airflow.git
    cd airflow

    # Install the MariaDB provider from source
    pip install -e ./providers/mariadb

    # Or install with development dependencies
    pip install -e ".[dev] ./providers/mariadb"

Building from source
--------------------

To build the MariaDB provider package from source:

.. code-block:: bash

    # Navigate to the MariaDB provider directory
    cd providers/mariadb

    # Build the package
    python -m build

    # Install the built package
    pip install dist/apache_airflow_providers_mariadb-*.whl

Development setup
-----------------

For development, you can install the provider in editable mode:

.. code-block:: bash

    # Install in editable mode with development dependencies
    pip install -e ".[dev] ./providers/mariadb"

    # Run tests
    pytest tests/

    # Run linting
    pre-commit run --all-files
