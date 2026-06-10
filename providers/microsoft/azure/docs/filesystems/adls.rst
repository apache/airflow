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

Azure Data Lake Storage Gen2 Filesystem
=======================================

The Azure Data Lake Storage Gen2 filesystem provides access to ADLS Gen2
through Airflow's ObjectStoragePath interface.

Supported URL formats:

* ``abfs://connection_id/container/path/to/file``
* ``abfss://connection_id/container/path/to/file``
* ``adl://connection_id/container/path/to/file``

Connection Configuration
------------------------

Create an ADLS Gen2 connection in Airflow with the following parameters:

* **Connection Type**: adls
* **Host**: Storage account name, for example ``mystorageaccount``
* **Login**: Client ID for Active Directory authentication
* **Password**: Client secret for Active Directory authentication, or account key for shared key authentication

The connection form provides additional configuration fields:

* ``tenant_id``: Tenant ID for Active Directory authentication
* ``connection_string``: ADLS Gen2 connection string; this takes precedence over other authentication fields
* ``account_name``: Optional explicit storage account name override
* ``account_host``: Optional custom storage account host, for private endpoints or custom domains

Examples
--------

Read from ADLS Gen2 with ObjectStoragePath:

.. code-block:: python

    from airflow.sdk.io.path import ObjectStoragePath

    path = ObjectStoragePath("abfs://adls_default/raw/events.json")
    content = path.read_text()

Write to ADLS Gen2:

.. code-block:: python

    from airflow.sdk.io.path import ObjectStoragePath

    output_path = ObjectStoragePath("abfss://adls_default/processed/events.json")
    output_path.write_text('{"status": "ok"}')

Requirements
------------

The Azure Data Lake Storage Gen2 filesystem requires the ``adlfs`` Python package.

Cross-References
----------------

* :doc:`ADLS Gen2 connection </connections/adls_v2>`
