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



Amazon Athena Operators
=========================

Amazon Athena is an interactive query service that makes it easy to analyze data in Amazon S3 using standard SQL. While Amazon Athena itself does not provide a DB API 2.0 (PEP 249) compliant connection, the PyAthena library offers this functionality, building upon the boto3 library.

This documentation covers two primary ways to interact with Amazon Athena with Airflow:

1. API (HTTP Boto3): This method uses Amazon Athena's direct API through the boto3 library. It is the preferred method for users who wish to interact with Athena at a lower level, directly through HTTP requests.

2. DB API Connection (Amazon Athena SQL): For users who prefer a more traditional database interaction, PyAthena implements the DB API 2.0 specification, allowing Athena to be used similarly to other relational databases through SQL.

Choosing Your Connection Method
---------------------------------

Airflow offers two ways to query data using Amazon Athena.

**Amazon Athena (API):** Choose this option if you need to execute a single statement without bringing back the results in airflow.

**Amazon Athena SQL (DB API Connection):** Opt for this if you need to execute multiple queries in the same operator and it's essential to retrieve and process query results directly in Airflow, such as for sensing values or further data manipulation.

.. note::
   Both connection methods uses `Amazon Web Services Connection <../../connections/aws>`_ under the hood for authentication.
   You should decide which connection method to use based on your use case.

.. toctree::
    :maxdepth: 1
    :glob:

    *
