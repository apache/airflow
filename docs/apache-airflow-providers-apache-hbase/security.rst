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

Security
--------

The Apache HBase provider uses the HappyBase library to connect to HBase via the Thrift protocol. 

Security Considerations
~~~~~~~~~~~~~~~~~~~~~~~

* **Connection Security**: Ensure that HBase Thrift server is properly secured and accessible only from authorized networks
* **Authentication**: Configure proper authentication mechanisms in HBase if required by your environment
* **Data Encryption**: Consider enabling SSL/TLS for Thrift connections in production environments
* **Access Control**: Use HBase's built-in access control mechanisms to restrict table and column family access
* **Network Security**: Deploy HBase in a secure network environment with proper firewall rules

Connection Configuration
~~~~~~~~~~~~~~~~~~~~~~~~

When configuring HBase connections in Airflow:

* Use secure connection parameters in the connection configuration
* Store sensitive information like passwords in Airflow's connection management system
* Avoid hardcoding credentials in DAG files
* Consider using Airflow's secrets backend for enhanced security

For production deployments, consult the `HBase Security Guide <https://hbase.apache.org/book.html#security>`_ for comprehensive security configuration.