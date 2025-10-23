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
========

Security considerations for the MariaDB provider.

Connection Security
-------------------

* Always use encrypted connections (SSL/TLS) when connecting to MariaDB databases in production
* Store database credentials securely using Airflow's connection management
* Use strong passwords and consider using MariaDB's authentication plugins
* Regularly rotate database credentials

Data Security
-------------

* Be cautious when using the CpimportOperator with sensitive data
* Ensure proper file permissions on data files used with cpimport
* Use secure file transfer methods when working with S3 integration
* Consider data encryption at rest and in transit

Best Practices
--------------

* Use least privilege principle for database users
* Regularly update MariaDB connector dependencies
* Monitor database connections and query performance
* Implement proper logging and auditing for database operations
