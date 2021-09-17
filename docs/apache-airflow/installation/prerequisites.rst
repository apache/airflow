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

Prerequisites
-------------

Airflow is tested with:

* Python: 3.6, 3.7, 3.8, 3.9

* Databases:

  * PostgreSQL:  9.6, 10, 11, 12, 13
  * MySQL: 5.7, 8
  * SQLite: 3.15.0+

* Kubernetes: 1.18.15 1.19.7 1.20.2

**Note:** MySQL 5.x versions are unable to or have limitations with
running multiple schedulers -- please see: :doc:`/concepts/scheduler`. MariaDB is not tested/recommended.

**Note:** SQLite is used in Airflow tests. Do not use it in production. We recommend
using the latest stable version of SQLite for local development.

Starting with Airflow 2.1.2, Airflow is tested with Python 3.6, 3.7, 3.8, and 3.9.

The minimum memory required we recommend Airflow to run with is 4GB, but the actual requirements depends
wildly on the deployment options you have
