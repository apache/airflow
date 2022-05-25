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

* Python: 3.7, 3.8, 3.9, 3.10

* Databases:

  * PostgreSQL: 10, 11, 12, 13, 14
  * MySQL: 5.7, 8
  * SQLite: 3.15.0+
  * MSSQL(Experimental): 2017, 2019

* Kubernetes: 1.20.2, 1.21.1, 1.22.0, 1.23.0, 1.24.0

**Note:** MySQL 5.x versions are unable to or have limitations with
running multiple schedulers -- please see: :doc:`/concepts/scheduler`. MariaDB is not tested/recommended.

**Note:** SQLite is used in Airflow tests. Do not use it in production. We recommend
using the latest stable version of SQLite for local development.

Starting with Airflow 2.3.0, Airflow is tested with Python 3.7, 3.8, 3.9 and 3.10.

The minimum memory required we recommend Airflow to run with is 4GB, but the actual requirements depends
wildly on the deployment options you have

**Note**: Airflow currently can be run on POSIX-compliant Operating Systems. For development it is regularly
tested on fairly modern Linux Distros and recent versions of MacOS.
On Windows you can run it via WSL2 (Windows Subsystem for Linux 2) or via Linux Containers.
The work to add Windows support is tracked via `#10388 <https://github.com/apache/airflow/issues/10388>`__ but
it is not a high priority. You should only use Linux-based distros as "Production" execution environment
as this is the only environment that is supported. The only distro that is used in our CI tests and that
is used in the `Community managed DockerHub image <https://hub.docker.com/p/apache/airflow>`__ is
``Debian Bullseye``.
