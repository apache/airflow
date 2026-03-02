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

This section of the documentation covers security-related topics.

Make sure to get familiar with the :doc:`Airflow Security Model </security/security_model>` if you want to understand
the different user types of Apache Airflow®, what they have access to, and the role Deployment Managers have in deploying Airflow in a secure way.

Also, if you want to understand how Airflow releases security patches and what to expect from them,
head over to :doc:`Releasing security patches </security/releasing_security_patches>`.

There are number of services where you can track security issues reported and announced by Airflow same as for
any of the projects following the standard CVE databases.

One such database is run by the MITRE corporation and you can search
for `Airflow CVEs <https://www.cve.org/CVERecord/SearchResults?query=apache+airflow>`_
there, however you might use whatever database you and your organization prefers to track security issues and CVEs.

Follow the below topics as well to understand other aspects of Airflow security:

.. toctree::
    :maxdepth: 1
    :glob:

    *
    secrets/index
