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

SQL Injection
=============

Previously, Airflow issued CVE like `CVE-2025-27018 SQL injection in MySQL provider core function <https://www.cve.org/CVERecord?id=CVE-2025-27018/>`_.
The CVE were about the ability to inject SQL without considering the actor performing it.
Airflow will no longer issue CVE for cases of SQL Injection unless the reporter can demonstrate a scenario of exploitation.
For example, if in a security report the only actor that can operate the injection is Actor who has access to Dags folder the report will be rejected.
When submitting a security report of SQL injection the reporter must explain who is the user that can utilize the injection and how the user gained access to be able to perform it.
In simple words, if a user has legit access to write and access the specific Dag, there is no risk that this user will do SQL injection.
