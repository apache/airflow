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

SBOM
====

Software Bill Of Materials (SBOM) files are critical assets used for transparency, providing a clear inventory of all the components
used in Apache Airflow (name, version, supplier and transitive dependencies). They are an exhaustive representation
of the software dependencies.

The general use case for such files is to help assess and manage risks. For instance a quick lookup against your SBOM files can help identify if a CVE (Common Vulnerabilities and Exposures) in a
library is affecting you.

By default, Apache Airflow SBOM files are generated for airflow core with all providers. In the near future we aim at generating SBOM files per provider and also provide them for docker standard images.

Each airflow version has its own SBOM files, one for each supported python version.
You can find them `here <https://airflow.apache.org/docs/apache-airflow/stable/sbom>`_.
