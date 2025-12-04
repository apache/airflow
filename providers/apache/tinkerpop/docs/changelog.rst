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

``apache-airflow-providers-apache-tinkerpop``


1.1.0
.....

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.0.5
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Fix the '__init__.py' for tests (#58211)``
   * ``Enable PT006 rule to 13 files in providers (apache) (#57998)``

1.0.4
.....

Misc
~~~~

* ``Migrate Apache providers & Elasticsearch to ''common.compat'' (#57016)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare release for Sep 2025 2nd wave of providers (#55688)``
   * ``Prepare release for Sep 2025 1st wave of providers (#55203)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``
   * ``Make term Dag consistent in providers docs (#55101)``
   * ``Switch pre-commit to prek (#54258)``

1.0.3
.....

Misc
~~~~

* ``Bump gremlinpython to 3.7.3 (#54109)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.0.2
.....

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``cleanup type ignore (#53305)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Replace BaseHook to Task SDK for apache/tinkerpop (#52844)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.0.1
.....

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Replace 'models.BaseOperator' to Task SDK one for Apache TinkerPop (#52400)``
* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Filter only provided integration paths for breeze integration testing (#52462)``
   * ``Prepare release for providers May 2025 (#50531)``
   * ``Doc fix: remove extrac package name text in index (#50366)``
   * ``Fix PR number in changelog (#50190)``
   * ``Prepare release for June 2025 provider wave (#51724)``

1.0.0
.....

* ``Initial version of the provider. (#47446)``
