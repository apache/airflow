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


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

Changelog
---------

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Beginning with version 2.0.0, users could specify single-tenant dbt Cloud domains via the ``schema`` parameter
in an Airflow connection. Subsequently in version 2.3.1, users could also connect to the dbt Cloud instances
outside of the US region as well as private instances by using the ``host`` parameter of their Airflow
connection to specify the entire tenant domain. Backwards compatibility for using ``schema`` was left in
place. Version 3.0.0 removes support for using ``schema`` to specify the tenant domain of a dbt Cloud
instance. If you wish to connect to a single-tenant, instance outside of the US, or a private instance, you
must use the ``host`` parameter to specify the _entire_ tenant domain name in your Airflow connection.

* ``Drop Connection.schema use in DbtCloudHook  (#29166)``

2.3.1
.....

Bug Fixes
~~~~~~~~~
* ``Use entire tenant domain name in dbt Cloud connection (#28890)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.3.0
.....

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable string normalization in python formatting - providers (#27205)``

2.2.0
.....

Features
~~~~~~~~

* ``Add 'DbtCloudListJobsOperator' (#26475)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

2.1.0
.....

Features
~~~~~~~~

* ``Improve taskflow type hints with ParamSpec (#25173)``

2.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Update providers to use functools compat for ''cached_property'' (#24582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

Features
~~~~~~~~

* ``Enable dbt Cloud provider to interact with single tenant instances (#24264)``

Bug Fixes
~~~~~~~~~

* ``Fix typo in dbt Cloud provider description (#23179)``
* ``Fix new MyPy errors in main (#22884)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``AIP-47 - Migrate dbt DAGs to new design #22472 (#24202)``
   * ``Prepare provider documentation 2022.05.11 (#23631)``
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``
   * ``Replace usage of 'DummyOperator' with 'EmptyOperator' (#22974)``
   * ``Update dbt.py (#24218)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

1.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

1.0.1
.....

Initial version of the provider.
