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

``apache-airflow-providers-common-io``

Changelog
---------

1.4.2
.....

Misc
~~~~

* ``Drop python3.8 support core and providers (#42766)``
* ``Rename dataset related python variable names to asset (#41348)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Protect against None components of universal pathlib xcom backend (#41921)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.4.0
.....

Features
~~~~~~~~

* ``[AIP-62] Translate AIP-60 URI to OpenLineage (#40173)``
* ``openlineage: add file dataset type support into common.io provider (#40817)``

Misc
~~~~

* ``openlineage: migrate OpenLineage provider to V2 facets. (#39530)``
* ``openlineage: add support for hook lineage for S3Hook (#40819)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

1.3.2
.....

Bug Fixes
~~~~~~~~~

* ``fix: OpenLineage in FileTransferOperator for Airflow 2.8 (#39755)``

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

1.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix missing reverse quote in docs (#38275)``
* ``Fix remaining D401 checks (#37434)``

Misc
~~~~

* ``Improve XComObjectStorageBackend implementation (#38608)``
* ``Rename to XComObjectStorageBackend (#38607)``
* ``Turn common.io xcom exception into OptionalProviderFeatureException (#38543)``
* ``Update ObjectStoragePath for universal_pathlib>=v0.2.2 (#37930)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix XComObjectStoreBackend config var in docs (#38142)``
   * ``Revert ObjectStorage config variables name (#38415)``
   * ``Update yanked versions in providers changelogs (#38262)``
   * ``Revert "Update ObjectStoragePath for universal_pathlib>=v0.2.1 (#37524)" (#37567)``
   * ``Update ObjectStoragePath for universal_pathlib>=v0.2.1 (#37524)``
   * ``Add comment about versions updated by release manager (#37488)``

1.3.0
.....

Features
~~~~~~~~

* ``AIP-58: Add object storage backend for xcom (#37058)``

1.2.0
.....

Features
~~~~~~~~

* ``Add support for openlineage to AFS and common.io (#36410)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Re-apply updated version numbers to 2nd wave of providers in December (#36380)``
   * ``Prepare 2nd wave of providers in December (#36373)``
   * ``Prepare docs 1st wave of Providers December 2023 (#36112)``
   * ``Add documentation for 3rd wave of providers in Deember (#36464)``

1.1.0
.....

Features
~~~~~~~~

* ``Refactor ObjectStorage into a Path (#35612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use reproducible builds for provider packages (#35693)``
   * ``Fix and reapply templates for provider documentation (#35686)``

1.0.1 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Used older interface from 2.8.0.dev0 versions``

Bug Fixes
~~~~~~~~~

* ``fix changelog of common-io (#35241)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improvements to airflow.io (#35478)``

1.0.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Used older interface from 2.8.0.dev0 versions``

Initial version of the provider.
