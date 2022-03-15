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


Changelog
---------

2.1.2
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.1.1
.....

Bug Fixes
~~~~~~~~~

* ``fix param rendering in docs of SparkSubmitHook (#21788)``

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.0
.....

Features
~~~~~~~~

* ``Add more SQL template fields renderers (#21237)``
* ``Add optional features in providers. (#21074)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Ensure Spark driver response is valid before setting UNKNOWN status (#19978)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
    * ``Fix mypy providers (#20190)``
    * ``Fix mypy spark hooks (#20290)``
    * ``Fix MyPy errors in Apache Providers (#20422)``
    * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
    * ``Even more typing in operators (template_fields/ext) (#20608)``
    * ``Update documentation for provider December 2021 release (#20523)``

2.0.2
.....

Bug Fixes
~~~~~~~~~

* ``fix bug of SparkSql Operator log  going to infinite loop. (#19449)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Cleanup of start_date and default arg use for Apache example DAGs (#18657)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``More f-strings (#18855)``
   * ``Remove unnecessary string concatenations in AirflowException messages (#18817)``

2.0.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
   * ``Updating Apache example DAGs to use XComArgs (#16869)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Removes pylint from our toolchain (#16682)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Bug fixes
~~~~~~~~~

* ``Make SparkSqlHook use Connection (#15794)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.3
.....

Bug fixes
~~~~~~~~~

* ``Fix 'logging.exception' redundancy (#14823)``


1.0.2
.....

Bug fixes
~~~~~~~~~

* ``Use apache.spark provider without kubernetes (#14187)``


1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
