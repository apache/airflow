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
   Please, only add notes to the Changelog just below the "Changelog" header when there
   are some breaking changes and you want to add an explanation to the users on how they are supposed
   to deal with them. The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-vespa``

Changelog
---------

0.2.0
.....

.. note::
  The provider now supports Python 3.14. On 3.14 it requires ``pyvespa>=1.2.9``, the first release to
  allow Python 3.14: every earlier release caps itself at ``<3.14``, which was why the provider was
  excluded from it. Python 3.11-3.13 keep ``pyvespa>=1.1.2``, so nothing is forced to upgrade there.

Features
~~~~~~~~

* ``Support Python 3.14 with pyvespa 1.2.9 (#74559, #75062)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.1.2
.....

Misc
~~~~

* ``Drop support for Python 3.10 (#74157)``
* ``Adopt flit 4 as the provider distribution build backend (#71186)``

Doc-only
~~~~~~~~

* ``Update provider READMEs for the Python 3.11 baseline (#74158)``
* ``Fix inconsistency between generated provider docs and pyproject.toml (#68991)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``[main] Upgrade important CI environment (#73629)``
   * ``Revert "[main] Upgrade important CI environment (#73308)" (#73621)``
   * ``[main] Upgrade important CI environment (#73308)``

0.1.1
.....

Misc
~~~~

* ``Add explicit [tool.flit.sdist] sections to flit-based pyproject.tomls (#65861)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):


0.1.0
.....

Initial version of the provider.
