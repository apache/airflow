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

3.5.0
.....

Features
~~~~~~~~

* ``SSH Provider: Add cmd_timeout to ssh connection extra (#29347)``

3.4.0
.....

Features
~~~~~~~~

* ``Add .bash and other extensions to SSHOperator template_ext (#28617)``
* ``Add test_connection method for SSHHook (#28184)``
* ``SSH task exit code added to XCOM as 'ssh_exit' key (#27370)``

Misc
~~~~
* ``Remove outdated compat imports/code from providers (#28507)``
* ``[misc] Get rid of 'pass' statement in conditions (#27775)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.3.0
.....

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``Added docs regarding templated field (#27301)``
* ``Added environment to templated SSHOperator fields (#26824)``
* ``Apply log formatter on every output line in SSHOperator (#27442)``

Bug Fixes
~~~~~~~~~

* ``A few docs fixups (#26788)``
* ``SSHOperator ignores cmd_timeout (#27182) (#27184)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

3.2.0
.....

Features
~~~~~~~~

* ``feat: load host keys to save new host key (#25979)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

3.1.0
.....

Features
~~~~~~~~

* ``Less verbose logging in ssh operator (#24915)``
* ``Convert sftp hook to use paramiko instead of pysftp (#24512)``

Bug Fixes
~~~~~~~~~

* ``Update providers to use functools compat for ''cached_property'' (#24582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

Features
~~~~~~~~

* ``Add disabled_algorithms as an extra parameter for SSH connections (#24090)``

Bug Fixes
~~~~~~~~~

* ``fixing SSHHook bug when using allow_host_key_change param (#24116)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.4.4
.....

Bug Fixes
~~~~~~~~~

* ``Add exception to catch single line private keys (#23043)``

2.4.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.4.2
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.4.1
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.4.0
.....

Features
~~~~~~~~

* ``Add a retry with wait interval for SSH operator (#14489)``
* ``Add banner_timeout feature to SSH Hook/Operator (#21262)``
* ``Add a retry with wait interval for SSH operator #14489 (#19981)``
* ``Delay the creation of ssh proxy until get_conn() (#20474) (#20474)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add optional features in providers. (#21074)``
   * ``Fix last remaining MyPy errors (#21020)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Fix MyPy Errors for SSH provider (#20241)``
   * ``Refactor SSH tests to not use SSH server in operator tests (#21326)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.3.0
.....

Features
~~~~~~~~

* ``Refactor SSHOperator so a subclass can run many commands (#10874) (#17378)``
* ``update minimum version of sshtunnel to 0.3.2 (#18684)``
* ``Correctly handle get_pty attribute if command passed as XComArg or template (#19323)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add pre-commit hook for common misspelling check in files (#18964)``

2.2.0
.....

Features
~~~~~~~~

* ``[Airflow 16364] Add conn_timeout and cmd_timeout params to SSHOperator; add conn_timeout param to SSHHook (#17236)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.1
.....


Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Ignores exception raised during closing SSH connection (#17528)``

2.1.0
.....

Features
~~~~~~~~

* ``Add support for non-RSA type key for SFTP hook (#16314)``

Bug Fixes
~~~~~~~~~

* ``SSHHook: Using correct hostname for host_key when using non-default ssh port (#15964)``
* ``Correctly load openssh-gerenated private keys in SSHHook (#16756)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Removes pylint from our toolchain (#16682)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Bug Fixes
~~~~~~~~~

* ``Display explicit error in case UID has no actual username (#15212)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add Connection Documentation to more Providers (#15408)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.3.0
.....

Features
~~~~~~~~

* ``A bunch of template_fields_renderers additions (#15130)``

1.2.0
.....

Features
~~~~~~~~

* ``Added support for DSS, ECDSA, and Ed25519 private keys in SSHHook (#12467)``

1.1.0
.....

Updated documentation and readme files.

Features
~~~~~~~~

* ``[AIRFLOW-7044] Host key can be specified via SSH connection extras. (#12944)``

1.0.0
.....

Initial version of the provider.
