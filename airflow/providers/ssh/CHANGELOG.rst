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

2.4.0
.....

Features
~~~~~~~~

* ``Add a retry with wait interval for SSH operator (#14489)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

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

.. Review and move the new changes to one of the sections above:

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
