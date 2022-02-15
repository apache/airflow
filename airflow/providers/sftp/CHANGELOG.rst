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

2.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Bugfix: ''SFTPHook'' does not respect ''ssh_conn_id'' arg (#20756)``
* ``fix deprecation messages for SFTPHook (#20692)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.4.0
.....

Features
~~~~~~~~

* ``Making SFTPHook's constructor consistent with its superclass SSHHook (#20164)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix MyPy Errors for SFTP provider (#20242)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.3.0
.....

Features
~~~~~~~~

* ``Add test_connection method for sftp hook (#19609)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.2.0
.....

Features
~~~~~~~~

* ``SFTP hook to prefer the SSH paramiko key over the key file path (#18988)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``More f-strings (#18855)``

2.1.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

2.1.0
.....

Features
~~~~~~~~

* ``Add support for non-RSA type key for SFTP hook (#16314)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove redundant logging in SFTP Hook (#16704)``
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

Features
~~~~~~~~

* ``Depreciate private_key_pass in SFTPHook conn extra and rename to private_key_passphrase (#14028)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.2.0
.....

Features
~~~~~~~~

* ``Undeprecate private_key option in SFTPHook (#15348)``
* ``Add logs to show last modified in SFTP, FTP and Filesystem sensor (#15134)``

1.1.1
.....

Features
~~~~~~~~

* ``SFTPHook private_key_pass extra param is deprecated and renamed to private_key_passphrase, for consistency with
  arguments' naming in SSHHook``

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``


1.1.0
.....

Updated documentation and readme files.

Features
~~~~~~~~

* ``Add retryer to SFTP hook connection (#13065)``


1.0.0
.....

Initial version of the provider.
