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

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

Features
~~~~~~~~

* ``Add Connection Documentation for Providers (#15499)``

Bug Fixes
~~~~~~~~~

* ``Fix hooks extended from http hook (#16109)``
* ``Fix docstring formatting on ``SlackHook`` (#15840)``
* ``Fix Sphinx Issues with Docstrings (#14968)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Adds interactivity when generating provider documentation. (#15518)``
   * ``Rename the main branch of the Airflow repo to be 'main' (#16149)``
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Remove Backport Providers (#14886)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Don't allow SlackHook.call method accept *args (#14289)``


2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

We updated the support for ``slack_sdk`` from ``>=2.0.0,<3.0.0`` to ``>=3.0.0,<4.0.0``. In most cases,
this doesn't mean any breaking changes to the DAG files, but if you used this library directly
then you have to make the changes. For details, see
`the Migration Guide <https://slack.dev/python-slack-sdk/v3-migration/index.html#from-slackclient-2-x>`_
for Python Slack SDK.

* ``Upgrade slack_sdk to v3 (#13745)``


1.0.0
.....

Initial version of the provider.
