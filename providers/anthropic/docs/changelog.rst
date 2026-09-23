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

``apache-airflow-providers-anthropic``

Changelog
---------

1.0.0
.....

.. note::
  The provider now requires the ``anthropic`` 1.x SDK (``anthropic>=1.0.0``) and no longer
  installs alongside ``anthropic`` 0.x. Upgrading the provider therefore upgrades the client
  library, so Dag code that calls the SDK directly has to be checked against the 1.x API.

  ``anthropic`` 1.x also drops the implicit ``us-east-1`` fallback, so the ``bedrock`` and ``aws``
  platforms now need an explicit region: set ``aws_region`` in the connection extra, or provide
  ``AWS_REGION`` / ``AWS_DEFAULT_REGION`` (or a region on the AWS profile) on the worker.

Breaking changes
~~~~~~~~~~~~~~~~

* ``Migrate to the anthropic 1.x SDK (#72072, #72094)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.3.0
.....

Features
~~~~~~~~

* ``Record Anthropic agent session token usage and cost in XCom (#71463)``
* ``Add a budget argument to AnthropicAgentSessionOperator and update_session to the hook (#71462)``

Bug Fixes
~~~~~~~~~

* ``Interrupt a running Anthropic session before archiving it (#71465)``
* ``Fix misleading error when an Anthropic agent session stops on its budget (#71461)``

Doc-only
~~~~~~~~

* ``Document Anthropic advisor rosters and pinned inference regions (#71464)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Adopt flit 4 as the provider distribution build backend (#71186)``


0.2.1
.....

Misc
~~~~

* ``Move template-field validation out of AnthropicAgentSessionOperator __init__ (#70432)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.2.0
.....

Features
~~~~~~~~

* ``Default the Anthropic batch model from the connection (#69624)``

Doc-only
~~~~~~~~

* ``Fix dead Managed Agents link and incomplete first-party note in Anthropic provider docs (#69709)``
* ``Add feature-comparison table and toolset links to common.ai provider docs (#69649)``
* ``Document when to use common.ai vs vendor-specific AI providers (#69551)``
* ``Add quickstart to Anthropic provider (#69589)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):


0.1.0
.....

Initial version of the provider.

Features
~~~~~~~~

* ``Add Anthropic provider ('apache-airflow-providers-anthropic') (#69003)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Validate trigger events in Anthropic deferrable tasks (#69379)``
   * ``Reject invalid Amazon Bedrock model IDs in the Anthropic provider (#69404)``
   * ``Fix aws_region platform list in Anthropic connection docs (#69373)``
   * ``Regenerate Anthropic provider docs to remove stale common.compat extra (#69363)``
