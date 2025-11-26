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

``apache-airflow-providers-common-messaging``

Changelog
---------

2.0.1
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Fix documentation/provider.yaml consistencies (#57283)``
   * ``Prepare release for Oct 2025 wave of providers (#57029)``
   * ``Enable PT011 rule to prvoider tests (#56495)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``
   * ``Prepare release for Sep 2025 2nd wave of providers (#55688)``
   * ``Add section in doc about using message payload (#55438)``

2.0.0
.....


Breaking changes
~~~~~~~~~~~~~~~~

* ``Refactor Common Queue Interface (#54651)``

Bug Fixes
~~~~~~~~~

* ``fix(messaging): improve MessageQueueTrigger logging and add comprehensive tests (#54492)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch pre-commit to prek (#54258)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

1.0.5
.....

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.0.4
.....

Misc
~~~~

* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Move MESSAGE_QUEUE_PROVIDERS array to where it belongs - to msq_queue (#51774)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.0.2
.....

Misc
~~~~

* ``AIP-82: Add KafkaMessageQueueProvider (#49938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``

1.0.1
.....

Misc
~~~~

* ``Move SQS message queue to Amazon provider (#50057)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix version of common.messaging to 1.0.1 (#50099)``
   * ``Add back missing '[sources]' link in generated documentation's includes (#49978)``
   * ``Avoid committing history for providers (#49907)``
   * ``Prepare docs for Apr 3rd wave of providers (#49338)``
   * ``Move SQS message queue code example from core to provider docs (#49208)``

1.0.0
.....

.. note::
  This release of provider is only available for Airflow 3.0+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

* ``Initial version of the provider (#46694)``
