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
