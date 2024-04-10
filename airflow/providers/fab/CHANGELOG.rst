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

``apache-airflow-providers-fab``

Changelog
---------

1.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Rename 'allowed_filter_attrs' to 'allowed_sort_attrs' (#38626)``
* ``Fix azure authentication when no email is set (#38872)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``fix: try002 for provider fab (#38801)``

1.0.2
.....

First stable release for the provider


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade FAB to 4.4.1 (#38319)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Make the method 'BaseAuthManager.is_authorized_custom_view' abstract (#37915)``
   * ``Avoid use of 'assert' outside of the tests (#37718)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``
   * ``Remove useless methods from security manager (#37889)``
   * ``Use 'next' when redirecting (#37904)``
   * ``Add "MENU" permission in auth manager (#37881)``
   * ``Avoid to use too broad 'noqa' (#37862)``
   * ``Add post endpoint for dataset events (#37570)``
   * ``Add "queuedEvent" endpoint to get/delete DatasetDagRunQueue (#37176)``
   * ``Add swagger path to FAB Auth manager and Internal API (#37525)``
   * ``Revoking audit_log permission from all users except admin (#37501)``
   * ``Enable the 'Is Active?' flag by default in user view (#37507)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Until we release 2.9.0, we keep airflow >= 2.9.0.dev0 for FAB provider (#37421)``
   * ``Improve suffix handling for provider-generated dependencies (#38029)``

1.0.0 (YANKED)
..............

Initial version of the provider (beta).
