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

``apache-airflow-providers-edge``


Changelog
---------

0.5.0pre0
.........

Misc
~~~~

* ``Edge worker triggers graceful shutdown, if worker version and main instance do not match.``

0.4.0pre0
.........

Misc
~~~~

* ``Edge Worker uploads log file in chunks. Chunk size can be defined by push_log_chunk_size value in config.``

0.3.0pre0
.........

Misc
~~~~

* ``Edge Worker exports metrics``
* ``State is set to unknown if worker heartbeat times out.``

0.2.2re0
.........

Misc
~~~~

* ``Fixed type confusion for PID file paths (#43308)``

0.2.1re0
.........

Misc
~~~~

* ``Fixed handling of PID files in Edge Worker (#43153)``

0.2.0pre0
.........

Misc
~~~~

* ``Edge Worker can add or remove queues in the queue field in the DB (#43115)``

0.1.0pre0
.........


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.1.0
.....

|experimental|

Initial version of the provider.

.. note::
  This provider is currently experimental
