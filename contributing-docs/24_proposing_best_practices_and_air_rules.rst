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

Proposing Airflow Best Practices and Ruff AIR Rules
===================================================

This document explains how to propose a new Airflow best practice and, when appropriate, a matching Ruff
``AIR`` rule.

Not every best practice needs a linter rule, but for practices that can be checked automatically, ``AIR`` rules
help contributors and users detect issues early and keep code consistent.

Proposal process
----------------

1. Begin the voting process or initiate a Lazy Consensus regarding migration, deprecation,
   or any other issue where there is strong community agreement. If someone opposes the decision,
   we can revisit the discussion and hold another vote.
2. Once the vote or Lazy Consensus is approved, create a pull request and update the Airflow best practices documentation.
   At this point, the development of Ruff rules can proceed concurrently.
3. After the pull request for Ruff is merged, ensure that the relevant documentation is updated to incorporate
   the new Ruff rules accordingly.

See also
--------

* `How to communicate <02_how_to_communicate.rst>`__
* `ASF Voting Process <https://www.apache.org/foundation/voting.html>`__
