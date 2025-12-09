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

Performance tuning (API and UI)
===============================

This guide collects pragmatic tips that improve Airflow performance for API and UI workloads.

Custom metadata indexes
-----------------------

If you observe slowness in some API calls or specific UI views, you should inspect query plans and add indexes yourself
that match your workload. Listing endpoints and UI table views with specific ordering criteria are likely
to benefit from additional indexes if you have a large volume of metadata.

When to use
^^^^^^^^^^^

- Slow API list/detail endpoints caused by frequent scans or lookups on columns like ``start_date``, timestamps (e.g. ``dttm``), or status fields.
- UI pages that load large lists or perform heavy filtering on metadata tables.

Guidance
^^^^^^^^

- Inspect the query planner (e.g., ``EXPLAIN``/``EXPLAIN ANALYZE``) for slow endpoints and identify missing indexes.
- Prefer single or composite indexes that match your most common ordering logic, typically the ``order_by``
  query parameter used in API calls. Composite indexes can cover multi criteria ordering.
- Your optimal indexes depend on how you use the API and UI; there is no one-size-fits-all set we can ship by default.

Upgrade considerations
^^^^^^^^^^^^^^^^^^^^^^

To avoid conflicts with Airflow database upgrades, delete your custom indexes before running an Airflow DB upgrade
and re-apply them after the upgrade succeeds.

Notes
^^^^^

- Review query plans (e.g. via ``EXPLAIN``) to choose effective column sets and ordering for your workload.
- Composite indexes should list columns in selectivity order appropriate to your most common predicates.
- Indexes incur write overhead; add only those that materially improve your read paths.
