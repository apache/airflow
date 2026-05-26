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

Example Dag Review Checklist
============================

This document provides a checklist for reviewing example Dags submitted to
Apache Airflow. The goal is to ensure that example Dags are clean, consistent,
and follow best practices while serving their multiple purposes:

* Clean and consistent
* Easy to understand
* Useful for tutorials and documentation
* Compatible with testing and CI processes
* Properly structured across core and provider packages

General Guidelines
------------------

- [ ] The Dag has a clear and descriptive file name.
- [ ] The purpose of the Dag is documented at the top of the file.
- [ ] The Dag demonstrates a single feature or concept.
- [ ] Examples do not duplicate functionality across the repo.
- [ ] Example runs in a reasonable amount of time (suitable for tutorials & CI).

Example Categorization
----------------------

Example Dags should clearly indicate which category they belong to:

- [ ] **Tutorial Examples** — educational, simple to read and follow.
- [ ] **Documentation Snippets** — aligned with docs examples.
- [ ] **Testing/CI Examples** — used to exercise system features in CI.
- [ ] Example type is noted in module-level docstring or metadata.

Structure & Readability
-----------------------

- [ ] The Dag and task IDs are meaningful and consistent.
- [ ] The code follows `PEP 8 <https://peps.python.org/pep-0008/>`_ formatting.
- [ ] Imports are organized, minimal, and sorted.
- [ ] Reusable logic is factored into helper functions or modules.
- [ ] Example layout is consistent with other examples of the same category.

Documentation
-------------

- [ ] The Dag contains a module-level docstring explaining:
  * What the Dag does
  * Why it exists
  * When and how to use it
  * Any prerequisites or expected services
- [ ] The Dag contains ``doc_md`` for in-UI documentation where appropriate.
- [ ] Inline comments explain complex or non-obvious logic.

Best Practices
--------------

- [ ] The Dag uses well-named variables and avoids hard-coded secrets.
- [ ] Default arguments are defined and minimal.
- [ ] Tasks are idempotent and side-effect clean.
- [ ] Uses modern Airflow APIs and operators.
- [ ] Avoids deprecated or discouraged features.
- [ ] Providers examples import only allowed modules.

Testing & CI Compatibility
--------------------------

- [ ] The Dag parses cleanly (no import errors).
- [ ] Example does not depend on external services unless documented.
- [ ] Example is uniquely identifiable and does not conflict with others.
- [ ] Example respects AIRFLOW__* environment variables where applicable.

Provider Example Dags
---------------------

Provider examples have slightly different expectations:

- [ ] The provider example uses provider-specific operators/hooks correctly.
- [ ] Provider dependencies are clearly documented in the docstring.
- [ ] Example is not auto-loaded unless intended.
- [ ] Example naming follows provider naming conventions.

Reference Examples
------------------

The following example Dags are kept aligned with this checklist and are good
templates for new tutorial-style examples. Both implement the same
"measurement correction" storyline so the TaskFlow and ``PythonOperator``
styles can be compared side by side:

- `example_measurement_correction_decorator.py
  <https://github.com/apache/airflow/blob/main/providers/standard/src/airflow/providers/standard/example_dags/example_measurement_correction_decorator.py>`_ — TaskFlow version.
- `example_measurement_correction_operator.py
  <https://github.com/apache/airflow/blob/main/providers/standard/src/airflow/providers/standard/example_dags/example_measurement_correction_operator.py>`_ — classic ``PythonOperator`` version.

When introducing a new tutorial-style example, prefer copying the shape of
these two files (module docstring, ``doc_md``, per-task docstrings, no
external dependencies) rather than starting from scratch.
