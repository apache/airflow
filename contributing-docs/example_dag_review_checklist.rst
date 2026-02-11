Example DAG Review Checklist
============================

This document provides a checklist for reviewing example DAGs submitted to
Apache Airflow. The goal is to ensure that example DAGs are clean, consistent,
and follow best practices while serving their multiple purposes:

* Clean and consistent
* Easy to understand
* Useful for tutorials and documentation
* Compatible with testing and CI processes
* Properly structured across core and provider packages

General Guidelines
------------------

- [ ] The DAG has a clear and descriptive file name.
- [ ] The purpose of the DAG is documented at the top of the file.
- [ ] The DAG demonstrates a single feature or concept.
- [ ] Examples do not duplicate functionality across the repo.
- [ ] Example runs in a reasonable amount of time (suitable for tutorials & CI).

Example Categorization
----------------------

Example DAGs should clearly indicate which category they belong to:

- [ ] **Tutorial Examples** — educational, simple to read and follow.
- [ ] **Documentation Snippets** — aligned with docs examples.
- [ ] **Testing/CI Examples** — used to exercise system features in CI.
- [ ] Example type is noted in module-level docstring or metadata.

Structure & Readability
-----------------------

- [ ] The DAG and task IDs are meaningful and consistent.
- [ ] The code follows PEP 8 formatting.
- [ ] Imports are organized, minimal, and sorted.
- [ ] Reusable logic is factored into helper functions or modules.
- [ ] Example layout is consistent with other examples of the same category.

Documentation
-------------

- [ ] The DAG contains a module-level docstring explaining:
  * What the DAG does
  * Why it exists
  * When and how to use it
  * Any prerequisites or expected services
- [ ] The DAG contains ``doc_md`` for in-UI documentation where appropriate.
- [ ] Inline comments explain complex or non-obvious logic.

Best Practices
--------------

- [ ] The DAG uses well-named variables and avoids hard-coded secrets.
- [ ] Default arguments are defined and minimal.
- [ ] Tasks are idempotent and side-effect clean.
- [ ] Uses modern Airflow APIs and operators.
- [ ] Avoids deprecated or discouraged features.
- [ ] Providers examples import only allowed modules.

Testing & CI Compatibility
--------------------------

- [ ] The DAG parses cleanly (no import errors).
- [ ] Example does not depend on external services unless documented.
- [ ] Example is uniquely identifiable and does not conflict with others.
- [ ] Example respects AIRFLOW__* environment variables where applicable.

Provider Example DAGs
---------------------

Provider examples have slightly different expectations:

- [ ] The provider example uses provider-specific operators/hooks correctly.
- [ ] Provider dependencies are clearly documented in the docstring.
- [ ] Example is not auto-loaded unless intended.
- [ ] Example naming follows provider naming conventions.
