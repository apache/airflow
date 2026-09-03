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

Apache Airflow Mypy plugins
===========================

``apache-airflow-mypy`` provides `mypy <https://mypy-lang.org/>`_ plugins for Airflow-specific patterns.
The package is optional, independently versioned, and is not required to run Airflow.

Use the plugins when type-checking Dags, custom operators, or hooks to avoid false positives that plain
``mypy`` cannot resolve. The plugins support:

* **Typed decorators** -- decorators that inject keyword arguments at runtime, such as
  ``GoogleBaseHook.fallback_to_default_project_id``.
* **Operator outputs** -- the ``.output`` attribute of operators and the return value of ``@task``-decorated
  functions are resolved from ``XComArg`` to their underlying runtime type.

Installation
------------

Install the package alongside ``mypy``:

.. code-block:: bash

   pip install apache-airflow-mypy

The package follows `SemVer <https://semver.org/>`_ and can be upgraded independently of Airflow.

Configuration
-------------

Enable both plugins in ``mypy.ini``, ``setup.cfg``, or ``pyproject.toml``:

.. code-block:: ini

   [mypy]
   plugins = airflow_mypy.plugins.decorators, airflow_mypy.plugins.outputs

For example, the output plugin lets ``mypy`` infer the return type of a TaskFlow task:

.. code-block:: python

   @task
   def count_characters(value: str) -> int:
       return len(value)


   @task
   def report_count(count: int) -> None: ...


   report_count(count_characters("Airflow"))

Without the plugin, ``mypy`` sees an ``XComArg`` passed to ``report_count``. With the plugin enabled, it
understands that ``count_characters`` produces an ``int``.

.. toctree::
    :hidden:
    :caption: Reference

    release_notes
