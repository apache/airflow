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

.. _howto/static-type-checking:

Static Type Checking for Dags
=============================

Airflow publishes a set of `mypy <https://mypy-lang.org/>`_ plugins as a standalone, independently
versioned distribution: `apache-airflow-mypy <https://pypi.org/project/apache-airflow-mypy/>`_.

When to use it
--------------

If you run ``mypy`` over your Dags, custom operators, or hooks, install the plugins to get accurate
results for Airflow-specific patterns that plain ``mypy`` cannot reason about and would otherwise report
as false positives. The plugins teach ``mypy`` about:

* **Typed decorators** -- decorators that inject keyword arguments at runtime (for example
  ``GoogleBaseHook.fallback_to_default_project_id``), so ``mypy`` does not flag those arguments as missing.
* **Operator outputs** -- the ``.output`` attribute of operators and the return value of ``@task``-decorated
  functions (an ``XComArg``) are resolved to the underlying runtime type. This lets you wire a task's output
  into a downstream task without spurious type errors:

  .. code-block:: python

     @task
     def f(a: str) -> int:
         return len(a)


     @task
     def g(b: int) -> None: ...


     g(f("hello"))  # mypy understands the output of f() is an int

The package is entirely optional -- Airflow does not require it at runtime; it only improves the accuracy
of static type checking for Dag authors.

Installation
------------

Install it alongside ``mypy``:

.. code-block:: bash

   pip install apache-airflow-mypy

The package follows `SemVer <https://semver.org/>`_ and is released on its own cadence, so you can adopt it
independently of your Airflow version.

Configuration
-------------

Enable the plugins in your ``mypy`` configuration (``mypy.ini``, ``setup.cfg`` or ``pyproject.toml``):

.. code-block:: ini

   [mypy]
   plugins = airflow_mypy.plugins.decorators, airflow_mypy.plugins.outputs
