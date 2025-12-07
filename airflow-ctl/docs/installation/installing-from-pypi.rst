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

Installation from PyPI
----------------------

This page describes installations using the ``apache-airflow-ctl`` package `published in
PyPI <https://pypi.org/project/apache-airflow-ctl/>`__.

Installation tools
''''''''''''''''''

Only ``pip`` and ``uv`` installation is currently officially supported.

.. note::

  While there are some successes with using other tools like `poetry <https://python-poetry.org/>`_ or
  `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
  ``pip`` - especially when it comes to constraint vs. requirements management.
  Installing via ``Poetry`` or ``pip-tools`` is not currently supported. If you wish to install airflow
  using those tools you should use the constraints and convert them to appropriate
  format and workflow that your tool requires. Uv follows ``pip`` approach
  with ``uv pip`` so it should work similarly.

  There are known issues with ``bazel`` that might lead to circular dependencies when using it to install
  Airflow. Please switch to ``pip`` if you encounter such problems. ``Bazel`` community works on fixing
  the problem in `this PR <https://github.com/bazelbuild/rules_python/pull/1166>`_ so it might be that
  newer versions of ``bazel`` will handle it.

Typical command to install airflowctl from scratch in a reproducible way from PyPI looks like below:

.. code-block:: bash

    pip install "apache-airflow-ctl==|version|"

Those are just examples, see further for more explanation why those are the best practices.

.. note::

   Generally speaking, Python community established practice is to perform application installation in a
   virtualenv created with ``virtualenv`` or ``venv`` tools. You can also use ``uv`` or ``pipx`` to install
   Airflow in application dedicated virtual environment created for you. There are also other tools that can be used
   to manage your virtualenv installation and you are free to choose how you are managing the environments.
   Airflow has no limitation regarding to the tool of your choice when it comes to virtual environment.

   The only exception where you might consider not using virtualenv is when you are building a container
   image with only Airflow installed - this is for example how Airflow is installed in the official Container
   image.

.. _installation:constraints:
