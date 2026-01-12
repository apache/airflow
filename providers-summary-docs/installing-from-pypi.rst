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

This page describes installations using the ``apache-airflow-providers`` package `published in
PyPI <https://pypi.org/search/?q=apache-airflow-providers>`__.

Installation tools
''''''''''''''''''

Only ``pip`` and ``uv`` installation is officially supported.

.. note::

  While there are some successes with using other tools like `poetry <https://python-poetry.org/>`_ or
  `pip-tools <https://pypi.org/project/pip-tools/>`_, they do not share the same workflow as
  ``pip`` - especially when it comes to constraint vs. requirements management.
  Installing via ``Poetry`` or ``pip-tools`` is not currently supported. If you wish to install airflow
  using those tools you should use the constraints and convert them to appropriate
  format and workflow that your tool requires.

  There are known issues with ``bazel`` that might lead to circular dependencies when using it to install
  Airflow. Please switch to ``pip`` if you encounter such problems. ``Bazel`` community works on fixing
  the problem in `this PR <https://github.com/bazelbuild/rules_python/pull/1166>`_ so it might be that
  newer versions of ``bazel`` will handle it.


Typical command to install Airflow from PyPI looks like below (you need to use the right Airflow version and Python version):

.. code-block::

    pip install "apache-airflow-providers-celery" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.10.txt"

This is an example, see :doc:`apache-airflow:installation/installing-from-pypi` for more examples, including how to upgrade the providers.
