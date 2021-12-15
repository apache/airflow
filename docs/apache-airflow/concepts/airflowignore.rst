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

``.airflowignore``
==================

A ``.airflowignore`` file specifies the directories or files in ``DAG_FOLDER``
or ``PLUGINS_FOLDER`` that Airflow should intentionally ignore.
Each line in ``.airflowignore`` specifies a regular expression pattern,
and directories or files whose names (not DAG id) match any of the patterns
would be ignored (under the hood, ``Pattern.search()`` is used to match the pattern).
Overall it works like a ``.gitignore`` file.
Use the ``#`` character to indicate a comment; all characters
on a line following a ``#`` will be ignored.

``.airflowignore`` file should be put in your ``DAG_FOLDER``.
For example, you can prepare a ``.airflowignore`` file with content

.. code-block::

    project_a
    tenant_[\d]

Then files like ``project_a_dag_1.py``, ``TESTING_project_a.py``, ``tenant_1.py``,
``project_a/dag_1.py``, and ``tenant_1/dag_1.py`` in your ``DAG_FOLDER`` would be ignored
(If a directory's name matches any of the patterns, this directory and all its subfolders
would not be scanned by Airflow at all. This improves efficiency of DAG finding).

The scope of a ``.airflowignore`` file is the directory it is in plus all its subfolders.
You can also prepare ``.airflowignore`` file for a subfolder in ``DAG_FOLDER`` and it
would only be applicable for that subfolder.