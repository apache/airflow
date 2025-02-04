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

airflow.providers.cncf.kubernetes.python_kubernetes_script
==========================================================

.. py:module:: airflow.providers.cncf.kubernetes.python_kubernetes_script

.. autoapi-nested-parse::

   Utilities for using the kubernetes decorator.



Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.python_kubernetes_script.remove_task_decorator
   airflow.providers.cncf.kubernetes.python_kubernetes_script.write_python_script


Module Contents
---------------

.. py:function:: remove_task_decorator(python_source, task_decorator_name)

   Remove @task.kubernetes or similar as well as @setup and @teardown.

   :param python_source: python source code
   :param task_decorator_name: the task decorator name


.. py:function:: write_python_script(jinja_context, filename, render_template_as_native_obj = False)

   Render the python script to a file to execute in the virtual environment.

   :param jinja_context: The jinja context variables to unpack and replace with its placeholders in the
       template file.
   :param filename: The name of the file to dump the rendered script to.
   :param render_template_as_native_obj: If ``True``, rendered Jinja template would be converted
       to a native Python object
