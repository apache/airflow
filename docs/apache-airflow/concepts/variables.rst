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

Variables
=========

Variables are Airflow's runtime configuration concept - a general key/value store that is global and can be queried from your tasks, and easily set via Airflow's user interface, or bulk-uploaded as a JSON file.

To use them, just import and call ``get`` on the Variable model::

    from airflow.models import Variable

    # Normal call style
    foo = Variable.get("foo")

    # Auto-deserializes a JSON value
    bar = Variable.get("bar", deserialize_json=True)

    # Returns the value of default_var (None) if the variable is not set
    baz = Variable.get("baz", default_var=None)

You can also use them from :ref:`templates <concepts:jinja-templating>`::

    # Raw value
    echo {{ var.value.<variable_name> }}

    # Auto-deserialize JSON value
    echo {{ var.json.<variable_name> }}

Variables are **global**, and should only be used for overall configuration that covers the entire installation; to pass data from one Task/Operator to another, you should use :doc:`xcoms` instead.

We also recommend that you try to keep most of your settings and configuration in your DAG files, so it can be versioned using source control; Variables are really only for values that are truly runtime-dependent.

For more information on setting and managing variables, see :doc:`/howto/variable`.
