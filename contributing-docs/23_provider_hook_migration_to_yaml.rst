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

Provider hook to YAML Migration
===============================

We can now, redefine connection form metadata declaratively in ``provider.yaml`` of a provider instead of Python hook code,
reducing dependencies and improving API server startup performance.

**The outline for this document in GitHub is available at top-right corner button (with 3-dots and 3 lines).**

Background
----------

Previously, connection form UI metadata was defined in hook code of a provider using:

* ``get_connection_form_widgets()`` - Custom form fields
* ``get_ui_field_behaviour()`` - Field customizations (hidden, relabeling, placeholders)

These methods required importing heavy dependencies ``flask_appbuilder`` and ``wtforms``, adding unnecessary dependencies
to the API server and having API server to load all the provider hook code just to display a static form.
The new yaml approach allows metadata to be loaded without importing hook classes.


YAML Schema Structure
---------------------

Connection metadata is defined under ``connection-types`` in ``provider.yaml`` of a provider:

ui-field-behaviour
~~~~~~~~~~~~~~~~~~

Customizations for standard connection fields:

.. code-block:: yaml

    ui-field-behaviour:
      hidden-fields:
        - schema
        - extra
      relabeling:
        host: Registry URL
        login: Username
      placeholders:
        port: '5432'

conn-fields
~~~~~~~~~~~

Custom fields which will be stored within ``Connection.extra``. The ``schema`` property uses
`JSON Schema <https://json-schema.org/>`_ to define field types and validation. For details on
supported field options, see
`Use Params to Provide a Trigger UI Form <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/params.html#use-params-to-provide-a-trigger-ui-form>`_.

.. code-block:: yaml

    conn-fields:
      keyfile_dict:
        label: "Keyfile JSON"
        description: "Service account JSON key"
        schema:
          type: string
          format: password
      project:
        label: "Project Id"
        schema:
          type: string
          default: "my-project"

Migration Tool
--------------

The ``generate_yaml_format_for_hooks.py`` script extracts metadata from existing Python hook code.
Use the airflow virtual environment to run the script.

Basic Usage
~~~~~~~~~~~

Extract from a provider:

.. code-block:: bash

    python scripts/generate_yaml_format_for_hooks.py --provider docker

Extract from a specific hook (some providers can have many hooks):

.. code-block:: bash

    python scripts/generate_yaml_format_for_hooks.py \
      --hook-class airflow.providers.docker.hooks.docker.DockerHook

Update provider.yaml directly:

.. code-block:: bash

    python scripts/generate_yaml_format_for_hooks.py --provider docker --update-yaml
