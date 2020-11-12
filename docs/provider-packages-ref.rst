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

Providers packages reference
''''''''''''''''''''''''''''

Here's the list of the :doc:`provider packages <provider-packages>` and what they enable:


.. contents:: :local:

.. jinja:: providers_ctx

    {% for package in providers %}

    .. provider:: {{ package["package-name"] }}

    ``{{ package["package-name"] }}``
    {{ "=" * (package["package-name"]|length + 4) }}

    {% if package["description"] %}
    {{ package["description"] }}
    {% endif %}

    {% if package["versions"] %}
    To install, run:

    .. code-block:: bash

        pip install '{{ package["package-name"] }}'

    :Available versions: {% for version in package["versions"] %}``{{ version }}``{% if not loop.last %}, {% else %}.{% endif %}{%- endfor %}

    :Reference: `PyPI Repository <https://pypi.org/project/{{ package["package-name"] }}/>`__
    {% if package["python-module"] %}
    :Python API Reference: :mod:`{{ package["python-module"] }}`
    {% endif %}
    {% else %}

    .. warning::

      This package has not yet been released.

    {% if package["python-module"] %}
    :Python API Reference: :mod:`{{ package["python-module"] }}`
    {% endif %}
    {% endif %}
    {% endfor %}
