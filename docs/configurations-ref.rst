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


Configuration Reference
=======================

This page contains the list of all the available Airflow configurations that you
can set in ``airflow.cfg`` file or using environment variables.

.. note::
    For more information on setting the configuration, see :doc:`howto/set-config`

.. contents:: Sections:
   :local:
   :depth: 1

.. jinja:: config_ctx

    {% for section in configs %}

    .. _config:{{ section["name"] }}:

    [{{ section["name"] }}]
    {{ "=" * (section["name"]|length + 2) }}

    {% if section["description"] %}
    {{ section["description"] }}
    {% endif %}

    {% for option in section["options"] %}

    .. _config:{{ section["name"] }}__{{ option["name"] }}:

    {{ option["name"] }}
    {{ "-" * option["name"]|length }}

    {% if option["version_added"] %}
    .. versionadded:: {{ option["version_added"] }}
    {% endif %}

    {% if option["description"] %}
    {{ option["description"] }}
    {% endif %}

    {% if option.get("see_also") %}
    .. seealso:: {{ option["see_also"] }}
    {% endif %}

    :Type: {{ option["type"] }}
    :Default: ``{{ "''" if option["default"] == "" else option["default"] }}``
    :Environment Variable: ``AIRFLOW__{{ section["name"] | upper }}__{{ option["name"] | upper }}``
    {% if option["example"] %}
    :Example:
        ``{{ option["example"] }}``
    {% endif %}

    {% endfor %}
    {% endfor %}
