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

.. contents:: Sections:
   :local:
   :depth: 1

.. jinja:: config_ctx

    {% for section_name, section in configs.items() %}

    .. _config:{{ section_name }}:

    [{{ section_name }}]
    {{ "=" * (section_name|length + 2) }}

    {% if 'renamed' in section %}
    *Renamed in version {{ section['renamed']['version'] }}, previous name was {{ section['renamed']['previous_name'] }}*
    {% endif %}

    {% if section["description"] %}
    {{ section["description"] }}
    {% endif %}

    {% for option_name, option in section["options"].items() %}
    .. _config:{{ section_name }}__{{ option_name }}:

    {{ option_name }}
    {{ "-" * option_name|length }}

        {% if option["version_added"] %}
     .. versionadded:: {{ option["version_added"] }}
        {% endif %}

        {% if option["description"] %}
    {{ option["description"] }}
        {% endif %}

        {% if option.get("version_deprecated") %}
    .. deprecated:: {{ option["version_deprecated"] }}
        {{ option["deprecation_reason"] | indent(width=8) }}
        {% endif %}

        {% if option.get("see_also") %}
    .. seealso:: {{ option["see_also"] }}
        {% endif %}

    :Type: {{ option["type"] }}
    :Default:
          {% set default = option["default"] %}
          {% if default and "\n" in default %}
      .. code-block::

        {{ default }}
          {% else %}
        ``{{ "''" if default == "" else default  }}``
          {% endif %}
        {% if option.get("sensitive") %}
    :Environment Variables:
      ``AIRFLOW__{{ section_name | replace(".", "_") | upper }}__{{ option_name | upper }}``

      ``AIRFLOW__{{ section_name | replace(".", "_") | upper }}__{{ option_name | upper }}_CMD``

      ``AIRFLOW__{{ section_name | replace(".", "_") | upper }}__{{ option_name | upper }}_SECRET``
        {% else %}
    :Environment Variable: ``AIRFLOW__{{ section_name | replace(".", "_") | upper }}__{{ option_name | upper }}``
        {% endif %}
        {% set example = option["example"] %}
        {% if example %}
    :Example:
          {% if "\n" in example %}
      .. code-block::

        {{ example }}
          {% else %}
        ``{{ example }}``
          {% endif %}
        {% endif %}
    {% endfor %}

    {% if section_name in deprecated_options %}

    {% for deprecated_option_name, (new_section_name, new_option_name, since_version) in deprecated_options[section_name].items() %}
    .. _config:{{ section_name }}__{{ deprecated_option_name }}:

    {{ deprecated_option_name }} (Deprecated)
    {{ "-" * (deprecated_option_name + " (Deprecated)")|length }}

    .. deprecated:: {{ since_version }}
     The option has been moved to :ref:`{{ new_section_name }}.{{ new_option_name }} <config:{{ new_section_name }}__{{ new_option_name }}>`
    {% endfor %}
    {% endif %}

    {% endfor %}
