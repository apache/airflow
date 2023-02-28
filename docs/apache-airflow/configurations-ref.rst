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

Use the same configuration across all the Airflow components. While each component
does not require all, some configurations need to be same otherwise they would not
work as expected. A good example for that is :ref:`secret_key<config:webserver__secret_key>` which
should be same on the Webserver and Worker to allow Webserver to fetch logs from Worker.

The webserver key is also used to authorize requests to Celery workers when logs are retrieved. The token
generated using the secret key has a short expiry time though - make sure that time on ALL the machines
that you run airflow components on is synchronized (for example using ntpd) otherwise you might get
"forbidden" errors when the logs are accessed.

.. note::
    For more information on setting the configuration, see :doc:`howto/set-config`

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

    {% if option.get("see_also") %}
    .. seealso:: {{ option["see_also"] }}
    {% endif %}

    :Type: {{ option["type"] }}
    :Default: ``{{ "''" if option["default"] == "" else option["default"] }}``
    {% if option.get("sensitive") %}
    :Environment Variables:
        ``AIRFLOW__{{ section_name | upper }}__{{ option_name | upper }}``

        ``AIRFLOW__{{ section_name | upper }}__{{ option_name | upper }}_CMD``

        ``AIRFLOW__{{ section_name | upper }}__{{ option_name | upper }}_SECRET``
    {% else %}
    :Environment Variable: ``AIRFLOW__{{ section_name | upper }}__{{ option_name | upper }}``
    {% endif %}
    {% if option["example"] %}
    :Example:
        ``{{ option["example"] }}``
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
