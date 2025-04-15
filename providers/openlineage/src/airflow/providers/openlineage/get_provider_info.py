# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# NOTE! THIS FILE IS AUTOMATICALLY GENERATED AND WILL BE OVERWRITTEN!
#
# IF YOU WANT TO MODIFY THIS FILE, YOU SHOULD MODIFY THE TEMPLATE
# `get_provider_info_TEMPLATE.py.jinja2` IN the `dev/breeze/src/airflow_breeze/templates` DIRECTORY


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-openlineage",
        "name": "OpenLineage Airflow",
        "description": "`OpenLineage <https://openlineage.io/>`__\n",
        "integrations": [
            {
                "integration-name": "OpenLineage",
                "external-doc-url": "https://openlineage.io",
                "logo": "/docs/integration-logos/openlineage.svg",
                "tags": ["protocol"],
            }
        ],
        "plugins": [
            {
                "name": "openlineage",
                "plugin-class": "airflow.providers.openlineage.plugins.openlineage.OpenLineageProviderPlugin",
            }
        ],
        "config": {
            "openlineage": {
                "description": "This section applies settings for OpenLineage integration.\nMore about configuration and it's precedence can be found in the `user's guide\n<https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html#transport-setup>`_.\n",
                "options": {
                    "disabled": {
                        "description": "Disable sending events without uninstalling the OpenLineage Provider by setting this to true.\n",
                        "type": "boolean",
                        "example": None,
                        "default": "False",
                        "version_added": None,
                    },
                    "disabled_for_operators": {
                        "description": "Exclude some Operators from emitting OpenLineage events by passing a string of semicolon separated\nfull import paths of Operators to disable.\n",
                        "type": "string",
                        "example": "airflow.providers.standard.operators.bash.BashOperator; airflow.providers.standard.operators.python.PythonOperator",
                        "default": "",
                        "version_added": "1.1.0",
                    },
                    "selective_enable": {
                        "description": "If this setting is enabled, OpenLineage integration won't collect and emit metadata,\nunless you explicitly enable it per `DAG` or `Task` using  `enable_lineage` method.\n",
                        "type": "boolean",
                        "default": "False",
                        "example": None,
                        "version_added": "1.7.0",
                    },
                    "namespace": {
                        "description": "Set namespace that the lineage data belongs to, so that if you use multiple OpenLineage producers,\nevents coming from them will be logically separated.\n",
                        "version_added": None,
                        "type": "string",
                        "example": "my_airflow_instance_1",
                        "default": None,
                    },
                    "extractors": {
                        "description": "Register custom OpenLineage Extractors by passing a string of semicolon separated full import paths.\n",
                        "type": "string",
                        "example": "full.path.to.ExtractorClass;full.path.to.AnotherExtractorClass",
                        "default": None,
                        "version_added": None,
                    },
                    "custom_run_facets": {
                        "description": "Register custom run facet functions by passing a string of semicolon separated full import paths.\n",
                        "type": "string",
                        "example": "full.path.to.custom_facet_function;full.path.to.another_custom_facet_function",
                        "default": "",
                        "version_added": "1.10.0",
                    },
                    "config_path": {
                        "description": "Specify the path to the YAML configuration file.\nThis ensures backwards compatibility with passing config through the `openlineage.yml` file.\n",
                        "version_added": None,
                        "type": "string",
                        "example": "full/path/to/openlineage.yml",
                        "default": "",
                    },
                    "transport": {
                        "description": "Pass OpenLineage Client transport configuration as a JSON string, including the transport type\nand any additional options specific to that type, as described in `OpenLineage docs\n<https://openlineage.io/docs/client/python/#built-in-transport-types>`_.\n\nCurrently supported types are:\n\n  * HTTP\n  * Kafka\n  * Console\n  * File\n  * Composite\n  * Custom\n",
                        "type": "string",
                        "example": '{"type": "http", "url": "http://localhost:5000", "endpoint": "api/v1/lineage"}',
                        "default": "",
                        "version_added": None,
                    },
                    "disable_source_code": {
                        "description": "Disable the inclusion of source code in OpenLineage events by setting this to `true`.\nBy default, several Operators (e.g. Python, Bash) will include their source code in the events\nunless disabled.\n",
                        "default": "False",
                        "example": None,
                        "type": "boolean",
                        "version_added": None,
                    },
                    "dag_state_change_process_pool_size": {
                        "description": "Number of processes to utilize for processing DAG state changes\nin an asynchronous manner within the scheduler process.\n",
                        "default": "1",
                        "example": None,
                        "type": "integer",
                        "version_added": "1.8.0",
                    },
                    "execution_timeout": {
                        "description": "Maximum amount of time (in seconds) that OpenLineage can spend executing metadata extraction.\nNote that other configurations, sometimes with higher priority, such as\n`[core] task_success_overtime\n<https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#task-success-overtime>`_,\nmay also affect how much time OpenLineage has for execution.\n",
                        "default": "10",
                        "example": None,
                        "type": "integer",
                        "version_added": "1.9.0",
                    },
                    "include_full_task_info": {
                        "description": "If true, OpenLineage event will include full task info - potentially containing large fields.\n",
                        "default": "False",
                        "example": None,
                        "type": "boolean",
                        "version_added": "1.10.0",
                    },
                    "debug_mode": {
                        "description": "If true, OpenLineage events will include information useful for debugging - potentially\ncontaining large fields e.g. all installed packages and their versions.\n",
                        "default": "False",
                        "example": None,
                        "type": "boolean",
                        "version_added": "1.11.0",
                    },
                    "spark_inject_parent_job_info": {
                        "description": "Automatically inject OpenLineage's parent job (namespace, job name, run id) information into Spark\napplication properties for supported Operators.\n",
                        "type": "boolean",
                        "default": "False",
                        "example": None,
                        "version_added": "2.0.0",
                    },
                    "spark_inject_transport_info": {
                        "description": "Automatically inject OpenLineage's transport information into Spark application properties\nfor supported Operators.\n",
                        "type": "boolean",
                        "default": "False",
                        "example": None,
                        "version_added": "2.1.0",
                    },
                },
            }
        },
    }
