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
from __future__ import annotations

import logging
import pathlib
import re
import sys
from collections import defaultdict
from typing import Any

from packaging.version import Version, parse as parse_version

import airflow
from airflow.configuration import AirflowConfigParser
from sphinx_exts.docs_build.third_party_inventories import THIRD_PARTY_INDEXES

AIRFLOW_REPO_ROOT_PATH = pathlib.Path(__file__).parents[4].resolve()
DOCS_PATH = AIRFLOW_REPO_ROOT_PATH / "docs"
SPELLING_WORDLIST_PATH = DOCS_PATH / "spelling_wordlist.txt"
GENERATED_PATH = AIRFLOW_REPO_ROOT_PATH / "generated"
INVENTORY_CACHE_DIR = GENERATED_PATH / "_inventory_cache"
DEVEL_COMMON_PATH = AIRFLOW_REPO_ROOT_PATH / "devel-common"
SPHINX_DESIGN_STATIC_PATH = DEVEL_COMMON_PATH / "sphinx_design" / "static"


AIRFLOW_CORE_ROOT_PATH = AIRFLOW_REPO_ROOT_PATH / "airflow-core"
AIRFLOW_CORE_DOCS_PATH = AIRFLOW_CORE_ROOT_PATH / "docs"
AIRFLOW_CORE_DOC_STATIC_PATH = AIRFLOW_CORE_DOCS_PATH / "static"
AIRFLOW_CORE_DOCKER_COMPOSE_PATH = AIRFLOW_CORE_DOCS_PATH / "howto" / "docker-compose"
AIRFLOW_CORE_SRC_PATH = AIRFLOW_CORE_ROOT_PATH / "src"
AIRFLOW_FAVICON_PATH = AIRFLOW_CORE_SRC_PATH / "airflow" / "ui" / "public" / "pin_32.png"

AIRFLOW_CTL_ROOT_PATH = AIRFLOW_REPO_ROOT_PATH / "airflow-ctl"
AIRFLOW_CTL_DOCS_PATH = AIRFLOW_CTL_ROOT_PATH / "docs"
AIRFLOW_CTL_DOC_STATIC_PATH = AIRFLOW_CTL_DOCS_PATH / "static"
AIRFLOW_CTL_SRC_PATH = AIRFLOW_CTL_ROOT_PATH / "src"

CHART_PATH = AIRFLOW_CORE_ROOT_PATH / "chart"
CHART_DOC_PATH = AIRFLOW_CORE_DOCS_PATH / "docs"

# Add sphinx_exts to the path so that sphinx can find them
sys.path.append((DEVEL_COMMON_PATH / "src" / "sphinx_exts").as_posix())


def get_global_substitutions(package_version: str, airflow_core: bool) -> dict[str, str]:
    return {
        "version": package_version,
        "airflow-version": airflow.__version__,
        "experimental": "This is an :ref:`experimental feature <experimental>`."
        if airflow_core
        else "This is an :external:ref:`experimental feature <experimental>`.",
    }


def get_rst_epilogue(package_version: str, airflow_core: bool) -> str:
    return "\n".join(
        f".. |{key}| replace:: {replace}"
        for key, replace in get_global_substitutions(package_version, airflow_core).items()
    )


SMARTQUOTES_EXCLUDES = {"builders": ["man", "text", "spelling"]}

BASIC_SPHINX_EXTENSIONS = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinxarg.ext",
    "sphinx.ext.intersphinx",
    "exampleinclude",
    "docroles",
    "removemarktransform",
    "sphinx_copybutton",
    "airflow_intersphinx",
    "sphinxcontrib.spelling",
    "sphinx_airflow_theme",
    "redirects",
    "substitution_extensions",
    "sphinx_design",
    "pagefind_search",
]

SPHINX_REDOC_EXTENSIONS = [
    "autoapi.extension",
    # First, generate redoc
    "sphinxcontrib.redoc",
    # Second, update redoc script
    "sphinx_script_update",
]

REDOC_SCRIPT_URL = "https://cdn.jsdelivr.net/npm/redoc@2.0.0-rc.48/bundles/redoc.standalone.js"


def get_rst_filepath_from_path(filepath: pathlib.Path, root: pathlib.Path):
    if filepath.is_dir():
        result = filepath
    else:
        if filepath.name == "__init__.py":
            result = filepath.parent
        else:
            result = filepath.with_name(filepath.stem)
        result /= "index.rst"

    return f"_api/{result.relative_to(root)}"


def get_html_sidebars(package_version: str) -> dict[str, list[str]]:
    """
    Get HTML sidebars for Sphinx documentation.

    :return: Dictionary of HTML sidebars.
    """
    return {
        "**": [
            "version-selector.html",
            "searchbox.html",
            "globaltoc.html",
        ]
        if package_version != "devel"
        else [
            "searchbox.html",
            "globaltoc.html",
        ]
    }


def get_html_theme_options():
    return {
        "hide_website_buttons": True,
        "sidebar_includehidden": True,
        "navbar_links": [
            {"href": "/community/", "text": "Community"},
            {"href": "/meetups/", "text": "Meetups"},
            {"href": "/docs/", "text": "Documentation"},
            {"href": "/use-cases/", "text": "Use Cases"},
            {"href": "/announcements/", "text": "Announcements"},
            {"href": "/blog/", "text": "Blog"},
            {"href": "/ecosystem/", "text": "Ecosystem"},
        ],
    }


def get_html_context(conf_py_path: str):
    return {
        # Variables used to build a button for editing the source code
        #
        # The path is created according to the following template:
        #
        # https://{{ github_host|default("github.com") }}/{{ github_user }}/{{ github_repo }}/
        # {{ theme_vcs_pageview_mode|default("blob") }}/{{ github_version }}{{ conf_py_path }}
        # {{ pagename }}{{ suffix }}
        #
        # More information:
        # https://github.com/readthedocs/readthedocs.org/blob/master/readthedocs/doc_builder/templates/doc_builder/conf.py.tmpl#L100-L103
        # https://github.com/readthedocs/sphinx_rtd_theme/blob/master/sphinx_rtd_theme/breadcrumbs.html#L45
        # https://github.com/apache/airflow-site/blob/91f760c/sphinx_airflow_theme/sphinx_airflow_theme/suggest_change_button.html#L36-L40
        #
        "theme_vcs_pageview_mode": "edit",
        "conf_py_path": conf_py_path,
        "github_user": "apache",
        "github_repo": "airflow",
        "github_version": "main",
        "display_github": "main",
        "suffix": ".rst",
    }


def get_configs_and_deprecations(
    package_version: Version,
    config_descriptions: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, tuple[str, str, str]]], dict[str, dict[str, tuple[str, str, str]]]]:
    deprecated_options: dict[str, dict[str, tuple[str, str, str]]] = defaultdict(dict)
    for (section, key), (
        (deprecated_section, deprecated_key, since_version)
    ) in AirflowConfigParser.deprecated_options.items():
        deprecated_options[deprecated_section][deprecated_key] = section, key, since_version
    # We want the default/example we show in the docs to reflect the value _after_
    # the config has been templated, not before
    # e.g. {{dag_id}} in default_config.cfg -> {dag_id} in airflow.cfg, and what we want in docs
    keys_to_format = ["default", "example"]
    for conf_section in config_descriptions.values():
        for option_name, option in list(conf_section["options"].items()):
            for key in keys_to_format:
                if option[key] and "{{" in option[key]:
                    option[key] = option[key].replace("{{", "{").replace("}}", "}")
            version_added = option["version_added"]
            if version_added is not None and parse_version(version_added) > package_version:
                del conf_section["options"][option_name]

    # Sort options, config and deprecated options for JINJA variables to display
    for config in config_descriptions.values():
        config["options"] = {k: v for k, v in sorted(config["options"].items())}
    configs = {k: v for k, v in sorted(config_descriptions.items())}
    for section in deprecated_options:
        deprecated_options[section] = {k: v for k, v in sorted(deprecated_options[section].items())}
    return configs, deprecated_options


def get_autodoc_mock_imports() -> list[str]:
    return [
        "MySQLdb",
        "adal",
        "alibabacloud_adb20211201",
        "alibabacloud_tea_openapi",
        "analytics",
        "azure",
        "azure.cosmos",
        "azure.datalake",
        "azure.kusto",
        "azure.mgmt",
        "boto3",
        "botocore",
        "bson",
        "cassandra",
        "celery",
        "cloudant",
        "cryptography",
        "datadog",
        "distributed",
        "docker",
        "google",
        "google_auth_httplib2",
        "googleapiclient",
        "grpc",
        "hdfs",
        "httplib2",
        "jaydebeapi",
        "jenkins",
        "jira",
        "kubernetes",
        "msrestazure",
        "oss2",
        "oracledb",
        "pandas",
        "pandas_gbq",
        "paramiko",
        "pinotdb",
        "psycopg",
        "psycopg2",
        "pydruid",
        "pyhive",
        "pyhive",
        "pymongo",
        "pymssql",
        "pysftp",
        "qds_sdk",
        "redis",
        "simple_salesforce",
        "slack_sdk",
        "smbclient",
        "snowflake",
        "sqlalchemy-drill",
        "sshtunnel",
        "telegram",
        "tenacity",
        "vertica_python",
        "winrm",
        "zenpy",
    ]


def get_intersphinx_mapping() -> dict[str, tuple[str, tuple[str]]]:
    return {
        pkg_name: (f"{THIRD_PARTY_INDEXES[pkg_name]}/", (f"{INVENTORY_CACHE_DIR}/{pkg_name}/objects.inv",))
        for pkg_name in [
            "boto3",
            "celery",
            "docker",
            "hdfs",
            "jinja2",
            "mongodb",
            "pandas",
            "python",
            "requests",
            "sqlalchemy",
        ]
    }


def get_google_intersphinx_mapping() -> dict[str, tuple[str, tuple[str]]]:
    return {
        pkg_name: (
            f"{THIRD_PARTY_INDEXES[pkg_name]}/",
            (f"{INVENTORY_CACHE_DIR}/{pkg_name}/objects.inv",),
        )
        for pkg_name in [
            "google-api-core",
            "google-cloud-automl",
            "google-cloud-bigquery",
            "google-cloud-bigquery-datatransfer",
            "google-cloud-bigquery-storage",
            "google-cloud-bigtable",
            "google-cloud-container",
            "google-cloud-core",
            "google-cloud-datacatalog",
            "google-cloud-datastore",
            "google-cloud-dlp",
            "google-cloud-kms",
            "google-cloud-language",
            "google-cloud-monitoring",
            "google-cloud-pubsub",
            "google-cloud-redis",
            "google-cloud-spanner",
            "google-cloud-speech",
            "google-cloud-storage",
            "google-cloud-tasks",
            "google-cloud-texttospeech",
            "google-cloud-translate",
            "google-cloud-videointelligence",
            "google-cloud-vision",
        ]
    }


BASIC_AUTOAPI_IGNORE_PATTERNS = [
    "*/airflow/_vendor/*",
    "*/airflow/executors/*",
    "*/_internal*",
    "*/node_modules/*",
    "*/migrations/*",
    "*/contrib/*",
    "*/example_taskflow_api_docker_virtualenv.py",
    "*/example_dag_decorator.py",
    "*/conftest.py",
    "*/tests/__init__.py",
    "*/tests/system/__init__.py",
    "*/tests/system/*/tests/*",
    "*/tests/system/example_empty.py",
]

IGNORE_PATTERNS_RECOGNITION = re.compile(r"\[AutoAPI\] .* Ignoring \s (?P<path>/[\w/.]*)", re.VERBOSE)


# Make the "Ignoring /..." log messages slightly less verbose
def filter_autoapi_ignore_entries(record: logging.LogRecord) -> bool:
    matches = IGNORE_PATTERNS_RECOGNITION.search(record.msg)
    if not matches:
        return True
    if matches["path"].endswith("__init__.py"):
        record.msg = record.msg.replace("__init__.py", "")
        return True
    return False


AUTOAPI_OPTIONS = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
]

SUPPRESS_WARNINGS = [
    "autoapi.python_import_resolution",
]


def skip_util_classes_extension(app, what, name, obj, skip, options):
    if what == "data" and "STATICA_HACK" in name:
        skip = True
    elif ":sphinx-autoapi-skip:" in obj.docstring:
        skip = True
    elif ":meta private:" in obj.docstring:
        skip = True
    return skip
