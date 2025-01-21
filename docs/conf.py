# Disable Flake8 because of all the sphinx imports
#
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
"""Configuration of Airflow Docs"""

from __future__ import annotations

# Airflow documentation build configuration file, created by
# sphinx-quickstart on Thu Oct  9 20:50:01 2014.
#
# This file is execfile()d with the current directory set to its
# containing dir.
#
# Note that not all possible configuration values are present in this
# autogenerated file.
#
# All configuration values have a default; values that are commented out
# serve to show the default.
import json
import logging
import os
import pathlib
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml
from packaging.version import Version, parse as parse_version

import airflow
from airflow.configuration import AirflowConfigParser, retrieve_configuration_description

sys.path.append(str(Path(__file__).parent / "exts"))

from docs_build.third_party_inventories import THIRD_PARTY_INDEXES

CONF_DIR = pathlib.Path(__file__).parent.absolute()
INVENTORY_CACHE_DIR = CONF_DIR / "_inventory_cache"
ROOT_DIR = CONF_DIR.parent

# By default (e.g. on RTD), build docs for `airflow` package
PACKAGE_NAME = os.environ.get("AIRFLOW_PACKAGE_NAME", "apache-airflow")
PACKAGE_DIR: pathlib.Path
SYSTEM_TESTS_DIR: pathlib.Path | None

conf_py_path = f"/docs/{PACKAGE_NAME}/"

if PACKAGE_NAME == "apache-airflow":
    PACKAGE_DIR = ROOT_DIR / "airflow"
    PACKAGE_VERSION = airflow.__version__
    SYSTEM_TESTS_DIR = (ROOT_DIR / "tests" / "system" / "core").resolve(strict=True)
elif PACKAGE_NAME.startswith("apache-airflow-providers-"):
    from provider_yaml_utils import load_package_data

    ALL_PROVIDER_YAMLS = load_package_data(include_suspended=True)
    try:
        CURRENT_PROVIDER = next(
            provider_yaml
            for provider_yaml in ALL_PROVIDER_YAMLS
            if provider_yaml["package-name"] == PACKAGE_NAME
        )
    except StopIteration:
        raise RuntimeError(f"Could not find provider.yaml file for package: {PACKAGE_NAME}")

    # Oddity: since we set autoapi_python_use_implicit_namespaces for provider packages, it does a "../"on the
    # dir we give it. So we want to set the package dir to be airflow so it goes up to src, else we end up
    # with "src" in the output paths of modules which we don't want

    package_id = PACKAGE_NAME[len("apache-airflow-providers-") :].replace("-", ".")
    # TODO(potiuk) - remove the if when all providers are new-style
    if CURRENT_PROVIDER["is_new_provider"]:
        base_provider_dir = (ROOT_DIR / "providers").joinpath(*package_id.split("."))
        PACKAGE_DIR = base_provider_dir / "src" / "airflow"
        PACKAGE_VERSION = CURRENT_PROVIDER["versions"][0]
        SYSTEM_TESTS_DIR = base_provider_dir / "tests" / "system"
        target_dir = ROOT_DIR / "docs" / PACKAGE_NAME
        conf_py_path = f"/providers/{package_id.replace('.', '/')}/docs/"
    else:
        PACKAGE_DIR = ROOT_DIR / "providers" / "src" / "airflow"
        PACKAGE_VERSION = CURRENT_PROVIDER["versions"][0]
        SYSTEM_TESTS_DIR = ROOT_DIR / "providers" / "tests" / "system"
elif PACKAGE_NAME == "apache-airflow-providers":
    from provider_yaml_utils import load_package_data

    PACKAGE_DIR = ROOT_DIR / "providers" / "src"
    PACKAGE_VERSION = "devel"
    ALL_PROVIDER_YAMLS = load_package_data()
    SYSTEM_TESTS_DIR = None
elif PACKAGE_NAME == "helm-chart":
    PACKAGE_DIR = ROOT_DIR / "chart"
    chart_yaml_file = PACKAGE_DIR / "Chart.yaml"

    with chart_yaml_file.open() as chart_file:
        chart_yaml_contents = yaml.safe_load(chart_file)

    PACKAGE_VERSION = chart_yaml_contents["version"]
    SYSTEM_TESTS_DIR = None
else:
    PACKAGE_VERSION = "devel"
    SYSTEM_TESTS_DIR = None
# Adds to environment variables for easy access from other plugins like airflow_intersphinx.
os.environ["AIRFLOW_PACKAGE_NAME"] = PACKAGE_NAME

# Hack to allow changing for piece of the code to behave differently while
# the docs are being built. The main objective was to alter the
# behavior of the utils.apply_default that was hiding function headers
os.environ["BUILDING_AIRFLOW_DOCS"] = "TRUE"

# Use for generate rst_epilog and other post-generation substitutions
global_substitutions = {
    "version": PACKAGE_VERSION,
    "airflow-version": airflow.__version__,
    "experimental": "This is an :ref:`experimental feature <experimental>`.",
}

if PACKAGE_NAME != "apache-airflow":
    global_substitutions["experimental"] = "This is an :external:ref:`experimental feature <experimental>`."


# == Sphinx configuration ======================================================

# -- Project information -------------------------------------------------------
# See: https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

# General information about the project.
project = PACKAGE_NAME
# # The version info for the project you're documenting
version = PACKAGE_VERSION
# The full version, including alpha/beta/rc tags.
release = PACKAGE_VERSION

# -- General configuration -----------------------------------------------------
# See: https://www.sphinx-doc.org/en/master/usage/configuration.html

rst_epilog = "\n".join(f".. |{key}| replace:: {replace}" for key, replace in global_substitutions.items())

smartquotes_excludes = {"builders": ["man", "text", "spelling"]}

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
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
]
if PACKAGE_NAME == "apache-airflow":
    extensions.extend(
        [
            "sphinx_jinja",
            "sphinx.ext.graphviz",
            "sphinxcontrib.httpdomain",
            "sphinxcontrib.httpdomain",
            "extra_files_with_substitutions",
            # First, generate redoc
            "sphinxcontrib.redoc",
            # Second, update redoc script
            "sphinx_script_update",
        ]
    )

if PACKAGE_NAME == "apache-airflow-providers":
    extensions.extend(
        [
            "sphinx_jinja",
            "operators_and_hooks_ref",
            "providers_packages_ref",
        ]
    )
elif PACKAGE_NAME == "helm-chart":
    extensions.append("sphinx_jinja")
elif PACKAGE_NAME == "docker-stack":
    extensions.append("extra_files_with_substitutions")
elif PACKAGE_NAME.startswith("apache-airflow-providers-"):
    extensions.extend(
        [
            "extra_provider_files_with_substitutions",
            "autoapi.extension",
            "providers_extensions",
        ]
    )
else:
    extensions.append("autoapi.extension")
# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns: list[str]
if PACKAGE_NAME == "apache-airflow":
    exclude_patterns = [
        # We only link to selected subpackages.
        "_api/airflow/index.rst",
        # Included in the cluster-policies doc
        "_api/airflow/policies/index.rst",
        "README.rst",
    ]
elif PACKAGE_NAME.startswith("apache-airflow-providers-"):
    extensions.extend(
        [
            "sphinx_jinja",
        ]
    )
    exclude_patterns = ["operators/_partials"]
else:
    exclude_patterns = []


def _get_rst_filepath_from_path(filepath: pathlib.Path):
    if filepath.is_dir():
        result = filepath
    else:
        if filepath.name == "__init__.py":
            result = filepath.parent
        else:
            result = filepath.with_name(filepath.stem)
        result /= "index.rst"

    return f"_api/{result.relative_to(ROOT_DIR)}"


if PACKAGE_NAME == "apache-airflow":
    # Exclude top-level packages
    # do not exclude these top-level modules from the doc build:
    _allowed_top_level = ("exceptions.py", "policies.py")

    browsable_packages = {
        "hooks",
        "decorators",
        "example_dags",
        "executors",
        "models",
        "operators",
        "providers",
        "secrets",
        "sensors",
        "timetables",
        "triggers",
        "utils",
    }
    browsable_utils: set[str] = {
        "state.py",
    }

    models_included: set[str] = {
        "baseoperator.py",
        "baseoperatorlink.py",
        "connection.py",
        "dag.py",
        "dagrun.py",
        "dagbag.py",
        "param.py",
        "taskinstance.py",
        "taskinstancekey.py",
        "variable.py",
        "xcom.py",
    }

    root = ROOT_DIR / "airflow"
    for path in root.iterdir():
        if path.is_file() and path.name not in _allowed_top_level:
            exclude_patterns.append(_get_rst_filepath_from_path(path))
        if path.is_dir() and path.name not in browsable_packages:
            exclude_patterns.append(f"_api/airflow/{path.name}")

    # Don't include all of utils, just the specific ones we decided to include
    for path in (root / "utils").iterdir():
        if path.name not in browsable_utils:
            exclude_patterns.append(_get_rst_filepath_from_path(path))

    for path in (root / "models").iterdir():
        if path.name not in models_included:
            exclude_patterns.append(_get_rst_filepath_from_path(path))


elif PACKAGE_NAME != "docker-stack":
    exclude_patterns.extend(
        _get_rst_filepath_from_path(f) for f in pathlib.Path(PACKAGE_DIR).rglob("example_dags")
    )

# Add any paths that contain templates here, relative to this directory.
templates_path = ["templates"]

# If true, keep warnings as "system message" paragraphs in the built documents.
keep_warnings = True

# -- Options for HTML output ---------------------------------------------------
# See: https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "sphinx_airflow_theme"

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
if PACKAGE_NAME == "apache-airflow":
    html_title = "Airflow Documentation"
else:
    html_title = f"{PACKAGE_NAME} Documentation"
# A shorter title for the navigation bar.  Default is the same as html_title.
html_short_title = ""

#  given, this must be the name of an image file (path relative to the
#  configuration directory) that is the favicon of the docs. Modern browsers
#  use this as the icon for tabs, windows and bookmarks. It should be a
#  Windows-style icon file (.ico), which is 16x16 or 32x32 pixels large.
html_favicon = "../airflow/www/static/pin_32.png"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
if PACKAGE_NAME in ["apache-airflow", "helm-chart"]:
    html_static_path = [f"{PACKAGE_NAME}/static"]
else:
    html_static_path = []

html_static_path.append("sphinx_design/static/")  # Style overrides for the sphinx-design extension.

# A list of JavaScript filename. The entry must be a filename string or a
# tuple containing the filename string and the attributes dictionary. The
# filename must be relative to the html_static_path, or a full URI with
# scheme like http://example.org/script.js.
if PACKAGE_NAME in ["apache-airflow", "helm-chart"]:
    html_js_files = ["gh-jira-links.js"]
else:
    html_js_files = []
if PACKAGE_NAME == "apache-airflow":
    html_extra_path = [
        f"{ROOT_DIR}/docs/apache-airflow/howto/docker-compose/airflow.sh",
    ]
    html_extra_with_substitutions = [
        f"{ROOT_DIR}/docs/apache-airflow/howto/docker-compose/docker-compose.yaml",
    ]
    # Substitute in links
    manual_substitutions_in_generated_html = [
        "installation/installing-from-pypi.html",
        "installation/installing-from-sources.html",
        "administration-and-deployment/logging-monitoring/advanced-logging-configuration.html",
        "howto/docker-compose/index.html",
    ]
if PACKAGE_NAME.startswith("apache-airflow-providers"):
    manual_substitutions_in_generated_html = ["example-dags.html", "operators.html", "index.html"]
if PACKAGE_NAME == "docker-stack":
    # Substitute in links
    manual_substitutions_in_generated_html = ["build.html", "index.html"]

html_css_files = ["custom.css"]

# -- Theme configuration -------------------------------------------------------
# Custom sidebar templates, maps document names to template names.
html_sidebars = {
    "**": [
        "version-selector.html",
        "searchbox.html",
        "globaltoc.html",
    ]
    if PACKAGE_VERSION != "devel"
    else [
        "searchbox.html",
        "globaltoc.html",
    ]
}

# If false, no index is generated.
html_use_index = True

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
html_show_copyright = False

html_theme_options: dict[str, Any] = {"hide_website_buttons": True, "sidebar_includehidden": True}

html_theme_options["navbar_links"] = [
    {"href": "/community/", "text": "Community"},
    {"href": "/meetups/", "text": "Meetups"},
    {"href": "/docs/", "text": "Documentation"},
    {"href": "/use-cases/", "text": "Use Cases"},
    {"href": "/announcements/", "text": "Announcements"},
    {"href": "/blog/", "text": "Blog"},
    {"href": "/ecosystem/", "text": "Ecosystem"},
]

# A dictionary of values to pass into the template engine's context for all pages.
html_context = {
    # Google Analytics ID.
    # For more information look at:
    # https://github.com/readthedocs/sphinx_rtd_theme/blob/master/sphinx_rtd_theme/layout.html#L222-L232
    "theme_analytics_id": "UA-140539454-1",
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

# == Extensions configuration ==================================================

# -- Options for sphinx_jinja ------------------------------------------
# See: https://github.com/tardyp/sphinx-jinja

airflow_version = parse_version(
    re.search(  # type: ignore[union-attr,arg-type]
        r"__version__ = \"([0-9\.]*)(\.dev[0-9]*)?\"",
        (Path(__file__).parents[1] / "airflow" / "__init__.py").read_text(),
    ).groups(0)[0]
)


def get_configs_and_deprecations(
    package_name: str,
    package_version: Version,
) -> tuple[dict[str, dict[str, tuple[str, str, str]]], dict[str, dict[str, tuple[str, str, str]]]]:
    deprecated_options: dict[str, dict[str, tuple[str, str, str]]] = defaultdict(dict)
    for (section, key), (
        (deprecated_section, deprecated_key, since_version)
    ) in AirflowConfigParser.deprecated_options.items():
        deprecated_options[deprecated_section][deprecated_key] = section, key, since_version

    if package_name == "apache-airflow":
        configs = retrieve_configuration_description(include_providers=False)
    else:
        configs = retrieve_configuration_description(
            include_airflow=False, include_providers=True, selected_provider=package_name
        )

    # We want the default/example we show in the docs to reflect the value _after_
    # the config has been templated, not before
    # e.g. {{dag_id}} in default_config.cfg -> {dag_id} in airflow.cfg, and what we want in docs
    keys_to_format = ["default", "example"]
    for conf_section in configs.values():
        for option_name, option in list(conf_section["options"].items()):
            for key in keys_to_format:
                if option[key] and "{{" in option[key]:
                    option[key] = option[key].replace("{{", "{").replace("}}", "}")
            version_added = option["version_added"]
            if version_added is not None and parse_version(version_added) > package_version:
                del conf_section["options"][option_name]

    # Sort options, config and deprecated options for JINJA variables to display
    for config in configs.values():
        config["options"] = {k: v for k, v in sorted(config["options"].items())}
    configs = {k: v for k, v in sorted(configs.items())}
    for section in deprecated_options:
        deprecated_options[section] = {k: v for k, v in sorted(deprecated_options[section].items())}
    return configs, deprecated_options


# Jinja context
if PACKAGE_NAME == "apache-airflow":
    configs, deprecated_options = get_configs_and_deprecations(PACKAGE_NAME, airflow_version)
    jinja_contexts = {
        "config_ctx": {"configs": configs, "deprecated_options": deprecated_options},
        "quick_start_ctx": {
            "doc_root_url": f"https://airflow.apache.org/docs/apache-airflow/{PACKAGE_VERSION}/"
        },
        "official_download_page": {
            "base_url": f"https://downloads.apache.org/airflow/{PACKAGE_VERSION}",
            "closer_lua_url": f"https://www.apache.org/dyn/closer.lua/airflow/{PACKAGE_VERSION}",
            "airflow_version": PACKAGE_VERSION,
        },
    }
elif PACKAGE_NAME.startswith("apache-airflow-providers-"):
    configs, deprecated_options = get_configs_and_deprecations(PACKAGE_NAME, parse_version(PACKAGE_VERSION))
    jinja_contexts = {
        "config_ctx": {
            "configs": configs,
            "deprecated_options": deprecated_options,
            "package_name": PACKAGE_NAME,
        },
        "official_download_page": {
            "base_url": "https://downloads.apache.org/airflow/providers",
            "closer_lua_url": "https://www.apache.org/dyn/closer.lua/airflow/providers",
            "package_name": PACKAGE_NAME,
            "package_name_underscores": PACKAGE_NAME.replace("-", "_"),
            "package_version": PACKAGE_VERSION,
        },
    }
elif PACKAGE_NAME == "apache-airflow-providers":
    jinja_contexts = {
        "official_download_page": {
            "all_providers": ALL_PROVIDER_YAMLS,
        },
    }
elif PACKAGE_NAME == "helm-chart":

    def _str_representer(dumper, data):
        style = "|" if "\n" in data else None  # show as a block scalar if we have more than 1 line
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style)

    yaml.add_representer(str, _str_representer)

    def _format_default(value: Any) -> str:
        if value == "":
            return '""'
        if value is None:
            return "~"
        return str(value)

    def _format_examples(param_name: str, schema: dict) -> str | None:
        if not schema.get("examples"):
            return None

        # Nicer to have the parameter name shown as well
        out = ""
        for ex in schema["examples"]:
            if schema["type"] == "array":
                ex = [ex]
            out += yaml.dump({param_name: ex})
        return out

    def _get_params(root_schema: dict, prefix: str = "", default_section: str = "") -> list[dict]:
        """
        Given an jsonschema objects properties dict, return a flattened list of all parameters
        from that object and any nested objects
        """
        # TODO: handle arrays? probably missing more cases too
        out = []
        for param_name, schema in root_schema.items():
            prefixed_name = f"{prefix}.{param_name}" if prefix else param_name
            section_name = schema["x-docsSection"] if "x-docsSection" in schema else default_section
            if section_name and schema["description"] and "default" in schema:
                out.append(
                    {
                        "section": section_name,
                        "name": prefixed_name,
                        "description": schema["description"],
                        "default": _format_default(schema["default"]),
                        "examples": _format_examples(param_name, schema),
                    }
                )
            if schema.get("properties"):
                out += _get_params(schema["properties"], prefixed_name, section_name)
        return out

    schema_file = PACKAGE_DIR / "values.schema.json"
    with schema_file.open() as config_file:
        chart_schema = json.load(config_file)

    params = _get_params(chart_schema["properties"])

    # Now, split into sections
    sections: dict[str, list[dict[str, str]]] = {}
    for param in params:
        if param["section"] not in sections:
            sections[param["section"]] = []

        sections[param["section"]].append(param)

    # and order each section
    for section in sections.values():  # type: ignore
        section.sort(key=lambda i: i["name"])  # type: ignore

    # and finally order the sections!
    ordered_sections = []
    for name in chart_schema["x-docsSectionOrder"]:
        if name not in sections:
            raise ValueError(f"Unable to find any parameters for section: {name}")
        ordered_sections.append({"name": name, "params": sections.pop(name)})

    if sections:
        raise ValueError(f"Found section(s) which were not in `section_order`: {list(sections.keys())}")

    jinja_contexts = {
        "params_ctx": {"sections": ordered_sections},
        "official_download_page": {
            "base_url": "https://downloads.apache.org/airflow/helm-chart",
            "closer_lua_url": "https://www.apache.org/dyn/closer.lua/airflow/helm-chart",
            "package_name": PACKAGE_NAME,
            "package_version": PACKAGE_VERSION,
        },
    }


# -- Options for sphinx.ext.autodoc --------------------------------------------
# See: https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html

# This value contains a list of modules to be mocked up. This is useful when some external dependencies
# are not met at build time and break the building process.
autodoc_mock_imports = [
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

# The default options for autodoc directives. They are applied to all autodoc directives automatically.
autodoc_default_options = {"show-inheritance": True, "members": True}

autodoc_typehints = "description"
autodoc_typehints_description_target = "documented"
autodoc_typehints_format = "short"


# -- Options for sphinx.ext.intersphinx ----------------------------------------
# See: https://www.sphinx-doc.org/en/master/usage/extensions/intersphinx.html

# This config value contains names of other projects that should
# be linked to in this documentation.
# Inventories are only downloaded once by docs/exts/docs_build/fetch_inventories.py.
intersphinx_mapping = {
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
if PACKAGE_NAME in ("apache-airflow-providers-google", "apache-airflow"):
    intersphinx_mapping.update(
        {
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
    )

# -- Options for sphinx.ext.viewcode -------------------------------------------
# See: https://www.sphinx-doc.org/es/master/usage/extensions/viewcode.html

# If this is True, viewcode extension will emit viewcode-follow-imported event to resolve the name of
# the module by other extensions. The default is True.
viewcode_follow_imported_members = True

# -- Options for sphinx-autoapi ------------------------------------------------
# See: https://sphinx-autoapi.readthedocs.io/en/latest/config.html

# Paths (relative or absolute) to the source code that you wish to generate
# your API documentation from.
autoapi_dirs: list[os.PathLike] = []

if PACKAGE_NAME != "docker-stack":
    autoapi_dirs.append(PACKAGE_DIR)


# A directory that has user-defined templates to override our default templates.
if PACKAGE_NAME == "apache-airflow":
    autoapi_template_dir = "autoapi_templates"

# A list of patterns to ignore when finding files
autoapi_ignore = [
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
    "*/tests/system/example_empty.py",
    "*/test_aws_auth_manager.py",
    # These sub-folders aren't really providers, but we need __init__.py files else various tools (ruff, mypy)
    # get confused by providers/tests/systems/cncf/kubernetes and think that folder is the top level
    # kubernetes module!
    # TODO (potiuk): remove these once we move all providers to the new structure
    "*/providers/src/airflow/providers/__init__.py",
    "*/providers/tests/__init__.py",
    "*/providers/tests/cncf/__init__.py",
    "*/providers/tests/common/__init__.py",
    "*/providers/tests/apache/__init__.py",
    "*/providers/tests/dbt/__init__.py",
    "*/providers/tests/microsoft/__init__.py",
    "*/providers/tests/system/__init__.py",
    "*/providers/tests/system/apache/__init__.py",
    "*/providers/tests/system/cncf/__init__.py",
    "*/providers/tests/system/common/__init__.py",
    "*/providers/tests/system/dbt/__init__.py",
    "*/providers/tests/system/microsoft/__init__.py",
]

ignore_re = re.compile(r"\[AutoAPI\] .* Ignoring \s (?P<path>/[\w/.]*)", re.VERBOSE)


# Make the "Ignoring /..." log messages slightly less verbose
def filter_ignore(record: logging.LogRecord) -> bool:
    matches = ignore_re.search(record.msg)
    if not matches:
        return True
    if matches["path"].endswith("__init__.py"):
        record.msg = record.msg.replace("__init__.py", "")
        return True
    return False


autoapi_log = logging.getLogger("sphinx.autoapi.mappers.base")
autoapi_log.addFilter(filter_ignore)

if PACKAGE_NAME.startswith("apache-airflow-providers-"):
    autoapi_python_use_implicit_namespaces = True
    from provider_yaml_utils import load_package_data

    autoapi_ignore.extend(
        (
            "*/airflow/__init__.py",
            "*/airflow/providers/__init__.py",
            "*/example_dags/*",
            "*/airflow/providers/cncf/kubernetes/backcompat/*",
            "*/providers/src/apache/airflow/providers/cncf/kubernetes/backcompat/*",
            "*/providers/__init__.py",
        )
    )

    for p in load_package_data(include_suspended=True):
        if p["package-name"] == PACKAGE_NAME:
            continue
        autoapi_ignore.extend((p["package-dir"] + "/*", p["system-tests-dir"] + "/*"))

    autoapi_keep_files = True

    if SYSTEM_TESTS_DIR and os.path.exists(SYSTEM_TESTS_DIR):
        test_dir = SYSTEM_TESTS_DIR.parent
        autoapi_dirs.append(test_dir)

        autoapi_ignore.extend(f"{d}/*" for d in test_dir.glob("*") if d.is_dir() and d.name != "system")
else:
    if SYSTEM_TESTS_DIR and os.path.exists(SYSTEM_TESTS_DIR):
        autoapi_dirs.append(SYSTEM_TESTS_DIR)
# Keep the AutoAPI generated files on the filesystem after the run.
# Useful for debugging.
autoapi_keep_files = True

# Relative path to output the AutoAPI files into. This can also be used to place the generated documentation
# anywhere in your documentation hierarchy.
autoapi_root = "_api"

# Whether to insert the generated documentation into the TOC tree. If this is False, the default AutoAPI
# index page is not generated and you will need to include the generated documentation in a
# TOC tree entry yourself.
autoapi_add_toctree_entry = False

# By default autoapi will include private members -- we don't want that!
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "special-members",
]

suppress_warnings = [
    "autoapi.python_import_resolution",
]

# -- Options for ext.exampleinclude --------------------------------------------
exampleinclude_sourceroot = os.path.abspath("..")

# -- Options for ext.redirects -------------------------------------------------
redirects_file = "redirects.txt"

# -- Options for sphinxcontrib-spelling ----------------------------------------
spelling_word_list_filename = [os.path.join(CONF_DIR, "spelling_wordlist.txt")]
if PACKAGE_NAME == "apache-airflow":
    spelling_exclude_patterns = ["project.rst", "changelog.rst"]
if PACKAGE_NAME == "helm-chart":
    spelling_exclude_patterns = ["changelog.rst"]
spelling_ignore_contributor_names = False
spelling_ignore_importable_modules = True


graphviz_output_format = "svg"

# -- Options for sphinxcontrib.redoc -------------------------------------------
# See: https://sphinxcontrib-redoc.readthedocs.io/en/stable/
if PACKAGE_NAME == "apache-airflow":
    OPENAPI_FILE = os.path.join(
        os.path.dirname(__file__), "..", "airflow", "api_connexion", "openapi", "v1.yaml"
    )
    redoc = [
        {
            "name": "Airflow REST API",
            "page": "stable-rest-api-ref",
            "spec": OPENAPI_FILE,
            "opts": {
                "hide-hostname": True,
                "no-auto-auth": True,
            },
        },
    ]

    # Options for script updater
    redoc_script_url = "https://cdn.jsdelivr.net/npm/redoc@2.0.0-rc.48/bundles/redoc.standalone.js"

elif PACKAGE_NAME == "apache-airflow-providers-fab":
    OPENAPI_FILE = os.path.join(
        os.path.dirname(__file__), "..", "providers", "fab", "auth_manager", "openapi", "v1.yaml"
    )
    redoc = [
        {
            "name": "Fab provider REST API",
            "page": "stable-rest-api-ref",
            "spec": OPENAPI_FILE,
            "opts": {
                "hide-hostname": True,
                "no-auto-auth": True,
            },
        },
    ]

    # Options for script updater
    redoc_script_url = "https://cdn.jsdelivr.net/npm/redoc@2.0.0-rc.48/bundles/redoc.standalone.js"


def skip_util_classes(app, what, name, obj, skip, options):
    if what == "data" and "STATICA_HACK" in name:
        skip = True
    elif ":sphinx-autoapi-skip:" in obj.docstring:
        skip = True
    elif ":meta private:" in obj.docstring:
        skip = True
    return skip


def setup(sphinx):
    if "autoapi.extension" in extensions:
        sphinx.connect("autoapi-skip-member", skip_util_classes)
