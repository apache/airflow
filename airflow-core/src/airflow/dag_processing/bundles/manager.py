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

import warnings
from typing import TYPE_CHECKING

from itsdangerous import URLSafeSerializer
from sqlalchemy import delete

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models.dagbundle import DagBundleModel
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.orm import Session

    from airflow.dag_processing.bundles.base import BaseDagBundle

_example_dag_bundle_name = "example_dags"


def _bundle_item_exc(msg):
    return AirflowConfigException(
        "Invalid config for section `dag_processor` key `dag_bundle_config_list`. " + msg
    )


def _validate_bundle_config(config_list):
    all_names = []
    expected_keys = {"name", "classpath", "kwargs"}
    for item in config_list:
        if not isinstance(item, dict):
            raise _bundle_item_exc(f"Expected dict but got {item.__class__}")
        actual_keys = set(item.keys())
        if not actual_keys == expected_keys:
            raise _bundle_item_exc(f"Expected keys {expected_keys} but found {actual_keys}")
        bundle_name = item["name"]
        if not bundle_name:
            raise _bundle_item_exc(f"Item {item} missing required `name` attr.")
        if bundle_name == _example_dag_bundle_name:
            raise AirflowConfigException(
                f"Bundle name '{_example_dag_bundle_name}' is a reserved name. Please choose another name for your bundle."
                " Example DAGs can be enabled with the '[core] load_examples' config."
            )

        all_names.append(bundle_name)
    if len(all_names) != len(set(all_names)):
        raise _bundle_item_exc(f"One or more bundle names appeared multiple times: {all_names}")


def _add_example_dag_bundle(config_list):
    from airflow import example_dags

    example_dag_folder = next(iter(example_dags.__path__))
    config_list.append(
        {
            "name": _example_dag_bundle_name,
            "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
            "kwargs": {
                "path": example_dag_folder,
            },
        }
    )


def _is_safe_bundle_url(url: str) -> bool:
    """
    Check if a bundle URL is safe to use.

    This function validates that the URL:
    - Uses HTTP or HTTPS schemes (no JavaScript, data, or other schemes)
    - Is properly formatted
    - Doesn't contain malicious content
    """
    import logging
    from urllib.parse import urlparse

    logger = logging.getLogger(__name__)

    if not url:
        return False

    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            logger.error(
                "Bundle URL uses unsafe scheme '%s'. Only 'http' and 'https' are allowed", parsed.scheme
            )
            return False

        if not parsed.netloc:
            logger.error("Bundle URL '%s' has no network location", url)
            return False

        if any(ord(c) < 32 for c in url):
            logger.error("Bundle URL '%s' contains control characters (ASCII < 32)", url)
            return False

        return True
    except Exception as e:
        logger.error("Failed to parse bundle URL '%s': %s", url, str(e))
        return False


def _sign_bundle_url(url: str, bundle_name: str) -> str:
    """
    Sign a bundle URL for integrity verification.

    :param url: The URL to sign
    :param bundle_name: The name of the bundle (used in the payload)
    :return: The signed URL token
    """
    serializer = URLSafeSerializer(conf.get_mandatory_value("core", "fernet_key"))
    payload = {
        "url": url,
        "bundle_name": bundle_name,
    }
    return serializer.dumps(payload)


class DagBundlesManager(LoggingMixin):
    """Manager for DAG bundles."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._bundle_config = {}
        self.parse_config()

    def parse_config(self) -> None:
        """
        Get all DAG bundle configurations and store in instance variable.

        If a bundle class for a given name has already been imported, it will not be imported again.

        todo (AIP-66): proper validation of the bundle configuration so we have better error messages

        :meta private:
        """
        if self._bundle_config:
            return

        config_list = conf.getjson("dag_processor", "dag_bundle_config_list")
        if not config_list:
            return
        if not isinstance(config_list, list):
            raise AirflowConfigException(
                "Section `dag_processor` key `dag_bundle_config_list` "
                f"must be list but got {config_list.__class__}"
            )
        _validate_bundle_config(config_list)
        if conf.getboolean("core", "LOAD_EXAMPLES"):
            _add_example_dag_bundle(config_list)

        for cfg in config_list:
            name = cfg["name"]
            class_ = import_string(cfg["classpath"])
            kwargs = cfg["kwargs"]
            self._bundle_config[name] = (class_, kwargs)
        self.log.info("DAG bundles loaded: %s", ", ".join(self._bundle_config.keys()))

    @provide_session
    def sync_bundles_to_db(self, *, session: Session = NEW_SESSION) -> None:
        self.log.debug("Syncing DAG bundles to the database")

        def _extract_and_sign_template(bundle_name: str) -> tuple[str | None, dict]:
            bundle_instance = self.get_bundle(name)
            new_template_ = bundle_instance.view_url_template()
            new_params_ = self._extract_template_params(bundle_instance)
            if new_template_:
                if not _is_safe_bundle_url(new_template_):
                    self.log.warning(
                        "Bundle %s has unsafe URL template '%s', skipping URL update",
                        bundle_name,
                        new_template_,
                    )
                    new_template_ = None
                else:
                    # Sign the URL for integrity verification
                    new_template_ = _sign_bundle_url(new_template_, bundle_name)
                    self.log.debug("Signed URL template for bundle %s", bundle_name)
            return new_template_, new_params_

        stored = {b.name: b for b in session.query(DagBundleModel).all()}

        for name in self._bundle_config.keys():
            if bundle := stored.pop(name, None):
                bundle.active = True
                new_template, new_params = _extract_and_sign_template(name)
                if new_template != bundle.signed_url_template:
                    bundle.signed_url_template = new_template
                    self.log.debug("Updated URL template for bundle %s", name)
                if new_params != bundle.template_params:
                    bundle.template_params = new_params
                    self.log.debug("Updated template parameters for bundle %s", name)
            else:
                new_template, new_params = _extract_and_sign_template(name)
                new_bundle = DagBundleModel(name=name)
                new_bundle.signed_url_template = new_template
                new_bundle.template_params = new_params

                session.add(new_bundle)
                self.log.info("Added new DAG bundle %s to the database", name)

        for name, bundle in stored.items():
            bundle.active = False
            self.log.warning("DAG bundle %s is no longer found in config and has been disabled", name)
            from airflow.models.errors import ParseImportError

            session.execute(delete(ParseImportError).where(ParseImportError.bundle_name == name))
            self.log.info("Deleted import errors for bundle %s which is no longer configured", name)

    @staticmethod
    def _extract_template_params(bundle_instance: BaseDagBundle) -> dict:
        """
        Extract template parameters from a bundle instance's view_url_template method.

        :param bundle_instance: The bundle instance to extract parameters from
        :return: Dictionary of template parameters
        """
        import re

        params: dict[str, str] = {}
        template = bundle_instance.view_url_template()

        if not template:
            return params

        # Extract template placeholders using regex
        # This matches {placeholder} patterns in the template
        PLACEHOLDER_PATTERN = re.compile(r"\{([^}]+)\}")
        placeholders = PLACEHOLDER_PATTERN.findall(template)

        # Extract values for each placeholder found in the template
        for placeholder in placeholders:
            field_value = getattr(bundle_instance, placeholder, None)
            if field_value:
                params[placeholder] = field_value

        return params

    def get_bundle(self, name: str, version: str | None = None) -> BaseDagBundle:
        """
        Get a DAG bundle by name.

        :param name: The name of the DAG bundle.
        :param version: The version of the DAG bundle you need (optional). If not provided, ``tracking_ref`` will be used instead.

        :return: The DAG bundle.
        """
        cfg_tuple = self._bundle_config.get(name)
        if not cfg_tuple:
            raise ValueError(f"Requested bundle '{name}' is not configured.")
        class_, kwargs = cfg_tuple
        return class_(name=name, version=version, **kwargs)

    def get_all_dag_bundles(self) -> Iterable[BaseDagBundle]:
        """
        Get all DAG bundles.

        :return: list of DAG bundles.
        """
        for name, (class_, kwargs) in self._bundle_config.items():
            yield class_(name=name, version=None, **kwargs)

    def view_url(self, name: str, version: str | None = None) -> str | None:
        warnings.warn(
            "The 'view_url' method is deprecated and will be removed when providers "
            "have Airflow 3.1 as the minimum supported version. "
            "Use DagBundleModel.render_url() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        bundle = self.get_bundle(name, version)
        return bundle.view_url(version=version)
