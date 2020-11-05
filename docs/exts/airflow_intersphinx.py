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

import os
import time
from typing import Any, Dict

from sphinx.application import Sphinx

from docs.exts.provider_yaml_utils import load_package_data

CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir, os.pardir))
DOCS_DIR = os.path.join(ROOT_DIR, 'docs')
DOCS_PROVIDER_DIR = os.path.join(ROOT_DIR, 'docs')


def _create_init_py(app, config):
    del app
    # del config
    intersphinx_mapping = getattr(config, 'intersphinx_mapping', None) or {}

    providers_mapping = generate_provider_intersphinx_mapping()
    intersphinx_mapping.update(providers_mapping)

    config.intersphinx_mapping = intersphinx_mapping


def generate_provider_intersphinx_mapping():
    airflow_mapping = {}
    for provider in load_package_data():
        package_name = provider['package-name']
        if os.environ.get('AIRFLOW_PACKAGE_NAME') == package_name:
            continue
        if (
            'amazon' not in package_name
            and 'google' not in package_name
            and 'providers-apache' not in package_name
        ):
            continue
        airflow_mapping[package_name] = (
            # base URI
            f'/docs/{package_name}/latest/',
            # Index locations list
            # If passed None, this will try to fetch the index from `[base_url]/objects.inv`
            # If we pass a path containing `://` then we will try to index from the given address.
            # Otherwise, it will try to read the local file
            #
            # In this case, the local index will be read. If unsuccessful, the remote index
            # will be fetched.
            (
                f'{DOCS_PROVIDER_DIR}/_build/docs/{package_name}/latest/objects.inv',
                f'http://airflow-docs-build-dev-mik-laj.s3-website.eu-central-1.amazonaws.com/docs/{package_name}/latest/objects.inv',
            ),
        )
    if os.environ.get('AIRFLOW_PACKAGE_NAME') != 'apache-airflow':
        airflow_mapping['apache-airflow'] = (
            # base URI
            f'/docs/airflow-core/latest/',
            # Index locations list
            # If passed None, this will try to fetch the index from `[base_url]/objects.inv`
            # If we pass a path containing `://` then we will try to index from the given address.
            # Otherwise, it will try to read the local file
            #
            # In this case, the local index will be read. If unsuccessful, the remote index
            # will be fetched.
            (
                f'{DOCS_PROVIDER_DIR}/_build/html/latest/objects.inv',
                f'https://airflow.readthedocs.io/en/latest/objects.inv',
            ),
        )

    return airflow_mapping


def setup(app: Sphinx):
    """
    Sets the plugin up
    """
    app.connect("config-inited", _create_init_py)

    return {"version": "builtin", "parallel_read_safe": True, "parallel_write_safe": True}


if __name__ == "__main__":

    def main():
        import concurrent.futures
        import sys

        from sphinx.ext.intersphinx import fetch_inventory_group

        class MockConfig:
            intersphinx_timeout = None
            intersphinx_cache_limit = 1
            tls_verify = False
            user_agent = None

        class MockApp:
            srcdir = ''
            config = MockConfig()

            def warn(self, msg: str) -> None:
                print(msg, file=sys.stderr)

        def fetch_inventories(intersphinx_mapping) -> Dict[str, Any]:
            now = int(time.time())

            cache = {}
            with concurrent.futures.ThreadPoolExecutor() as pool:
                for name, (uri, invs) in intersphinx_mapping.values():
                    pool.submit(fetch_inventory_group, name, uri, invs, cache, MockApp(), now)

            inv_dict = {}
            for uri, (name, now, invdata) in cache.items():
                del uri
                del now
                inv_dict[name] = invdata
            return inv_dict

        def inspect_main(inv_data, name) -> None:
            """Debug functionality to print out an inventory"""
            try:
                for key in sorted(inv_data or {}):
                    for entry, einfo in sorted(inv_data[key].items()):
                        domain, object_type = key.split(":")
                        if domain == 'py':
                            from sphinx.domains.python import PythonDomain

                            role_name = PythonDomain.object_types[object_type].roles[0]
                        elif domain == 'std':
                            from sphinx.domains.std import StandardDomain

                            role_name = StandardDomain.object_types[object_type].roles[0]
                        else:
                            role_name = object_type
                        print(f":{role_name}:`{name}:{entry}`")
            except ValueError as exc:
                print(exc.args[0] % exc.args[1:])
            except Exception as exc:
                print('Unknown error: %r' % exc)

        provider_mapping = generate_provider_intersphinx_mapping()

        for key, value in provider_mapping.copy().items():
            provider_mapping[key] = (key, value)

        inv_dict = fetch_inventories(provider_mapping)

        for name, inv_data in inv_dict.items():
            inspect_main(inv_data, name)

    import logging

    logging.basicConfig(level=logging.DEBUG)
    main()
