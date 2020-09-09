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

import importlib
import pkgutil


def get_provider_info(conn_type_to_hook, connection_types):
    """
    Retrieves provider information and adds information to hooks/connection
    :param conn_type_to_hook: dictionary of mapping for connections -> hooks
    :param connection_types: array of connection types
    :return: dictionary metadata about all providers installed
    """
    def ignore(_):
        pass

    def update_or_replace_connection_type(core_connection_types, provider_connection_types):
        """
        Updates or replaces all core connection types with those that come from a provider.
        If same connection type is found in core types it is replaced with the provider one,
        otherwise the provider connection type is appended to the list.

        :param core_connection_types: all core connection tepes
        :type core_connection_types List[Tuple]
        :param provider_connection_types: provider connection types
        :type provider_connection_types List[Tuple]
        :return: None
        """
        for provider_connection_type in provider_connection_types:
            for index, core_connection_type in enumerate(connection_types):
                if core_connection_type[0] == provider_connection_type[0]:
                    connection_types[index] = provider_connection_type
                    break
            core_connection_types.append(provider_connection_type)
    try:
        from airflow import providers
    except ImportError:
        print("No providers are available!")
        return

    providers_path = providers.__path__
    providers_name = providers.__name__
    provider_dict = {}

    for (_, name, ispkg) in pkgutil.walk_packages(path=providers_path,
                                                  prefix=providers_name + ".",
                                                  onerror=ignore):
        try:
            if ispkg:
                provider_info_module = importlib.import_module(".provider_info", package=name)
                conn_type_to_hook.update(provider_info_module.CONN_TYPE_TO_HOOK)
                update_or_replace_connection_type(connection_types, provider_info_module.connection_types)
                provider_metadata = {
                    'name': provider_info_module.PROVIDER_NAME,
                    'version': provider_info_module.PROVIDER_VERSION,
                    'url': provider_info_module.PROVIDER_URL,
                    'docs': provider_info_module.PROVIDER_DOCS,
                }
                provider_dict[provider_info_module.PROVIDER_NAME] = provider_metadata
                print(provider_metadata)
        except ModuleNotFoundError:
            pass
        except Exception as e:  # noqa pylint: disable=broad-except
            print("Provider {} could not be loaded because of {}".format(name, e))
    return providers
