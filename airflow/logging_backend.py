# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow import configuration, AirflowException
from airflow.logging_backends.base_logging_backend import BaseLoggingBackend
from airflow.utils.imports import import_class_by_name

LOGGING_BACKEND = None

# Backend alias to full class name mapping
LOGGING_BACKEND_ALIAS = {
    'elasticsearch': ('airflow.logging_backends.elasticsearch_logging_backend'
                      '.ElasticsearchLoggingBackend'),
}


def cached_logging_backend():

    if not configuration.get('core', 'logging_backend_url'):
        return None

    global LOGGING_BACKEND

    if LOGGING_BACKEND:
        return LOGGING_BACKEND

    url = configuration.get('core', 'logging_backend_url')
    LOGGING_BACKEND = _get_logging_backend(url)

    return LOGGING_BACKEND


def _get_logging_backend(url):
    """
    Get logging backend instance.
    :param url: Url of the logging backend in form schema_alias://host
    :raises ValueError: if url or schema invalid
    :raises TypeError: if backend is not an instance of BaseLoggingBackend
    """
    parsed = url.split('://', 1)
    if len(parsed) != 2:
        raise ValueError("Logging backend url {} invalid. Please use correct "
                         "format: schema_alias://host.".format(url))

    schema, host = parsed
    cls_name = LOGGING_BACKEND_ALIAS.get(schema)
    if not cls_name:
        raise ValueError("Logging backend alias {} not found".format(schema))

    backend_cls = import_class_by_name(cls_name)
    backend = backend_cls(host)
    if not isinstance(backend, BaseLoggingBackend):
        raise AirflowException("Backend is not an instance of BaseLoggingBackend")

    return backend
