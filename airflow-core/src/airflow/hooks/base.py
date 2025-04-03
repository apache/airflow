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
"""Base class for all hooks."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Protocol

from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.connection import Connection  # Avoid circular imports.

log = logging.getLogger(__name__)


class BaseHook(LoggingMixin):
    """
    Abstract base class for hooks.

    Hooks are meant as an interface to
    interact with external systems. MySqlHook, HiveHook, PigHook return
    object that can handle the connection and interaction to specific
    instances of these systems, and expose consistent methods to interact
    with them.

    :param logger_name: Name of the logger used by the Hook to emit logs.
        If set to `None` (default), the logger name will fall back to
        `airflow.task.hooks.{class.__module__}.{class.__name__}` (e.g. DbApiHook will have
        *airflow.task.hooks.airflow.providers.common.sql.hooks.sql.DbApiHook* as logger).
    """

    def __init__(self, logger_name: str | None = None):
        super().__init__()
        self._log_config_logger_name = "airflow.task.hooks"
        self._logger_name = logger_name

    @classmethod
    def get_connection(cls, conn_id: str) -> Connection:
        """
        Get connection, given connection id.

        :param conn_id: connection id
        :return: connection
        """
        from airflow.models.connection import Connection

        conn = Connection.get_connection_from_secrets(conn_id)
        log.info("Connection Retrieved '%s'", conn.conn_id)
        return conn

    @classmethod
    def get_hook(cls, conn_id: str, hook_params: dict | None = None) -> BaseHook:
        """
        Return default hook for this connection id.

        :param conn_id: connection id
        :param hook_params: hook parameters
        :return: default hook for this connection
        """
        connection = cls.get_connection(conn_id)
        return connection.get_hook(hook_params=hook_params)

    def get_conn(self) -> Any:
        """Return connection for the hook."""
        raise NotImplementedError()

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        return {}

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {}


class DiscoverableHook(Protocol):
    """
    Interface that providers *can* implement to be discovered by ProvidersManager.

    It is not used by any of the Hooks, but simply methods and class fields described here are
    implemented by those Hooks. Each method is optional -- only implement the ones you need.

    The conn_name_attr, default_conn_name, conn_type should be implemented by those
    Hooks that want to be automatically mapped from the connection_type -> Hook when get_hook method
    is called with connection_type.

    Additionally hook_name should be set when you want the hook to have a custom name in the UI selection
    Name. If not specified, conn_name will be used.

    The "get_ui_field_behaviour" and "get_connection_form_widgets" are optional - override them if you want
    to customize the Connection Form screen. You can add extra widgets to parse your extra fields via the
    get_connection_form_widgets method as well as hide or relabel the fields or pre-fill
    them with placeholders via get_ui_field_behaviour method.

    Note that the "get_ui_field_behaviour" and "get_connection_form_widgets" need to be set by each class
    in the class hierarchy in order to apply widget customizations.

    For example, even if you want to use the fields from your parent class, you must explicitly
    have a method on *your* class:

    .. code-block:: python

        @classmethod
        def get_ui_field_behaviour(cls):
            return super().get_ui_field_behaviour()

    You also need to add the Hook class name to list 'hook_class_names' in provider.yaml in case you
    build an internal provider or to return it in dictionary returned by provider_info entrypoint in the
    package you prepare.

    You can see some examples in airflow/providers/jdbc/hooks/jdbc.py.

    """

    conn_name_attr: str
    default_conn_name: str
    conn_type: str
    hook_name: str

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """
        Return dictionary of widgets to be added for the hook to handle extra values.

        If you have class hierarchy, usually the widgets needed by your class are already
        added by the base class, so there is no need to implement this method. It might
        actually result in warning in the logs if you try to add widgets that have already
        been added by the base class.

        Note that values of Dict should be of wtforms.Field type. It's not added here
        for the efficiency of imports.

        """
        ...

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """
        Attributes of the UI field.

        Returns dictionary describing customizations to implement in javascript handling the
        connection form. Should be compliant with airflow/customized_form_field_behaviours.schema.json'


        If you change conn_type in a derived class, you should also
        implement this method and return field customizations appropriate to your Hook. This
        is because the child hook will have usually different conn_type and the customizations
        are per connection type.

        .. seealso::
            :class:`~airflow.providers.google.cloud.hooks.compute_ssh.ComputeSSH` as an example

        """
        ...
