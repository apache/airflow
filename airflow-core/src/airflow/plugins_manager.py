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
"""Manages all plugins."""

from __future__ import annotations

import difflib
import inspect
import json
import logging
import types
from collections.abc import Iterable
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Union, get_args, get_origin

from airflow import settings
from airflow._shared.module_loading import import_string, qualname
from airflow._shared.plugins_manager import (
    AirflowPlugin as AirflowPlugin,
    AirflowPluginSource as AirflowPluginSource,
    AppliesToDict as AppliesToDict,
    BaseDestinationLiteral as BaseDestinationLiteral,
    ExternalViewDict as ExternalViewDict,
    FastAPIAppDict as FastAPIAppDict,
    FastAPIRootMiddlewareDict as FastAPIRootMiddlewareDict,
    PluginsDirectorySource as PluginsDirectorySource,
    ReactAppDict as ReactAppDict,
    _load_entrypoint_plugins,
    _load_plugins_from_plugin_directory,
    is_valid_plugin,
)
from airflow.configuration import conf
from airflow.serialization.helpers import (
    is_core_partition_mapper_import_path,
    is_core_timetable_import_path,
)

if TYPE_CHECKING:
    from airflow.listeners.listener import ListenerManager
    from airflow.models.deadline import DeadlineReferenceType
    from airflow.partition_mappers.base import PartitionMapper
    from airflow.partition_mappers.window import Window
    from airflow.task.priority_strategy import PriorityWeightStrategy
    from airflow.timetables.base import Timetable

log = logging.getLogger(__name__)


def _load_providers_plugins() -> tuple[list[AirflowPlugin], dict[str, str]]:
    from airflow.providers_manager import ProvidersManager

    log.debug("Loading plugins from providers")
    providers_manager = ProvidersManager()
    providers_manager.initialize_providers_plugins()

    plugins: list[AirflowPlugin] = []
    import_errors: dict[str, str] = {}
    for plugin in providers_manager.plugins:
        log.debug("Importing plugin %s from class %s", plugin.name, plugin.plugin_class)

        try:
            plugin_instance = import_string(plugin.plugin_class)
            if is_valid_plugin(plugin_instance):
                plugins.append(plugin_instance)
            else:
                log.warning("Plugin %s is not a valid plugin", plugin.name)
        except ImportError:
            log.exception("Failed to load plugin %s from class name %s", plugin.name, plugin.plugin_class)
    return plugins, import_errors


def ensure_plugins_loaded() -> None:
    """
    Load plugins from plugins directory and entrypoints.

    Plugins are only loaded if they have not been previously loaded.
    """
    _get_plugins()


@cache
def _get_plugins() -> tuple[list[AirflowPlugin], dict[str, str]]:
    """
    Load plugins from plugins directory and entrypoints.

    Plugins are only loaded if they have not been previously loaded.
    """
    from airflow._shared.observability.metrics import stats
    from airflow.providers_manager import provider_incompatibility_reason

    if not settings.PLUGINS_FOLDER:
        raise ValueError("Plugins folder is not set")

    log.debug("Loading plugins")

    plugins: list[AirflowPlugin] = []
    import_errors: dict[str, str] = {}
    loaded_plugins: set[str | None] = set()

    def __register_plugins(plugin_instances: list[AirflowPlugin], errors: dict[str, str]) -> None:
        for plugin_instance in plugin_instances:
            if plugin_instance.name in loaded_plugins:
                message = f"Plugin {plugin_instance.name!r} already registered, skipping"
                log.warning(message)
                name = str(plugin_instance.source) if plugin_instance.source else plugin_instance.name or ""
                import_errors[name] = message
                continue

            loaded_plugins.add(plugin_instance.name)
            try:
                plugin_instance.on_load()
                plugins.append(plugin_instance)
            except Exception as e:
                log.exception("Failed to load plugin %s", plugin_instance.name)
                name = str(plugin_instance.source) if plugin_instance.source else plugin_instance.name or ""
                import_errors[name] = str(e)
        import_errors.update(errors)

    with stats.timer() as timer:
        load_examples = conf.getboolean("core", "LOAD_EXAMPLES")
        ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")
        __register_plugins(
            *_load_plugins_from_plugin_directory(
                plugins_folder=settings.PLUGINS_FOLDER,
                load_examples=load_examples,
                example_plugins_module="airflow.example_dags.plugins" if load_examples else None,
                ignore_file_syntax=ignore_file_syntax,
            )
        )
        __register_plugins(*_load_entrypoint_plugins(provider_incompatibility_reason))

        if not settings.LAZY_LOAD_PROVIDERS:
            __register_plugins(*_load_providers_plugins())

    if import_errors:
        log.warning(
            "Failed to load %d plugin file(s): %s",
            len(import_errors),
            sorted(import_errors.keys()),
        )
    elif not plugins:
        log.debug("No plugins loaded (plugins folder is empty or contains no valid plugins)")
    else:
        log.debug("Loading %d plugin(s) took %.2f ms", len(plugins), timer.duration)
    return plugins, import_errors


# The records each destination can resolve, and therefore the path roots it can evaluate. A
# destination missing from this mapping can evaluate nothing. Kept in sync with the table in
# docs/administration-and-deployment/plugins.rst.
_APPLIES_TO_ROOTS: dict[str, frozenset[str]] = {
    "dag": frozenset({"dag"}),
    "dag_overview": frozenset({"dag"}),
    "dag_run": frozenset({"dag", "dag_run"}),
    "task": frozenset({"dag", "task"}),
    "task_overview": frozenset({"dag", "task"}),
    "task_instance": frozenset({"dag", "dag_run", "task", "task_instance"}),
    "nav": frozenset(),
    "base": frozenset(),
    "dashboard": frozenset(),
    "asset": frozenset(),
}

# Which record an unqualified path is rooted at -- the entity the destination is about.
_APPLIES_TO_ENTITY_ROOT: dict[str, str] = {
    "dag": "dag",
    "dag_overview": "dag",
    "dag_run": "dag_run",
    "task": "task",
    "task_overview": "task",
    "task_instance": "task_instance",
}

_APPLIES_TO_ROOT_NAMES = frozenset({"dag", "dag_run", "task", "task_instance"})


def _applies_to_path_root(path: str, destination: str) -> str | None:
    """
    Return the record a path is rooted at, or ``None`` if the destination has no entity.

    A path may name a related record as its first segment; otherwise it is rooted at the
    entity the destination is about.
    """
    head, _, rest = path.partition(".")
    if head in _APPLIES_TO_ROOT_NAMES and rest:
        return head
    return _APPLIES_TO_ENTITY_ROOT.get(destination)


# Sentinel for an annotation that does not describe what it contains, so a path cannot be
# checked past it.
_OPAQUE = object()


@cache
def _applies_to_root_models() -> dict[str, Any]:
    """
    Return the response model backing each path root, for validating paths at plugin load.

    Imported lazily because ``datamodels.plugins`` imports this module; a module-level import
    would be circular. Only ``_get_ui_plugins`` reaches this, so components that load plugins
    without serving the UI never pay for it.

    These are the models the UI actually fetches for the ``applies_to`` context -- keep them in
    step with ``AppliesToContext`` in ``src/utils/pluginAppliesTo.ts``.
    """
    from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunResponse
    from airflow.api_fastapi.core_api.datamodels.dags import DAGResponse
    from airflow.api_fastapi.core_api.datamodels.task_instances import TaskInstanceResponse
    from airflow.api_fastapi.core_api.datamodels.tasks import TaskResponse

    return {
        "dag": DAGResponse,
        "dag_run": DAGRunResponse,
        "task": TaskResponse,
        "task_instance": TaskInstanceResponse,
    }


def _unwrap_applies_to_annotation(annotation: Any) -> Any:
    """
    Reduce a field annotation to the type a further path segment reads through.

    ``Annotated`` and ``X | None`` wrappers are stripped, and a list is stepped into, because
    traversing one fans out across its elements. Returns ``_OPAQUE`` for a union of several
    real types, whose fields depend on which member a record actually holds.
    """
    while True:
        origin = get_origin(annotation)
        if origin is Annotated:
            annotation = get_args(annotation)[0]
        elif origin in (Union, types.UnionType):
            members = [arg for arg in get_args(annotation) if arg is not type(None)]
            if len(members) != 1:
                return _OPAQUE
            annotation = members[0]
        elif origin in (list, set, frozenset, tuple):
            args = get_args(annotation)
            if not args:
                return _OPAQUE
            annotation = args[0]
        else:
            return annotation


def _applies_to_serialized_fields(model: Any) -> dict[str, Any] | None:
    """
    Map each field name *as the API serializes it* to the annotation behind it.

    ``applies_to`` paths are resolved in the browser against the JSON a response model produces,
    so validation has to use the serialized names rather than the Python attribute names:
    ``TaskInstanceResponse.run_id`` reaches the browser as ``dag_run_id``, and computed fields
    such as ``DAGResponse.is_backfillable`` have no entry in ``model_fields`` at all.

    Returns ``None`` for anything that is not a model.
    """
    fields = getattr(model, "model_fields", None)
    if fields is None:
        return None

    serialized = {
        field.serialization_alias or field.alias or name: field.annotation for name, field in fields.items()
    }
    for name, computed in getattr(model, "model_computed_fields", {}).items():
        serialized[computed.alias or name] = computed.return_type
    return serialized


def _describe_applies_to_path_error(path: str, root: str) -> str | None:
    """
    Return why ``path`` names no field on its root record, or ``None`` if it is not knowably wrong.

    The walk stops -- accepting whatever follows -- at a field the models do not describe the
    contents of, such as the bare ``dict`` behind ``class_ref`` or a Dag Run's ``conf``. So this
    catches a misspelling of a modelled field, not every bad path.
    """
    segments = path.split(".")
    # A qualified path's first segment names the record, which `root` has already resolved.
    if segments[0] == root and len(segments) > 1:
        segments = segments[1:]

    current: Any = _applies_to_root_models()[root]
    for segment in segments:
        if current is _OPAQUE or current is Any:
            return None
        base = get_origin(current) or current
        if isinstance(base, type) and issubclass(base, dict):
            return None

        fields = _applies_to_serialized_fields(current)
        if fields is None:
            name = getattr(current, "__name__", repr(current))
            return f"'{path}' reads '{segment}' from {name}, which has no fields"
        if segment not in fields:
            model = getattr(current, "__name__", repr(current))
            hint = ""
            if close := difflib.get_close_matches(segment, list(fields), n=1):
                hint = f" (did you mean '{close[0]}'?)"
            return f"'{path}' names no field '{segment}' on {model}{hint}"

        current = _unwrap_applies_to_annotation(fields[segment])
    return None


def _describe_applies_to_error(applies_to: Any) -> str | None:
    """Return a description of why ``applies_to`` is malformed, or ``None`` if it is valid."""
    if not isinstance(applies_to, dict):
        return f"expected a dictionary, got {type(applies_to).__name__}"
    if non_string_keys := [key for key in applies_to if not isinstance(key, str)]:
        return f"field paths must be strings, got {sorted(non_string_keys, key=repr)!r}"
    if empty_keys := [key for key in applies_to if not key.strip()]:
        return f"field paths must not be empty, got {empty_keys!r}"
    for path, values in applies_to.items():
        if values is None:
            continue
        if not isinstance(values, (list, tuple)) or not all(isinstance(value, str) for value in values):
            return f"'{path}' must be a list of strings, got {values!r}"
    return None


def _validate_applies_to(plugin_name: str | None, view: ExternalViewDict | ReactAppDict, kind: str) -> None:
    """
    Warn about scoping a UI plugin cannot honour, and strip it if it is malformed.

    A malformed block is removed so the view still loads unscoped, matching the default for
    a view that omits ``applies_to`` entirely. Criteria the destination cannot evaluate are
    only warned about — they are skipped at match time by design, so that one block can be
    shared across a plugin's Dag- and task-level destinations.
    """
    if "applies_to" not in view:
        return

    applies_to = view["applies_to"]
    if applies_to is None:
        return

    if error := _describe_applies_to_error(applies_to):
        log.warning(
            "Plugin '%s' has %s '%s' with an invalid 'applies_to': %s. The scoping will be ignored.",
            plugin_name,
            kind,
            view.get("name"),
            error,
        )
        del view["applies_to"]
        return

    # A null value means "no values configured", which the matcher already ignores the same way
    # it ignores an empty list. Leaving it in place would fail `PluginAppliesToResponse`
    # serialization and drop the whole plugin -- including its other, valid views -- from the
    # plugins API. Dropping the path keeps the rest of the block working.
    for path in [path for path, values in applies_to.items() if values is None]:
        del applies_to[path]

    destination = view.get("destination", "nav")
    if destination not in _APPLIES_TO_ROOTS:
        # An unrecognised destination already fails serialization; warning here too would
        # only add noise pointing at the wrong problem.
        return

    available = _APPLIES_TO_ROOTS[destination]
    unevaluable = sorted(
        path
        for path, values in applies_to.items()
        if values and _applies_to_path_root(path, destination) not in available
    )
    if unevaluable:
        log.warning(
            "Plugin '%s' has %s '%s' with destination '%s', which cannot evaluate %s. "
            "Those paths will be ignored.",
            plugin_name,
            kind,
            view.get("name"),
            destination,
            unevaluable,
        )

    # A path naming a field no record has is indistinguishable at match time from one the page
    # simply cannot judge, so the UI skips it -- which *widens* the scope instead of narrowing
    # it. Catching the misspelling here is the only place it can be told apart.
    for path, values in applies_to.items():
        root = _applies_to_path_root(path, destination)
        if not values or root is None or root not in available:
            continue
        if error := _describe_applies_to_path_error(path, root):
            log.warning(
                "Plugin '%s' has %s '%s' with an 'applies_to' path that matches no field: %s. "
                "That path will be ignored, so the %s will appear in more places than intended.",
                plugin_name,
                kind,
                view.get("name"),
                error,
                kind.split()[-1],
            )


@cache
def _get_ui_plugins() -> tuple[list[ExternalViewDict], list[ReactAppDict]]:
    """Collect extension points for the UI."""
    log.debug("Initialize UI plugin")

    seen_url_routes: dict[str, str | None] = {}

    external_views: list[ExternalViewDict] = []
    react_apps: list[ReactAppDict] = []
    for plugin in _get_plugins()[0]:
        external_views_to_remove: list[ExternalViewDict] = []
        react_apps_to_remove: list[ReactAppDict] = []
        for external_view in plugin.external_views:
            if not isinstance(external_view, dict):
                log.warning(
                    "Plugin '%s' has an external view that is not a dictionary. The view will not be loaded.",
                    plugin.name,
                )
                external_views_to_remove.append(external_view)
                continue
            _validate_applies_to(plugin.name, external_view, "an external view")
            url_route = external_view.get("url_route")
            if url_route is None:
                continue
            if url_route in seen_url_routes:
                log.warning(
                    "Plugin '%s' has an external view with an URL route '%s' "
                    "that conflicts with another plugin '%s'. The view will not be loaded.",
                    plugin.name,
                    url_route,
                    seen_url_routes[url_route],
                )
                external_views_to_remove.append(external_view)
                continue
            external_views.append(external_view)
            seen_url_routes[url_route] = plugin.name

        for react_app in plugin.react_apps:
            if not isinstance(react_app, dict):
                log.warning(
                    "Plugin '%s' has a React App that is not a dictionary. The React App will not be loaded.",
                    plugin.name,
                )
                react_apps_to_remove.append(react_app)
                continue
            _validate_applies_to(plugin.name, react_app, "a React App")
            url_route = react_app.get("url_route")
            if url_route is None:
                continue
            if url_route in seen_url_routes:
                log.warning(
                    "Plugin '%s' has a React App with an URL route '%s' "
                    "that conflicts with another plugin '%s'. The React App will not be loaded.",
                    plugin.name,
                    url_route,
                    seen_url_routes[url_route],
                )
                react_apps_to_remove.append(react_app)
                continue
            react_apps.append(react_app)
            seen_url_routes[url_route] = plugin.name

        for external_view in external_views_to_remove:
            plugin.external_views.remove(external_view)
        for react_app in react_apps_to_remove:
            plugin.react_apps.remove(react_app)
    return external_views, react_apps


@cache
def get_flask_plugins() -> tuple[list[Any], list[Any], list[Any]]:
    """Collect and get flask extension points for WEB UI (legacy)."""
    log.debug("Initialize legacy Web UI plugin")

    flask_appbuilder_views: list[Any] = []
    flask_appbuilder_menu_links: list[Any] = []
    flask_blueprints: list[Any] = []
    for plugin in _get_plugins()[0]:
        flask_appbuilder_views.extend(plugin.appbuilder_views)
        flask_appbuilder_menu_links.extend(plugin.appbuilder_menu_items)
        flask_blueprints.extend([{"name": plugin.name, "blueprint": bp} for bp in plugin.flask_blueprints])

        if (plugin.admin_views and not plugin.appbuilder_views) or (
            plugin.menu_links and not plugin.appbuilder_menu_items
        ):
            log.warning(
                "Plugin '%s' may not be compatible with the current Airflow version. "
                "Please contact the author of the plugin.",
                plugin.name,
            )
    return flask_blueprints, flask_appbuilder_views, flask_appbuilder_menu_links


@cache
def get_fastapi_plugins() -> tuple[list[Any], list[Any]]:
    """
    Collect extension points for the API.

    Each returned dict is a shallow copy of the plugin's own dict with the owning
    plugin's ``team_name`` added, so the API server can authorize a team-scoped
    plugin's app without re-deriving which plugin it came from. The plugin's dicts are
    left untouched, so this does not alter what ``get_plugin_info`` reports.
    """
    log.debug("Initialize FastAPI plugins")

    # Validate here (the API-server, DB-available path) so callers cannot mount
    # plugins without the team check running.
    validate_plugin_teams()

    fastapi_apps: list[Any] = []
    fastapi_root_middlewares: list[Any] = []
    for plugin in _get_plugins()[0]:
        fastapi_apps.extend({**app, "team_name": plugin.team_name} for app in plugin.fastapi_apps)
        fastapi_root_middlewares.extend(
            {**middleware, "team_name": plugin.team_name} for middleware in plugin.fastapi_root_middlewares
        )
    return fastapi_apps, fastapi_root_middlewares


PluginTranslations = dict[str, dict[str, dict[str, Any]]]


def merge_translations(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge ``override`` onto ``base`` (recursing into nested dicts) and return a new dict."""
    merged = dict(base)
    for key, value in override.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = merge_translations(existing, value)
        else:
            merged[key] = value
    return merged


def _load_translation_source(source: Any) -> PluginTranslations:
    """Load one ``ui_translations`` entry (inline mapping or ``<language>/<namespace>.json`` tree)."""
    result: PluginTranslations = {}

    if isinstance(source, dict):
        for language, namespaces in source.items():
            for namespace, keys in namespaces.items():
                if not isinstance(keys, dict):
                    raise ValueError(f"translations for {language!r}/{namespace!r} must be a mapping")
                result.setdefault(language, {})[namespace] = keys
        return result

    directory = Path(source)
    if not directory.is_dir():
        raise ValueError(f"translation source {source!r} is neither a directory nor an inline mapping")
    for language_dir in sorted(directory.iterdir()):
        if not language_dir.is_dir():
            continue
        for namespace_file in sorted(language_dir.glob("*.json")):
            try:
                content = json.loads(namespace_file.read_text("utf-8"))
            except (OSError, ValueError):
                log.warning("Skipping unreadable UI translation file %s", namespace_file)
                continue
            result.setdefault(language_dir.name, {})[namespace_file.stem] = content
    return result


@cache
def get_ui_translations() -> PluginTranslations:
    """
    Collect and deep-merge the ``language -> namespace -> keys`` UI translations from all plugins.

    Never raises: a broken source (unreadable file, bad ``ui_translations`` value) is skipped with a
    warning so it cannot stop the API server starting or keep other plugins from loading.
    """
    plugin_translations: PluginTranslations = {}
    for plugin in _get_plugins()[0]:
        try:
            for source in plugin.ui_translations:
                contributed = _load_translation_source(source)
                for language, namespaces in contributed.items():
                    language_translations = plugin_translations.setdefault(language, {})
                    for namespace, keys in namespaces.items():
                        language_translations[namespace] = merge_translations(
                            language_translations.get(namespace, {}), keys
                        )
        except Exception:
            log.exception("Skipping invalid UI translations from plugin %s", plugin.name)
            continue
    return plugin_translations


def warn_about_unknown_translation_keys(plugin_translations: PluginTranslations, reference_dir: Path) -> None:
    """Warn about plugin keys absent from the English reference (likely stale). Only logs; never raises."""

    def _warn(keys: dict[str, Any], reference: Any, language: str, namespace: str, prefix: str = "") -> None:
        for key, value in keys.items():
            in_reference = isinstance(reference, dict) and key in reference
            if isinstance(value, dict):
                _warn(
                    value,
                    reference.get(key) if in_reference else None,
                    language,
                    namespace,
                    f"{prefix}{key}.",
                )
            elif not in_reference:
                log.warning(
                    "Plugin UI translation for %s/%s sets key %r, which is not in the English "
                    "reference and may be stale.",
                    language,
                    namespace,
                    f"{prefix}{key}",
                )

    for language, namespaces in plugin_translations.items():
        for namespace, keys in namespaces.items():
            try:
                reference = json.loads((reference_dir / f"{namespace}.json").read_text("utf-8"))
            except (OSError, ValueError):
                reference = {}
            _warn(keys, reference, language, namespace)


@cache
def _get_extra_operators_links_plugins() -> tuple[list[Any], list[Any]]:
    """Create and get modules for loaded extension from extra operators links plugins."""
    log.debug("Initialize extra operators links plugins")

    global_operator_extra_links: list[Any] = []
    operator_extra_links: list[Any] = []
    for plugin in _get_plugins()[0]:
        global_operator_extra_links.extend(plugin.global_operator_extra_links)
        operator_extra_links.extend(list(plugin.operator_extra_links))
    return global_operator_extra_links, operator_extra_links


def get_global_operator_extra_links() -> list[Any]:
    """Get global operator extra links registered by plugins."""
    return _get_extra_operators_links_plugins()[0]


def get_operator_extra_links() -> list[Any]:
    """Get operator extra links registered by plugins."""
    return _get_extra_operators_links_plugins()[1]


@cache
def _get_extra_link_class_teams() -> dict[type, frozenset[str | None]]:
    """
    Map every plugin-registered extra link class to the teams that registered it.

    Keyed by class because neither of the alternatives works: ``BaseOperatorLink`` sets
    ``__hash__ = None`` and compares equal across instances, and two distinct link
    classes may share a ``name`` (plugin links deliberately override operator links of
    the same name), so a name key would conflate them.

    A class registered by several plugins maps to all of their teams, which
    :func:`is_extra_link_visible_to_team` then resolves least restrictively.
    """
    teams: dict[type, set[str | None]] = {}
    for plugin in _get_plugins()[0]:
        for link in (*plugin.global_operator_extra_links, *plugin.operator_extra_links):
            teams.setdefault(type(link), set()).add(plugin.team_name)
    return {link_class: frozenset(team_names) for link_class, team_names in teams.items()}


def is_extra_link_visible_to_team(link: Any, team_name: str | None) -> bool:
    """
    Whether ``link`` should be shown on a task instance belonging to ``team_name``.

    A team-scoped plugin's links are shown only on that team's task instances, so they
    appear neither on another team's Dags nor on teamless (global) ones. Links from
    global plugins, and links the operator defines itself, stay visible everywhere.

    :param link: The operator link object, whose class identifies the registering plugin.
    :param team_name: Team owning the Dag the link would be rendered for, or ``None``
        when the Dag is not team-owned.
    """
    link_teams = _get_extra_link_class_teams().get(type(link))
    # Not registered by any plugin (defined by the operator), or registered by at least
    # one global plugin: either way it is not restricted to a team.
    if link_teams is None or None in link_teams:
        return True
    return team_name in link_teams


@cache
def get_scheduling_class_teams() -> dict[str, frozenset[str | None]]:
    """
    Map the qualname of every plugin-registered scheduling class to the teams that registered it.

    Covers timetables, partition mappers, windows, deadline references and priority weight
    strategies: the registries a Dag names directly, with no team-aware lookup in between.

    Keyed by qualname because that is what a serialized Dag records and what the scheduler
    resolves through ``get_timetables_plugins()`` and its siblings. Class identity is not
    stable enough to key on: the plugin loader executes a plugin file again under its own
    module entry, so a Dag importing a class from that file can hold a different class
    object with the same qualname as the one that was registered.

    A qualname registered by several plugins maps to all of their teams, and is then
    resolved least restrictively.

    Airflow's own timetables, partition mappers and windows are left out even if a plugin
    lists them: the decoder imports anything under those core paths directly and never
    consults plugins, so a plugin cannot own them.
    """
    teams: dict[str, set[str | None]] = {}
    for plugin in _get_plugins()[0]:
        for scheduling_class in (
            *plugin.timetables,
            *plugin.partition_mappers,
            *plugin.windows,
            *plugin.deadline_references,
            *plugin.priority_weight_strategies,
        ):
            name = qualname(scheduling_class)
            # The partition mapper prefix also covers core windows.
            if is_core_timetable_import_path(name) or is_core_partition_mapper_import_path(name):
                continue
            teams.setdefault(name, set()).add(plugin.team_name)
    return {name: frozenset(team_names) for name, team_names in teams.items()}


@cache
def get_timetables_plugins() -> dict[str, type[Timetable]]:
    """Collect and get timetable classes registered by plugins."""
    log.debug("Initialize extra timetables plugins")

    return {
        qualname(timetable_class): timetable_class
        for plugin in _get_plugins()[0]
        for timetable_class in plugin.timetables
    }


@cache
def get_partition_mapper_plugins() -> dict[str, type[PartitionMapper]]:
    """Collect and get partition mapper classes registered by plugins."""
    log.debug("Initialize extra partition mapper plugins")

    return {
        qualname(partition_mapper_cls): partition_mapper_cls
        for plugin in _get_plugins()[0]
        for partition_mapper_cls in plugin.partition_mappers
    }


@cache
def get_windows_plugins() -> dict[str, type[Window]]:
    """Collect and get window classes registered by plugins."""
    log.debug("Initialize extra window plugins")

    return {qualname(window_cls): window_cls for plugin in _get_plugins()[0] for window_cls in plugin.windows}


@cache
def get_deadline_references_plugins() -> dict[str, type[DeadlineReferenceType]]:
    """Collect and get deadline reference classes registered by plugins."""
    log.debug("Initialize extra deadline reference plugins")

    return {
        qualname(deadline_ref_cls): deadline_ref_cls
        for plugin in _get_plugins()[0]
        for deadline_ref_cls in plugin.deadline_references
    }


@cache
def integrate_macros_plugins() -> None:
    """Integrates macro plugins."""
    from airflow._shared.plugins_manager import (
        integrate_macros_plugins as _integrate_macros_plugins,
    )
    from airflow.sdk.execution_time import macros

    plugins, _ = _get_plugins()
    _integrate_macros_plugins(
        target_macros_module=macros,
        macros_module_name_prefix="airflow.sdk.execution_time.macros",
        plugins=plugins,
    )


def integrate_listener_plugins(listener_manager: ListenerManager) -> None:
    """Add listeners from plugins."""
    from airflow._shared.plugins_manager import (
        integrate_listener_plugins as _integrate_listener_plugins,
    )

    plugins, _ = _get_plugins()
    _integrate_listener_plugins(listener_manager, plugins=plugins)


def get_plugin_info(attrs_to_dump: Iterable[str] | None = None) -> list[dict[str, Any]]:
    """
    Dump plugins attributes.

    :param attrs_to_dump: A list of plugin attributes to dump
    """
    get_flask_plugins()
    get_fastapi_plugins()
    get_global_operator_extra_links()
    get_operator_extra_links()
    _get_ui_plugins()
    if not attrs_to_dump:
        attrs_to_dump = {
            "macros",
            "admin_views",
            "flask_blueprints",
            "fastapi_apps",
            "fastapi_root_middlewares",
            "external_views",
            "react_apps",
            "menu_links",
            "appbuilder_views",
            "appbuilder_menu_items",
            "global_operator_extra_links",
            "operator_extra_links",
            "source",
            "timetables",
            "listeners",
            "priority_weight_strategies",
        }
    plugins_info = []
    for plugin in _get_plugins()[0]:
        info: dict[str, Any] = {"name": plugin.name, "team_name": plugin.team_name}
        for attr in attrs_to_dump:
            if attr in ("global_operator_extra_links", "operator_extra_links"):
                info[attr] = [f"<{qualname(d.__class__)} object>" for d in getattr(plugin, attr)]
            elif attr in ("macros", "timetables", "priority_weight_strategies"):
                info[attr] = [qualname(d) for d in getattr(plugin, attr)]
            elif attr == "listeners":
                # listeners may be modules or class instances
                info[attr] = [d.__name__ if inspect.ismodule(d) else qualname(d) for d in plugin.listeners]
            elif attr == "appbuilder_views":
                info[attr] = [
                    {**d, "view": qualname(d["view"].__class__) if "view" in d else None}
                    for d in plugin.appbuilder_views
                ]
            elif attr == "flask_blueprints":
                info[attr] = [
                    f"<{qualname(d.__class__)}: name={d.name!r} import_name={d.import_name!r}>"
                    for d in plugin.flask_blueprints
                ]
            elif attr == "fastapi_apps":
                info[attr] = [
                    {**d, "app": qualname(d["app"].__class__) if "app" in d else None}
                    for d in plugin.fastapi_apps
                ]
            elif attr == "fastapi_root_middlewares":
                # remove args and kwargs from plugin info to hide potentially sensitive info.
                info[attr] = [
                    {
                        k: (v if k != "middleware" else qualname(middleware_dict["middleware"]))
                        for k, v in middleware_dict.items()
                        if k not in ("args", "kwargs")
                    }
                    for middleware_dict in plugin.fastapi_root_middlewares
                ]
            else:
                info[attr] = getattr(plugin, attr)
        plugins_info.append(info)
    return plugins_info


@cache
def get_priority_weight_strategy_plugins() -> dict[str, type[PriorityWeightStrategy]]:
    """Collect and get priority weight strategy classes registered by plugins."""
    log.debug("Initialize extra priority weight strategy plugins")

    plugins_priority_weight_strategy_classes = {
        qualname(priority_weight_strategy_class): priority_weight_strategy_class
        for plugin in _get_plugins()[0]
        for priority_weight_strategy_class in plugin.priority_weight_strategies
    }
    return plugins_priority_weight_strategy_classes


def get_import_errors() -> dict[str, str]:
    """Get import errors encountered during plugin loading."""
    return _get_plugins()[1]


def validate_plugin_teams() -> None:
    """
    Validate that every team-scoped plugin references a team that exists in the database.

    Only enforced when multi-team mode is enabled. This must run in a context with
    metadata database access (the API server) — never in the Dag processor, triggerer,
    or workers, which reach the database only through the Execution API.

    A plugin that declares a ``team_name`` not present in the database is recorded as a
    plugin import error (surfaced like any other plugin load failure) and logged, rather
    than raising, so a single misconfigured plugin does not stop the API server and every
    other plugin from starting.
    """
    if not conf.getboolean("core", "multi_team"):
        return

    from airflow.models.team import Team

    plugins, import_errors = _get_plugins()
    known_teams = Team.get_all_team_names()
    for plugin in plugins:
        if plugin.team_name is None or plugin.team_name in known_teams:
            continue
        message = (
            f"Plugin '{plugin.name}' is assigned to team '{plugin.team_name}', which does not exist. "
            "Create a team with `airflow teams create <team_name>`, "
            "or update the plugin to use an existing team."
        )
        log.warning(message)
        source = str(plugin.source) if plugin.source else plugin.name or ""
        import_errors[source] = message
