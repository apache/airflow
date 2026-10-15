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
"""Utilities module for cli."""

from __future__ import annotations

import functools
import logging
import os
import re
import socket
import sys
import threading
import traceback
import warnings
from argparse import Namespace
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeVar, cast, overload

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.exceptions import AirflowException
from airflow.utils import cli_action_loggers
from airflow.utils.log.non_caching_file_handler import NonCachingFileHandler
from airflow.utils.platform import getuser, is_terminal_support_colors

T = TypeVar("T", bound=Callable)

if TYPE_CHECKING:
    from airflow.dag_processing.dagbag import BaggedDAG
    from airflow.sdk import DAG
    from airflow.serialization.definitions.dag import SerializedDAG

logger = logging.getLogger(__name__)


def _check_cli_args(args):
    if not args:
        raise ValueError("Args should be set")
    if not isinstance(args[0], Namespace):
        raise ValueError(
            f"1st positional argument should be argparse.Namespace instance, but is {type(args[0])}"
        )


def set_verbose_logging() -> None:
    """Enable debug output for the root logger and its handlers."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    for handler in root_logger.handlers:
        handler.setLevel(logging.DEBUG)


def action_cli(func=None, check_db=True):
    def action_logging(f: T) -> T:
        """
        Decorate function to execute function at the same time submitting action_logging but in CLI context.

        It will call action logger callbacks twice, one for
        pre-execution and the other one for post-execution.

        Action logger will be called with below keyword parameters:
            sub_command : name of sub-command
            start_datetime : start datetime instance by utc
            end_datetime : end datetime instance by utc
            full_command : full command line arguments
            user : current user
            log : airflow.models.log.Log ORM instance
            dag_id : dag id (optional)
            task_id : task_id (optional)
            logical_date : logical date (optional)
            team_name : team the command is scoped to (optional)
            error : exception instance if there's an exception

        :param f: function instance
        :return: wrapped function
        """

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            """
            Wrap cli functions; assume Namespace instance as first positional argument.

            :param args: Positional argument. It assumes to have Namespace instance
                at 1st positional argument
            :param kwargs: A passthrough keyword argument
            """
            _check_cli_args(args)
            metrics = _build_metrics(f.__name__, args[0])
            cli_action_loggers.on_pre_execution(**metrics)
            if getattr(args[0], "verbose", False):
                set_verbose_logging()
            try:
                # Check and run migrations if necessary
                if check_db:
                    from airflow.configuration import conf
                    from airflow.utils.db import check_and_run_migrations, synchronize_log_template

                    if conf.getboolean("database", "check_migrations"):
                        check_and_run_migrations()
                    synchronize_log_template()
                    # Set server context so validation logic (e.g. DagBundlesManager) runs correctly
                    original_ctx = os.environ.pop("_AIRFLOW_PROCESS_CONTEXT", None)
                    try:
                        os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "server"
                        return f(*args, **kwargs)
                    finally:
                        if original_ctx is None:
                            os.environ.pop("_AIRFLOW_PROCESS_CONTEXT", None)
                        else:
                            os.environ["_AIRFLOW_PROCESS_CONTEXT"] = original_ctx
                else:
                    return f(*args, **kwargs)
            except Exception as e:
                metrics["error"] = e
                raise
            finally:
                metrics["end_datetime"] = timezone.utcnow()
                cli_action_loggers.on_post_execution(**metrics)

        return cast("T", wrapper)

    if func:
        return action_logging(func)
    return action_logging


MASKED_VALUE = "*" * 8

# Options whose value is a secret on its own, keyed by the destination argparse stores the
# parsed value under. Masking is driven by what is present in the parsed namespace, so ``-p``
# is only treated as a password on commands where it parses into ``password`` (``api-server -p``
# parses into ``port`` and is left alone).
_SECRET_OPTIONS: dict[str, tuple[str, ...]] = {
    "password": ("-p", "--password"),
    "conn_password": ("--conn-password",),
    "basic_auth": ("-A", "--basic-auth"),
}
# Options whose value is JSON that may hold secrets under sensitive keys.
_JSON_OPTIONS: dict[str, tuple[str, ...]] = {
    "conn_json": ("--conn-json",),
    "conn_extra": ("--conn-extra",),
    "conf": ("-c", "--conf"),
    "dag_run_conf": ("--dag-run-conf",),
    "task_params": ("-t", "--task-params"),
    "env_vars": ("--env-vars",),
}
# Options whose value is a URI that may hold secrets in its credentials or query.
_URI_OPTIONS: dict[str, tuple[str, ...]] = {
    "conn_uri": ("--conn-uri",),
    "broker_api": ("-a", "--broker-api"),
}
# Short options of the commands that take one of the short options above, as
# ``(boolean flags, options taking a value)``, so a cluster such as ``-vpVALUE`` is walked the
# way argparse reads it. Keep in sync with the command definitions in ``cli_config`` and the
# FAB and Celery providers. Commands missing here are masked conservatively.
_SHORT_OPTION_ARITY: dict[tuple[str, str], tuple[str, str]] = {
    ("users", "create"): ("v", "eflpru"),
    ("users", "reset-password"): ("v", "epu"),
    ("celery", "flower"): ("Dv", "AHaclpu"),
    ("dags", "trigger"): ("v", "clor"),
    ("dags", "test"): ("v", "Bcf"),
    ("tasks", "test"): ("mv", "Bt"),
}


def _should_hide_key(key) -> bool:
    from airflow._shared.secrets_masker import _secrets_masker

    return bool(key) and isinstance(key, str) and _secrets_masker().should_hide_value_for_key(key)


def _redact_structure(obj):
    """
    Redact values whose key looks sensitive, at any depth.

    String values that themselves hold a JSON object or list are parsed and redacted too:
    ``--conn-json`` accepts ``extra`` as a JSON-encoded string as well as an object.
    """
    import json

    if isinstance(obj, dict):
        return {k: MASKED_VALUE if _should_hide_key(k) else _redact_structure(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact_structure(item) for item in obj]
    if isinstance(obj, str) and obj.lstrip()[:1] in ("{", "["):
        try:
            parsed = json.loads(obj)
        except ValueError:
            return obj
        redacted = _redact_structure(parsed)
        return obj if redacted == parsed else json.dumps(redacted)
    return obj


def _find_option_values(
    command: list[str],
    option_strings: tuple[str, ...],
    *,
    allow_abbrev: bool,
    short_option_arity: tuple[str, str] | None = None,
) -> list[tuple[int, str]]:
    """
    Locate every value given for an option in a recorded command, in any accepted spelling.

    argparse accepts ``--option VALUE``, ``--option=VALUE``, unambiguous abbreviations of
    long options (``--opt VALUE``), and for short options also ``-o VALUE``, ``-o=VALUE`` and
    ``-oVALUE``. Returns ``(index, prefix)`` pairs: the index of the element holding the
    value and the prefix to keep when rewriting it (empty for the separated forms).

    Every occurrence is returned: argparse keeps the last one, but all of them were typed.
    Abbreviations and short options are only matched with ``allow_abbrev``, i.e. when the
    parsed namespace confirms the command takes this option.

    A short option can also follow grouped flags (``-vpVALUE``, ``-vp VALUE``). With the
    command's ``short_option_arity`` the cluster is walked as argparse does: boolean flags are
    skipped and any other option ends the cluster by consuming its rest (``-uapple`` is a
    username). Without it, every character before the option is taken as a flag, which may
    mask more than the value but never less.
    """
    long_options = [o for o in option_strings if o.startswith("--")]
    short_options = [o for o in option_strings if not o.startswith("--")] if allow_abbrev else []
    positions: list[tuple[int, str]] = []
    for idx, argument in enumerate(command):
        if argument.startswith("--"):
            name, eq, _ = argument.partition("=")
            matches = (
                any(len(name) > 2 and option.startswith(name) for option in long_options)
                if allow_abbrev
                else name in long_options
            )
            if not matches:
                continue
            if eq:
                positions.append((idx, f"{name}="))
            elif idx + 1 < len(command):
                positions.append((idx + 1, ""))
        elif argument.startswith("-") and short_options:
            prefix = _find_short_option_prefix(argument, short_options, short_option_arity)
            if prefix is None:
                continue
            if prefix:
                positions.append((idx, prefix))
            elif idx + 1 < len(command):
                positions.append((idx + 1, ""))
    return positions


def _find_short_option_prefix(
    argument: str, short_options: list[str], short_option_arity: tuple[str, str] | None
) -> str | None:
    """
    Return the part of a short-option token that precedes the option value.

    ``None`` means the token does not carry the option; an empty string means the value is
    the next command element.
    """
    for pos in range(1, len(argument)):
        char = argument[pos]
        if f"-{char}" in short_options:
            rest = argument[pos + 1 :]
            if not rest:
                return ""
            return argument[: pos + 1] + ("=" if rest.startswith("=") else "")
        is_flag = char in short_option_arity[0] if short_option_arity else char.isalnum()
        if not is_flag:
            return None
    return None


def _redact_json_argument(value: str) -> str:
    """
    Redact sensitive entries in a JSON command-line argument.

    Nested structures are walked, because a secret one level down is still a secret:
    ``--conn-extra '{"nested": {"password": "..."}}'`` records the password otherwise.

    A value that is not JSON we can parse is redacted whole. These options carry
    connection material, so a value that cannot be inspected is not safe to record.
    """
    import json

    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError, ValueError):
        return MASKED_VALUE
    return json.dumps(_redact_structure(parsed))


_URL_BYTES_TO_REMOVE = str.maketrans("", "", "\t\r\n")
_C0_CONTROL_OR_SPACE = "".join(chr(i) for i in range(0x21))


def _normalize_url_like_urlsplit(value: str) -> str:
    """Drop the characters ``urlsplit`` ignores, so the inner URI is seen as ``Connection`` sees it."""
    return value.translate(_URL_BYTES_TO_REMOVE).lstrip(_C0_CONTROL_OR_SPACE)


def _redact_uri_argument(value: str) -> str:
    """
    Redact the secrets a URI command-line argument can carry.

    That is the password in the credentials, query parameters with a sensitive name
    (``?api_key=...``) and sensitive entries of the URL-encoded ``__extra__`` JSON that
    connection URIs accept.
    """
    from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlunparse

    value = _normalize_url_like_urlsplit(value)
    scheme, separator, rest = value.partition("://")
    rest = _normalize_url_like_urlsplit(rest)
    # ``Connection`` accepts a protocol in the host: ``http://https://user:pass@host``. A ``://``
    # elsewhere (``?next=http://...``) does not make the URI double-scheme.
    if separator and re.match(r"[A-Za-z][A-Za-z0-9+.-]*://", rest):
        return f"{scheme}{separator}{_redact_uri_argument(rest)}"
    try:
        parsed_uri = urlparse(value)
        has_password = parsed_uri.password is not None
    except ValueError:
        return MASKED_VALUE
    netloc = parsed_uri.netloc
    # A ``:`` followed later by ``@`` that the parse did not take as the credentials may still be
    # one (``u:pa?ss@h`` splits it into the query), so the whole value is masked. This errs on the
    # side of masking: ``user@host:port`` is kept, anything shaped like ``user:pass@`` is not.
    after_authority = urlunparse(
        ("", "", parsed_uri.path, parsed_uri.params, parsed_uri.query, parsed_uri.fragment)
    )
    unlocated = after_authority if has_password else rest if separator else value
    # ``Connection`` decodes query values, so ``u%3Apass%40host`` is a credential too.
    if any(re.search(r":.*@", text, re.DOTALL) for text in (unlocated, unquote(unlocated))):
        return MASKED_VALUE
    query = parsed_uri.query
    params = parse_qsl(query, keep_blank_values=True) if query else []
    if has_password:
        userinfo, _, host = netloc.rpartition("@")
        netloc = f"{userinfo.partition(':')[0]}:{MASKED_VALUE}@{host}"

    if params:
        redacted_params = []
        for key, item in params:
            if key == "__extra__":
                redacted_params.append((key, _redact_json_argument(item)))
            elif _should_hide_key(key):
                redacted_params.append((key, MASKED_VALUE))
            else:
                # ``Connection`` parses every query value as JSON into ``extra``.
                redacted_params.append((key, _redact_structure(item)))
        if redacted_params != params:
            query = urlencode(redacted_params, safe="*")

    return urlunparse(
        (parsed_uri.scheme, netloc, parsed_uri.path, parsed_uri.params, query, parsed_uri.fragment)
    )


def _mask_value_token(argument: str, value: str, redact) -> str:
    """
    Redact a parsed option value where it forms a whole token or ends a short-option cluster.

    A cluster of grouped short flags (``-vpVALUE``) carries the value of its last option, so a
    single-dash token ending with the value is rewritten too. Other tokens are left alone, so
    a short value is not masked wherever its text happens to appear.
    """
    if argument == value:
        return redact(value)
    if argument.startswith("-") and not argument.startswith("--") and argument.endswith(value):
        return argument[: -len(value)] + redact(value)
    return argument


def _mask_parsed_arguments(full_command: list[str], namespace) -> list[str]:
    """
    Mask secrets supplied through any supported argument spelling in the recorded command line.

    The values are taken from the parsed namespace, so the masking does not depend on how
    the option was spelled on the command line.
    """
    parsed = vars(namespace) if isinstance(namespace, Namespace) else {}
    full_command = list(full_command)
    short_option_arity = (
        _SHORT_OPTION_ARITY.get((full_command[1], full_command[2])) if len(full_command) > 2 else None
    )

    # argparse stores a default for every option of the command, so presence in the namespace,
    # not the truthiness of the last value, tells whether the command takes the option: a final
    # empty value (``--password ''``) must not unmask the earlier occurrences.
    for dest, option_strings in _SECRET_OPTIONS.items():
        if dest not in parsed:
            continue
        value = parsed[dest]
        for idx, prefix in _find_option_values(
            full_command,
            option_strings,
            allow_abbrev=True,
            short_option_arity=short_option_arity,
        ):
            full_command[idx] = prefix + MASKED_VALUE
        if isinstance(value, str) and value:
            full_command = [
                _mask_value_token(argument, value, lambda _: MASKED_VALUE) for argument in full_command
            ]

    for options, redact in ((_JSON_OPTIONS, _redact_json_argument), (_URI_OPTIONS, _redact_uri_argument)):
        for dest, option_strings in options.items():
            value = parsed.get(dest)
            for idx, prefix in _find_option_values(
                full_command,
                option_strings,
                allow_abbrev=dest in parsed,
                short_option_arity=short_option_arity,
            ):
                full_command[idx] = prefix + redact(full_command[idx][len(prefix) :])
            if isinstance(value, str) and value:
                full_command = [_mask_value_token(argument, value, redact) for argument in full_command]

    return full_command


def _mask_full_command(full_command: list[str], namespace) -> list[str]:
    """Mask the secrets in the command line recorded in the audit log."""
    from airflow._shared.secrets_masker import _secrets_masker

    sub_commands_to_check_for_sensitive_fields = {"users", "connections"}
    sub_commands_to_check_for_sensitive_key = {"variables"}
    sensitive_fields = {"-p", "--password", "--conn-password"}
    # The long spellings name a password on every subcommand, including ones contributed by
    # providers (``keycloak-auth-manager ... --password``), so they are masked everywhere. ``-p``
    # is only a password on the subcommands above; elsewhere it is e.g. ``--port``.
    always_sensitive_fields = {"--password", "--conn-password"}
    full_command = list(full_command)
    sub_command = full_command[1] if len(full_command) > 1 else None
    # For cases when value under sub_commands_to_check_for_sensitive_key have sensitive info
    if sub_command in sub_commands_to_check_for_sensitive_key:
        # Identify the value from the parsed command rather than from its position in
        # argv. The key and value are not reliably the last two arguments: any supported
        # trailing option displaces them (``variables set KEY VALUE --description TEXT``
        # ends in ``--description TEXT``), which left the value recorded in clear.
        key = getattr(namespace, "key", None)
        value = getattr(namespace, "value", None)
        if key and value and _secrets_masker().should_hide_value_for_key(key):
            full_command = [MASKED_VALUE if arg == value else arg for arg in full_command]
    else:
        fields = (
            sensitive_fields
            if sub_command in sub_commands_to_check_for_sensitive_fields
            else always_sensitive_fields
        )
        for idx, command in enumerate(full_command):
            if command in fields:
                # For cases when password is passed as "--password xyz" (with space between key and value)
                if idx + 1 < len(full_command):
                    full_command[idx + 1] = MASKED_VALUE
            else:
                # For cases when password is passed as "--password=xyz" (with '=' between key and value)
                for sensitive_field in fields:
                    if command.startswith(f"{sensitive_field}="):
                        full_command[idx] = f"{sensitive_field}={MASKED_VALUE}"

    # Options carrying secrets (on their own, or inside JSON and URIs) are masked by their
    # parsed value, whatever spelling was used for them on the command line.
    return _mask_parsed_arguments(full_command, namespace)


def _build_metrics(func_name, namespace):
    """
    Build metrics dict from function args.

    It assumes that function arguments is from airflow.bin.cli module's function
    and has Namespace instance where it optionally contains "dag_id", "task_id",
    "logical_date", and a team name.

    :param func_name: name of function
    :param namespace: Namespace instance from argparse
    :return: dict with metrics
    """
    try:
        full_command = _mask_full_command(sys.argv, namespace)
    except Exception as e:
        # This runs before the command itself, so a masking bug must not abort the command, and
        # must not record anything a secret could be in; the error text could quote it too.
        logger.warning(
            "Could not mask the command line for the audit log (%s); recording it masked", type(e).__name__
        )
        full_command = [*sys.argv[:1], MASKED_VALUE]

    metrics = {
        "sub_command": func_name,
        "start_datetime": timezone.utcnow(),
        "full_command": f"{full_command}",
        "user": getuser(),
    }

    if not isinstance(namespace, Namespace):
        raise ValueError(
            f"namespace argument should be argparse.Namespace instance, but is {type(namespace)}"
        )
    tmp_dic = vars(namespace)
    metrics["dag_id"] = tmp_dic.get("dag_id")
    metrics["task_id"] = tmp_dic.get("task_id")
    metrics["logical_date"] = tmp_dic.get("logical_date")
    # The ``teams`` commands take the team they act on as a positional ``name``; everything else
    # scoped to a team (``triggerer``, ``pools set``) takes it as ``--team-name``.
    metrics["team_name"] = tmp_dic.get("team_name") or (
        tmp_dic.get("name") if func_name.startswith("team_") else None
    )
    metrics["host_name"] = socket.gethostname()

    return metrics


def process_subdir(subdir: str | None):
    """Expand path to absolute by replacing 'DAGS_FOLDER', '~', '.', etc."""
    if subdir:
        if not settings.DAGS_FOLDER:
            raise ValueError("DAGS_FOLDER variable in settings should be filled.")
        subdir = subdir.replace("DAGS_FOLDER", settings.DAGS_FOLDER)
        subdir = os.path.abspath(os.path.expanduser(subdir))
    return subdir


def get_dag_by_file_location(dag_id: str):
    """Return DAG of a given dag_id by looking up file location."""
    # TODO: AIP-66 - investigate more, can we use serdag?
    from airflow.dag_processing.dagbag import DagBag
    from airflow.models import DagModel

    # Benefit is that logging from other dags in dagbag will not appear
    dag_model = DagModel.get_current(dag_id)
    if dag_model is None:
        raise AirflowException(
            f"Dag {dag_id!r} could not be found; either it does not exist or it failed to parse."
        )
    # This method is called only when we explicitly do not have a bundle name
    dagbag = DagBag(dag_folder=dag_model.fileloc)
    return dagbag.dags[dag_id]


def _search_for_dag_file(val: str | None) -> str | None:
    """
    Search for the file referenced at fileloc.

    By the time we get to this function, we've already run this `val` through `process_subdir`
    and loaded the DagBag there and came up empty.  So here, if `val` is a file path, we make
    a last ditch effort to try and find a dag file with the same name in our dags folder. (This
    avoids the unnecessary dag parsing that would occur if we just parsed the dags folder).

    If `val` is a path to a file, this likely means that the serializing process had a dags_folder
    equal to only the dag file in question. This prevents us from determining the relative location.
    And if the paths are different between worker and dag processor / scheduler, then we won't find
    the dag at the given location.
    """
    if val and Path(val).suffix in (".zip", ".py"):
        matches = list(Path(settings.DAGS_FOLDER).rglob(Path(val).name))
        if len(matches) == 1:
            return matches[0].as_posix()
    return None


@overload
def get_bagged_dag(
    bundle_names: list | None,
    dag_id: str,
    dagfile_path: str | None = None,
    *,
    allow_lang_sdk_dag: Literal[False] = False,
) -> DAG: ...


@overload
def get_bagged_dag(
    bundle_names: list | None,
    dag_id: str,
    dagfile_path: str | None = None,
    *,
    allow_lang_sdk_dag: bool,
) -> BaggedDAG: ...


def get_bagged_dag(
    bundle_names: list | None,
    dag_id: str,
    dagfile_path: str | None = None,
    *,
    allow_lang_sdk_dag: bool = False,
) -> BaggedDAG:
    """
    Return DAG of a given dag_id.

    First we'll try to use the given subdir.  If that doesn't work, we'll try to
    find the correct path (assuming it's a file) and failing that, use the configured
    dags folder.

    :param allow_lang_sdk_dag: when ``True``, also parses native Lang-SDK Dag files with their runtime,
        and may return such a Dag as a ``LangSDKSerializedDAG``.
    """
    from airflow.dag_processing.dagbag import BundleDagBag, LangSDKSerializedDAG, sync_bag_to_db
    from airflow.sdk.definitions._internal.dag_parsing_context import _airflow_parsing_context_manager

    def check_dag(dag: BaggedDAG) -> BaggedDAG:
        # TODO: Support running a Lang-SDK Dag directly from the CLI.
        if not allow_lang_sdk_dag and isinstance(dag, LangSDKSerializedDAG):
            raise SystemExit(f"Dag {dag_id!r} is a native Lang-SDK Dag, which this command cannot run.")
        return dag

    manager = DagBundlesManager()
    for bundle_name in bundle_names or ():
        bundle = manager.get_bundle(bundle_name)
        bundle.initialize()
        with _airflow_parsing_context_manager(dag_id=dag_id):
            dagbag = BundleDagBag(
                dag_folder=dagfile_path or bundle.path,
                bundle_path=bundle.path,
                bundle_name=bundle.name,
                parse_lang_sdk_files=allow_lang_sdk_dag,
            )
        if dag := dagbag.dags.get(dag_id):
            return check_dag(dag)

    manager.sync_bundles_to_db()
    for bundle in manager.get_all_dag_bundles():
        bundle.initialize()
        with _airflow_parsing_context_manager(dag_id=dag_id):
            dagbag = BundleDagBag(
                dag_folder=dagfile_path or bundle.path,
                bundle_path=bundle.path,
                bundle_name=bundle.name,
                parse_lang_sdk_files=allow_lang_sdk_dag,
            )
            sync_bag_to_db(dagbag, bundle.name, bundle.version)
        if dag := dagbag.dags.get(dag_id):
            return check_dag(dag)
    raise AirflowException(
        f"Dag {dag_id!r} could not be found; either it does not exist or it failed to parse."
    )


def get_db_dag(bundle_names: list | None, dag_id: str, dagfile_path: str | None = None) -> SerializedDAG:
    """
    Return DAG of a given dag_id.

    This gets a serialized dag from the database.
    """
    from airflow.models.serialized_dag import SerializedDagModel

    if dag := SerializedDagModel.get_dag(dag_id):
        return dag
    raise AirflowException(f"Dag {dag_id!r} could not be found in the database.")


def get_dags(bundle_names: list | None, dag_id: str, use_regex: bool = False, from_db: bool = False):
    """Return DAG(s) matching a given regex or dag_id."""
    from airflow.dag_processing.dagbag import BundleDagBag

    bundle_names = bundle_names or []

    if not use_regex:
        if from_db:
            return [get_db_dag(bundle_names=bundle_names, dag_id=dag_id)]
        return [get_bagged_dag(bundle_names=bundle_names, dag_id=dag_id)]

    def _find_dag(bundle):
        bundle.initialize()
        dagbag = BundleDagBag(dag_folder=bundle.path, bundle_path=bundle.path, bundle_name=bundle.name)
        matched_dags = [dag for dag in dagbag.dags.values() if re.search(dag_id, dag.dag_id)]
        return matched_dags

    manager = DagBundlesManager()
    matched_dags = []
    for bundle_name in bundle_names:
        bundle = manager.get_bundle(bundle_name)
        matched_dags = _find_dag(bundle)
        if matched_dags:
            break
    if not matched_dags:
        # Search in all bundles
        all_bundles = list(manager.get_all_dag_bundles())
        for bundle in all_bundles:
            matched_dags = _find_dag(bundle)
            if matched_dags:
                break
    if not matched_dags:
        raise AirflowException(
            f"dag_id could not be found with regex: {dag_id}. Either the dag did not exist or "
            f"it failed to parse."
        )
    return matched_dags


def setup_locations(process, pid=None, stdout=None, stderr=None, log=None):
    """Create logging paths."""
    if not stderr:
        stderr = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process}.err")
    if not stdout:
        stdout = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process}.out")
    if not log:
        log = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process}.log")

    if not pid:
        pid = os.path.join(settings.AIRFLOW_HOME, f"airflow-{process}.pid")
    else:
        pid = os.path.abspath(pid)

    return pid, stdout, stderr, log


def setup_logging(filename):
    """Create log file handler for daemon process."""
    root = logging.getLogger()
    handler = NonCachingFileHandler(filename)
    formatter = logging.Formatter(settings.SIMPLE_LOG_FORMAT)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    return handler.stream


def print_banner() -> None:
    """Print the Airflow ASCII art banner, unless JSON logging is enabled."""
    from airflow.configuration import conf

    if not conf.getboolean("logging", "json_logs", fallback=False):
        print(settings.HEADER)


def sigint_handler(sig, frame):
    """
    Return without error on SIGINT or SIGTERM signals in interactive command mode.

    e.g. CTRL+C or kill <PID>
    """
    sys.exit(0)


def sigquit_handler(sig, frame):
    """
    Help debug deadlocks by printing stacktraces when this gets a SIGQUIT.

    e.g. kill -s QUIT <PID> or CTRL+
    """
    print(f"Dumping stack traces for all threads in PID {os.getpid()}")
    id_to_name = {th.ident: th.name for th in threading.enumerate()}
    code = []
    for thread_id, stack in sys._current_frames().items():
        code.append(f"\n# Thread: {id_to_name.get(thread_id, '')}({thread_id})")
        for filename, line_number, name, line in traceback.extract_stack(stack):
            code.append(f'File: "{filename}", line {line_number}, in {name}')
            if line:
                code.append(f"  {line.strip()}")
    print("\n".join(code))


class ColorMode:
    """Coloring modes. If `auto` is then automatically detected."""

    ON = "on"
    OFF = "off"
    AUTO = "auto"


def should_use_colors(args) -> bool:
    """Process arguments and decide whether to enable color in output."""
    if args.color == ColorMode.ON:
        return True
    if args.color == ColorMode.OFF:
        return False
    return is_terminal_support_colors()


def suppress_logs_and_warning(f: T) -> T:
    """Suppress logging and warning messages in cli functions."""

    @functools.wraps(f)
    def _wrapper(*args, **kwargs):
        _check_cli_args(args)
        if args[0].verbose:
            f(*args, **kwargs)
        else:
            from airflow._shared.logging.structlog import respect_stdlib_disable

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                logging.disable(logging.CRITICAL)

                def drop(*_, **__):
                    from structlog import DropEvent

                    raise DropEvent()

                old_fn = respect_stdlib_disable.__code__
                respect_stdlib_disable.__code__ = drop.__code__
                try:
                    f(*args, **kwargs)
                finally:
                    # logging output again depends on the effective
                    # levels of individual loggers
                    logging.disable(logging.NOTSET)
                    respect_stdlib_disable.__code__ = old_fn

    return cast("T", _wrapper)


def validate_dag_bundle_arg(bundle_names: list[str]) -> None:
    """Make sure only known bundles are passed as arguments."""
    known_bundles = {b.name for b in DagBundlesManager().get_all_dag_bundles()}

    unknown_bundles: set[str] = set(bundle_names) - known_bundles
    if unknown_bundles:
        raise SystemExit(f"Bundles not found: {', '.join(unknown_bundles)}")


def should_enable_hot_reload(args) -> bool:
    """Check whether hot-reload should be enabled based on --dev flag or DEV_MODE env var."""
    if getattr(args, "dev", False):
        return True
    return os.getenv("DEV_MODE", "false").lower() == "true"
