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
"""
Turn resolved Polaris table-maintenance policies into Iceberg's Spark procedure calls.

Polaris stores and resolves maintenance policies but does not run them. :func:`to_spark_sql`
turns one item returned by ``PolarisGetApplicablePoliciesOperator(per_table=True)`` into the
``CALL`` statement that applies it, for a Spark task to run. It makes no network calls.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.apache.polaris.exceptions import PolarisInvalidArgumentError

log = logging.getLogger(__name__)

#: Policy type -> Iceberg Spark procedure.
PROCEDURES = {
    "system.snapshot-expiry": "expire_snapshots",
    "system.data-compaction": "rewrite_data_files",
    "system.metadata-compaction": "rewrite_manifests",
    "system.orphan-file-removal": "remove_orphan_files",
}

#: Policy ``config`` keys passed through as the procedure argument of the same name.
PASS_THROUGH = {
    "expire_snapshots": {"max_concurrent_deletes", "stream_results", "clean_expired_metadata"},
    "rewrite_data_files": {"sort_order", "where"},
    "rewrite_manifests": {"spec_id", "use_caching"},
    "remove_orphan_files": {"max_concurrent_deletes", "prefix_mismatch_mode", "prefix_listing"},
}

_SCHEMA_VERSION = "2025-02-03"
_STRATEGIES = {"bin-pack": "binpack", "binpack": "binpack", "sort": "sort"}
_PLAIN_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def to_spark_sql(
    item: dict[str, Any], *, spark_catalog: str | None = None, strict: bool = False, dry_run: bool = False
) -> str:
    """
    Return the Iceberg Spark ``CALL`` statement that applies one resolved maintenance policy.

    Policy settings map to procedure arguments as follows; times become UTC timestamps counted
    back from now:

    * ``system.snapshot-expiry`` -> ``expire_snapshots``: ``config.max_snapshot_age_days`` ->
      ``older_than``, ``config.min_snapshot_to_keep`` -> ``retain_last``.
    * ``system.data-compaction`` -> ``rewrite_data_files``: ``config.compaction_strategy``
      (``bin-pack`` or ``sort``) -> ``strategy``, ``config.target_file_size_bytes`` and every
      hyphenated key (an Iceberg rewrite option, e.g. ``min-input-files``) -> ``options``.
    * ``system.metadata-compaction`` -> ``rewrite_manifests``.
    * ``system.orphan-file-removal`` -> ``remove_orphan_files``: ``max_orphan_file_age_in_days``
      (at least 1, as Iceberg requires) -> ``older_than``; each of ``locations`` -> one
      statement with ``location``, which must lie inside the table's location.

    Other ``config`` keys named in :data:`PASS_THROUGH` are passed as the procedure argument of
    the same name. Any other key is dropped with a warning, or raises with ``strict=True``.

    :param item: One item of ``PolarisGetApplicablePoliciesOperator(per_table=True)``.
    :param spark_catalog: Name of the Polaris catalog in Spark, used for the ``CALL`` and to
        qualify the table; defaults to the Polaris catalog name.
    :param strict: Raise on ``config`` keys this function does not map, instead of dropping them.
    :param dry_run: Only list orphan files (``dry_run => true``). Raises for other policy types,
        which have no dry run.
    :return: The statement; for an orphan-file policy with several ``locations``, one statement
        per location, separated by ``;`` (``spark-sql`` and ``SparkSqlOperator`` run them all;
        PySpark's ``spark.sql()`` takes one at a time).
    :raises PolarisInvalidArgumentError: For a policy that is disabled, of an unsupported type or
        schema version, or that holds invalid values.
    """
    policy = item["policy"]
    procedure = PROCEDURES.get(policy["type"])
    if procedure is None:
        raise PolarisInvalidArgumentError(f"No Spark procedure for policy type {policy['type']!r}.")
    content = policy.get("content") or {}
    if content.get("enable") is False:
        raise PolarisInvalidArgumentError(f"Policy {policy['name']!r} is disabled.")
    if content.get("version", _SCHEMA_VERSION) != _SCHEMA_VERSION:
        raise PolarisInvalidArgumentError(f"Unsupported policy schema version {content['version']!r}.")
    if dry_run and procedure != "remove_orphan_files":
        raise PolarisInvalidArgumentError(f"{procedure} has no dry run; dry_run is for orphan-file removal.")

    config = dict(content.get("config") or {})
    catalog = spark_catalog or item["catalog"]
    # Qualify the table with its catalog: Iceberg tries the first part of a shorter name as a catalog,
    # logging a warning when there is none and failing when a Spark catalog is named like it.
    table = [catalog, *item["namespace"], item["table"]]
    args: dict[str, Any] = {"table": ".".join(map(_identifier, table))}
    locations: list[str | None] = [None]  # one statement, at the table's own location
    if procedure == "expire_snapshots":
        if "max_snapshot_age_days" in config:
            args["older_than"] = _days_ago("max_snapshot_age_days", config.pop("max_snapshot_age_days"), 0)
        if "min_snapshot_to_keep" in config:
            args["retain_last"] = _count("min_snapshot_to_keep", config.pop("min_snapshot_to_keep"))
    elif procedure == "rewrite_data_files":
        if "compaction_strategy" in config:
            strategy = config.pop("compaction_strategy")
            if strategy not in _STRATEGIES:
                raise PolarisInvalidArgumentError(f"Unknown compaction_strategy {strategy!r}.")
            args["strategy"] = _STRATEGIES[strategy]
        if "target_file_size_bytes" in config:  # Polaris's spelling; Iceberg's own key wins
            config.setdefault("target-file-size-bytes", config.pop("target_file_size_bytes"))
        if options := {key: _option(config.pop(key)) for key in sorted(config) if "-" in key}:
            args["options"] = options
    elif procedure == "remove_orphan_files":
        if "max_orphan_file_age_in_days" in content:
            args["older_than"] = _days_ago(
                "max_orphan_file_age_in_days", content["max_orphan_file_age_in_days"], 1
            )
        if dry_run:
            args["dry_run"] = True
        if content.get("locations"):
            locations = [
                _inside_table(location, item.get("table_location")) for location in content["locations"]
            ]

    for key in sorted(config):
        if key in PASS_THROUGH[procedure]:
            args[key] = config[key]
        elif strict:
            raise PolarisInvalidArgumentError(
                f"Policy {policy['name']!r}: {procedure} has no setting {key!r}."
            )
        else:
            log.warning(
                "Policy %r: ignoring config key %r, which %s does not support.",
                policy["name"],
                key,
                procedure,
            )

    return ";\n".join(
        _call(catalog, procedure, args if location is None else {**args, "location": location})
        for location in locations
    )


def _call(catalog: str, procedure: str, args: dict[str, Any]) -> str:
    arguments = ", ".join(f"{name} => {_literal(value)}" for name, value in args.items())
    return f"CALL {_identifier(catalog)}.system.{procedure}({arguments})"


def _inside_table(location: str, table_location: str | None) -> str:
    """Return ``location`` if it lies inside the table's location; orphan removal must not reach wider."""
    table = (table_location or "").rstrip("/")
    if not table or ".." in location.split("/") or not f"{location.rstrip('/')}/".startswith(f"{table}/"):
        raise PolarisInvalidArgumentError(
            f"Orphan-file location {location!r} is not inside the table location {table_location!r}; "
            "removing orphan files there could delete other tables' files."
        )
    return location


def _days_ago(name: str, days: Any, minimum: int) -> datetime:
    if isinstance(days, bool) or not isinstance(days, (int, float)) or days < minimum:
        raise PolarisInvalidArgumentError(
            f"{name} must be a number of days, at least {minimum}; got {days!r}."
        )
    return datetime.now(UTC) - timedelta(days=days)


def _count(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise PolarisInvalidArgumentError(f"{name} must be a whole number, at least 1; got {value!r}.")
    return value


def _identifier(name: str) -> str:
    """Quote a Spark identifier part with backticks unless it is a plain name."""
    return name if _PLAIN_IDENTIFIER.fullmatch(name) else "`" + name.replace("`", "``") + "`"


def _option(value: Any) -> str:
    """Iceberg rewrite options are strings; booleans in Java's spelling."""
    return str(value).lower() if isinstance(value, bool) else str(value)


def _literal(value: Any) -> str:
    """Render a procedure argument as a Spark SQL literal."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if isinstance(value, dict):
        return "map(" + ", ".join(f"{_literal(key)}, {_literal(item)}" for key, item in value.items()) + ")"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"
