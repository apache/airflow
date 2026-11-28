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

import pytest
import time_machine

from airflow.providers.apache.polaris.exceptions import PolarisInvalidArgumentError
from airflow.providers.apache.polaris.utils.maintenance import to_spark_sql

NOW = "2026-10-27 12:00:00+00:00"
CALL = "CALL analytics.system"


def _item(policy_type: str, content: dict, *, table_location: str | None = None) -> dict:
    """An item as PolarisGetApplicablePoliciesOperator(per_table=True) returns it."""
    item = {
        "catalog": "analytics",
        "namespace": ["sales"],
        "table": "orders",
        "policy": {"name": "p", "type": policy_type, "inherited": True, "content": content},
    }
    if table_location:
        item["table_location"] = table_location
    return item


@pytest.fixture(autouse=True)
def _frozen_clock():
    with time_machine.travel(NOW, tick=False):
        yield


class TestSnapshotExpiry:
    def test_maps_age_and_snapshots_to_keep(self):
        item = _item(
            "system.snapshot-expiry",
            {
                "version": "2025-02-03",
                "enable": True,
                "config": {"max_snapshot_age_days": 7, "min_snapshot_to_keep": 3},
            },
        )
        assert to_spark_sql(item) == (
            f"{CALL}.expire_snapshots(table => 'analytics.sales.orders', "
            "older_than => TIMESTAMP '2026-10-20 12:00:00+00:00', retain_last => 3)"
        )

    def test_fractional_days_and_pass_through_arguments(self):
        config = {"max_snapshot_age_days": 0.5, "clean_expired_metadata": True, "max_concurrent_deletes": 4}
        assert to_spark_sql(_item("system.snapshot-expiry", {"enable": True, "config": config})) == (
            f"{CALL}.expire_snapshots(table => 'analytics.sales.orders', "
            "older_than => TIMESTAMP '2026-10-27 00:00:00+00:00', "
            "clean_expired_metadata => true, max_concurrent_deletes => 4)"
        )

    def test_keeps_sub_second_precision(self):
        with time_machine.travel("2026-10-27 12:00:00.250000+00:00", tick=False):
            sql = to_spark_sql(
                _item("system.snapshot-expiry", {"enable": True, "config": {"max_snapshot_age_days": 1}})
            )
        assert "older_than => TIMESTAMP '2026-10-26 12:00:00.250000+00:00'" in sql

    def test_without_config_uses_the_table_properties(self):
        # Iceberg then applies the table's history.expire.* properties.
        assert to_spark_sql(_item("system.snapshot-expiry", {"enable": True})) == (
            f"{CALL}.expire_snapshots(table => 'analytics.sales.orders')"
        )

    @pytest.mark.parametrize(
        ("config", "match"),
        [
            ({"max_snapshot_age_days": -1}, "max_snapshot_age_days must be a number of days, at least 0"),
            ({"max_snapshot_age_days": "7"}, "max_snapshot_age_days must be a number"),
            ({"max_snapshot_age_days": True}, "max_snapshot_age_days must be a number"),
            ({"min_snapshot_to_keep": 0}, "min_snapshot_to_keep must be a whole number, at least 1"),
            ({"min_snapshot_to_keep": 1.5}, "min_snapshot_to_keep must be a whole number"),
        ],
    )
    def test_invalid_values(self, config, match):
        with pytest.raises(PolarisInvalidArgumentError, match=match):
            to_spark_sql(_item("system.snapshot-expiry", {"enable": True, "config": config}))


class TestDataCompaction:
    def test_maps_strategy_size_and_hyphenated_options(self):
        config = {
            "compaction_strategy": "bin-pack",
            "target_file_size_bytes": 134217728,
            "max-concurrent-file-group-rewrites": 5,
            "rewrite-all": False,
            "where": "day >= '2026-10-01'",
        }
        assert to_spark_sql(_item("system.data-compaction", {"enable": True, "config": config})) == (
            f"{CALL}.rewrite_data_files(table => 'analytics.sales.orders', strategy => 'binpack', "
            "options => map('max-concurrent-file-group-rewrites', '5', 'rewrite-all', 'false', "
            "'target-file-size-bytes', '134217728'), where => 'day >= \\'2026-10-01\\'')"
        )

    def test_iceberg_spelling_of_the_file_size_wins(self):
        config = {"target_file_size_bytes": 1, "target-file-size-bytes": 2}
        assert "options => map('target-file-size-bytes', '2')" in to_spark_sql(
            _item("system.data-compaction", {"enable": True, "config": config})
        )

    def test_sort_strategy_with_sort_order(self):
        config = {"compaction_strategy": "sort", "sort_order": "zorder(a, b)"}
        assert to_spark_sql(_item("system.data-compaction", {"enable": True, "config": config})) == (
            f"{CALL}.rewrite_data_files(table => 'analytics.sales.orders', strategy => 'sort', "
            "sort_order => 'zorder(a, b)')"
        )

    def test_unknown_strategy(self):
        with pytest.raises(PolarisInvalidArgumentError, match="Unknown compaction_strategy 'z-order'"):
            to_spark_sql(
                _item(
                    "system.data-compaction", {"enable": True, "config": {"compaction_strategy": "z-order"}}
                )
            )


class TestMetadataCompaction:
    def test_passes_spec_id_and_use_caching(self):
        config = {"spec_id": 1, "use_caching": False}
        assert to_spark_sql(_item("system.metadata-compaction", {"enable": True, "config": config})) == (
            f"{CALL}.rewrite_manifests(table => 'analytics.sales.orders', spec_id => 1, use_caching => false)"
        )


class TestOrphanFileRemoval:
    LOCATION = "s3://lake/sales/orders"

    def test_maps_age_and_config(self):
        content = {
            "enable": True,
            "max_orphan_file_age_in_days": 30,
            "config": {"prefix_mismatch_mode": "IGNORE"},
        }
        assert to_spark_sql(_item("system.orphan-file-removal", content)) == (
            f"{CALL}.remove_orphan_files(table => 'analytics.sales.orders', "
            "older_than => TIMESTAMP '2026-09-27 12:00:00+00:00', prefix_mismatch_mode => 'IGNORE')"
        )

    def test_dry_run(self):
        assert to_spark_sql(_item("system.orphan-file-removal", {"enable": True}), dry_run=True) == (
            f"{CALL}.remove_orphan_files(table => 'analytics.sales.orders', dry_run => true)"
        )

    def test_one_statement_per_location_inside_the_table(self):
        content = {"enable": True, "locations": [f"{self.LOCATION}/data", f"{self.LOCATION}/metadata/"]}
        item = _item("system.orphan-file-removal", content, table_location=f"{self.LOCATION}/")
        assert to_spark_sql(item).split(";\n") == [
            f"{CALL}.remove_orphan_files(table => 'analytics.sales.orders', location => '{self.LOCATION}/data')",
            f"{CALL}.remove_orphan_files(table => 'analytics.sales.orders', location => '{self.LOCATION}/metadata/')",
        ]

    @pytest.mark.parametrize(
        ("location", "table_location"),
        [
            ("s3://lake/sales", LOCATION),  # the namespace: other tables' files too
            ("s3://lake/sales/orders_archive", LOCATION),  # a sibling sharing the prefix
            (f"{LOCATION}/../customers", LOCATION),  # inside by prefix, outside once normalized
            (f"{LOCATION}/data", None),  # nothing to check against
        ],
    )
    def test_refuses_locations_outside_the_table(self, location, table_location):
        item = _item(
            "system.orphan-file-removal",
            {"enable": True, "locations": [location]},
            table_location=table_location,
        )
        with pytest.raises(PolarisInvalidArgumentError, match="is not inside the table location"):
            to_spark_sql(item)

    def test_age_below_one_day_is_refused(self):
        # Iceberg refuses older_than less than 24 hours ago.
        with pytest.raises(PolarisInvalidArgumentError, match="at least 1"):
            to_spark_sql(
                _item("system.orphan-file-removal", {"enable": True, "max_orphan_file_age_in_days": 0.5})
            )


class TestUnknownKeys:
    ITEM = _item("system.snapshot-expiry", {"enable": True, "config": {"max_ref_age_days": 3, "key1": "v"}})

    @pytest.mark.parametrize("key", ["key1", "max_ref_age_days"])
    def test_dropped_with_a_warning(self, caplog, key):
        assert to_spark_sql(self.ITEM) == f"{CALL}.expire_snapshots(table => 'analytics.sales.orders')"
        assert {
            "event": f"Policy 'p': ignoring config key {key!r}, which expire_snapshots does not support.",
            "log_level": "warning",
        } in caplog

    def test_strict_raises(self):
        with pytest.raises(PolarisInvalidArgumentError, match="expire_snapshots has no setting 'key1'"):
            to_spark_sql(self.ITEM, strict=True)

    def test_a_key_other_types_pass_through_is_unknown_here(self):
        item = _item("system.metadata-compaction", {"enable": True, "config": {"where": "x = 1"}})
        with pytest.raises(PolarisInvalidArgumentError, match="rewrite_manifests has no setting 'where'"):
            to_spark_sql(item, strict=True)


class TestPolicies:
    @pytest.mark.parametrize(
        ("policy_type", "content", "kwargs", "match"),
        [
            ("acme.retention", {"enable": True}, {}, "No Spark procedure for policy type 'acme.retention'"),
            ("system.snapshot-expiry", {"enable": False}, {}, "Policy 'p' is disabled"),
            (
                "system.snapshot-expiry",
                {"version": "2027-01-01", "enable": True},
                {},
                "schema version '2027-01-01'",
            ),
            (
                "system.snapshot-expiry",
                {"enable": True},
                {"dry_run": True},
                "expire_snapshots has no dry run",
            ),
        ],
    )
    def test_refused(self, policy_type, content, kwargs, match):
        with pytest.raises(PolarisInvalidArgumentError, match=match):
            to_spark_sql(_item(policy_type, content), **kwargs)

    def test_spark_catalog_names_the_call_and_qualifies_the_table(self):
        assert to_spark_sql(_item("system.snapshot-expiry", {"enable": True}), spark_catalog="polaris") == (
            "CALL polaris.system.expire_snapshots(table => 'polaris.sales.orders')"
        )

    def test_quotes_identifiers_and_qualifies_with_the_spark_catalog(self):
        item = _item("system.data-compaction", {"enable": True})
        item.update(namespace=["sales.eu", "raw"], table="order`s")
        assert to_spark_sql(item, spark_catalog="my-polaris") == (
            "CALL `my-polaris`.system.rewrite_data_files(table => '`my-polaris`.`sales.eu`.raw.`order``s`')"
        )

    def test_escapes_quotes_in_string_values(self):
        item = _item("system.data-compaction", {"enable": True, "config": {"where": r"name = 'O\'Hara'"}})
        assert "where => 'name = \\'O\\\\\\'Hara\\''" in to_spark_sql(item)
