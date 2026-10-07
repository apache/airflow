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

# ruff: noqa: S101
from __future__ import annotations

import importlib.util
import io
import json
import tarfile
from datetime import date
from pathlib import Path
from unittest import mock
from urllib.error import URLError

import pytest

SPEC = importlib.util.spec_from_file_location(
    "trace_target_support", Path(__file__).parents[1] / "trace_target_support.py"
)
trace = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(trace)

PYTHON = "3.15.0"
REGISTRY = {"registry": "https://pypi.org/simple"}
AMD64 = trace.scan_wheel_floors.build_environment(PYTHON, "x86_64")
ARM64 = trace.scan_wheel_floors.build_environment(PYTHON, "aarch64")


def member(name, **sections):
    return {"name": name, "source": {"editable": name}, **sections}


def third_party(name, version, **sections):
    return {"name": name, "version": version, "source": REGISTRY, **sections}


def walk(*packages, environment=AMD64, excluded=()):
    return {
        key[0]: record
        for key, record in trace.walk_lock({"package": list(packages)}, environment, set(excluded)).items()
    }


def release(version, *filenames, classifiers=(), uploaded="2026-10-01T00:00:00Z"):
    return {
        "info": {"version": version, "requires_python": ">=3.11", "classifiers": list(classifiers)},
        "urls": [
            {
                "filename": filename,
                "packagetype": "sdist" if filename.endswith(".tar.gz") else "bdist_wheel",
                "url": f"https://files.example/{filename}",
                "upload_time_iso_8601": uploaded,
            }
            for filename in filenames
        ],
    }


def sdist_payload(*members):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name in members:
            archive.addfile(tarfile.TarInfo(name))
    return buffer.getvalue()


def test_direct_and_transitive_packages_keep_their_owner_and_shortest_chain():
    records = walk(
        member("apache-airflow-core", dependencies=[{"name": "direct"}, {"name": "middle"}]),
        third_party("direct", "1.0", dependencies=[{"name": "leaf"}]),
        third_party("middle", "1.0", dependencies=[{"name": "deep"}]),
        third_party("deep", "1.0", dependencies=[{"name": "leaf"}]),
        third_party("leaf", "1.0"),
    )

    assert records["direct"]["relation"] == "direct"
    assert records["direct"]["declared_by"] == ["apache-airflow-core"]
    assert records["leaf"]["relation"] == "transitive"
    assert records["leaf"]["declared_by"] == []
    assert records["leaf"]["chain"] == ["apache-airflow-core", "direct", "leaf"]


def test_required_by_lists_every_member_reaching_a_package_but_only_declaring_members_own_it():
    records = walk(
        member("apache-airflow-core", dependencies=[{"name": "asgiref"}]),
        member("apache-airflow-providers-http", dependencies=[{"name": "apache-airflow-core"}]),
        member("apache-airflow-providers-slack", dependencies=[{"name": "asgiref"}]),
        third_party("asgiref", "3.12.1"),
    )

    assert records["asgiref"]["declared_by"] == ["apache-airflow-core", "apache-airflow-providers-slack"]
    assert records["asgiref"]["required_by"] == [
        "apache-airflow-core",
        "apache-airflow-providers-http",
        "apache-airflow-providers-slack",
    ]


def test_workspace_members_without_a_version_are_walked():
    records = walk(
        {
            "name": "apache-airflow-task-sdk",
            "source": {"editable": "task-sdk"},
            "dependencies": [{"name": "attrs"}],
        },
        third_party("attrs", "26.1.0"),
    )

    assert records["attrs"]["declared_by"] == ["apache-airflow-task-sdk"]


@pytest.mark.parametrize(
    ("environment", "expected"),
    [(AMD64, {"everywhere", "x86-only", "target-only"}), (ARM64, {"everywhere", "target-only"})],
)
def test_edges_whose_markers_fail_for_the_target_are_dropped(environment, expected):
    records = walk(
        member(
            "apache-airflow-core",
            dependencies=[
                {"name": "everywhere"},
                {"name": "x86-only", "marker": "platform_machine == 'x86_64'"},
                {"name": "target-only", "marker": "python_full_version >= '3.15'"},
                {"name": "older-only", "marker": "python_full_version < '3.15'"},
            ],
        ),
        *(third_party(name, "1.0") for name in ("everywhere", "x86-only", "target-only", "older-only")),
        environment=environment,
    )

    assert set(records) == expected


def test_duplicate_entries_resolve_by_edge_version_then_by_resolution_markers():
    records = walk(
        member(
            "apache-airflow-core",
            dependencies=[{"name": "pinned", "version": "2.0"}, {"name": "forked"}],
        ),
        third_party("pinned", "1.0"),
        third_party("pinned", "2.0"),
        third_party("forked", "1.0", **{"resolution-markers": ["python_full_version < '3.15'"]}),
        third_party("forked", "2.0", **{"resolution-markers": ["python_full_version >= '3.15'"]}),
    )

    assert records["pinned"]["version"] == "2.0"
    assert records["forked"]["version"] == "2.0"


def test_members_bring_every_extra_and_group_but_reached_packages_only_requested_extras():
    records = walk(
        member(
            "apache-airflow-providers-amazon",
            dependencies=[{"name": "apache-airflow-core"}, {"name": "boto", "extra": ["crt"]}],
            **{
                "optional-dependencies": {"s3fs": [{"name": "s3fs"}]},
                "dev-dependencies": {"dev": [{"name": "moto"}]},
            },
        ),
        member(
            "apache-airflow-core",
            **{
                "optional-dependencies": {"graphviz": [{"name": "graphviz"}]},
                "dev-dependencies": {"dev": [{"name": "core-dev-tool"}]},
            },
        ),
        third_party(
            "boto",
            "1.0",
            **{"optional-dependencies": {"crt": [{"name": "awscrt"}], "unused": [{"name": "never"}]}},
        ),
        *(
            third_party(name, "1.0")
            for name in ("s3fs", "moto", "graphviz", "core-dev-tool", "awscrt", "never")
        ),
    )

    assert "never" not in records
    assert records["awscrt"]["chain"] == ["apache-airflow-providers-amazon", "boto", "awscrt"]
    assert records["moto"]["declared_by"] == ["apache-airflow-providers-amazon"]
    assert records["core-dev-tool"]["required_by"] == ["apache-airflow-core"]
    assert records["graphviz"]["required_by"] == ["apache-airflow-core"]


def test_excluded_members_and_everything_only_they_need_are_skipped():
    records = walk(
        member(
            "apache-airflow",
            dependencies=[{"name": "apache-airflow-providers-apache-cassandra"}, {"name": "shared"}],
        ),
        member("apache-airflow-providers-apache-cassandra", dependencies=[{"name": "cassandra-driver"}]),
        third_party("cassandra-driver", "3.29"),
        third_party("shared", "1.0"),
        excluded={"apache-airflow-providers-apache-cassandra"},
    )

    assert set(records) == {"shared"}


@pytest.mark.parametrize(
    ("classifiers", "expected"),
    [
        (["Programming Language :: Python :: 3.14", "Programming Language :: Python :: 3.15"], "declared"),
        (
            ["Programming Language :: Python :: 3.14", "Programming Language :: Python :: 3 :: Only"],
            "not-declared",
        ),
        (["Programming Language :: Python :: 3"], "unknown"),
        ([], "unknown"),
    ],
)
def test_classifiers_are_read_as_a_three_state_support_signal(classifiers, expected):
    assert trace.get_declared_support({"classifiers": classifiers}, PYTHON) == expected


def audited(selected, latest):
    return {
        "name": "pkg",
        "version": selected["version"],
        "relation": "direct",
        "declared_by": ["apache-airflow-core"],
        "required_by": ["apache-airflow-core"],
        "chain": ["apache-airflow-core", "pkg"],
        "platform": "amd64",
        "selected": selected,
        "latest": latest,
    }


WHEEL_DECLARED = {"status": "wheel", "support": "declared", "uploaded": "2026-10-23"}
WHEEL_SILENT = {"status": "wheel", "support": "not-declared", "uploaded": "2026-07-14"}
COMPILED = {
    "status": "source-only",
    "disposition": "compiled-sources-review",
    "support": "unknown",
    "uploaded": "2026-07-14",
}
PURE = {
    "status": "source-only",
    "disposition": "pure-python-sdist",
    "support": "unknown",
    "uploaded": "2019-01-01",
}


@pytest.mark.parametrize(
    ("selected", "latest", "cutoff", "expected"),
    [
        pytest.param(
            WHEEL_SILENT | {"version": "3.12.1"},
            WHEEL_DECLARED | {"version": "3.13.0"},
            date(2026, 10, 21),
            "newer release declares target (3.13.0) (inside cooldown)",
            id="newer-release-declares-target-in-cooldown",
        ),
        pytest.param(
            WHEEL_SILENT | {"version": "3.12.1"},
            WHEEL_DECLARED | {"version": "3.13.0"},
            None,
            "newer release declares target (3.13.0)",
            id="newer-release-declares-target",
        ),
        pytest.param(
            COMPILED | {"version": "1.0"},
            WHEEL_SILENT | {"version": "2.0"},
            None,
            "upgrade to 2.0",
            id="newer-release-has-wheels",
        ),
        pytest.param(
            COMPILED | {"version": "1.0"},
            WHEEL_DECLARED | {"version": "2.0"},
            date(2026, 10, 21),
            "upgrade to 2.0 (inside cooldown)",
            id="newer-release-has-wheels-in-cooldown",
        ),
        pytest.param(
            PURE | {"version": "1.0"},
            WHEEL_SILENT | {"version": "2.0"},
            None,
            "builds without a compiler",
            id="pure-python-sdist-needs-no-upgrade",
        ),
        pytest.param(
            COMPILED | {"version": "1.0"},
            COMPILED | {"version": "1.0"},
            None,
            "not yet available",
            id="no-wheels-anywhere",
        ),
        pytest.param(
            PURE | {"version": "1.0"},
            PURE | {"version": "1.0"},
            None,
            "builds without a compiler",
            id="pure-python-sdist",
        ),
        pytest.param(
            WHEEL_SILENT | {"version": "1.0"},
            WHEEL_SILENT | {"version": "2.0"},
            None,
            "ok",
            id="missing-classifiers-are-not-a-gap",
        ),
        pytest.param(
            WHEEL_DECLARED | {"version": "1.0"},
            WHEEL_DECLARED | {"version": "2.0"},
            None,
            "ok",
            id="already-declared",
        ),
    ],
)
def test_disposition(selected, latest, cutoff, expected):
    assert trace.build_disposition(audited(selected, latest), cutoff) == expected


def test_audit_reports_the_locked_and_newest_release_for_each_platform():
    releases = {
        ("asgiref", "3.12.1"): release(
            "3.12.1",
            "asgiref-3.12.1-py3-none-any.whl",
            classifiers=["Programming Language :: Python :: 3.14"],
        ),
        ("asgiref", ""): release(
            "3.13.0",
            "asgiref-3.13.0-py3-none-any.whl",
            classifiers=["Programming Language :: Python :: 3.15"],
            uploaded="2026-10-23T08:00:00Z",
        ),
    }
    record = walk(
        member("apache-airflow-core", dependencies=[{"name": "asgiref"}]), third_party("asgiref", "3.12.1")
    )["asgiref"]

    with mock.patch.object(
        trace.audit_wheels, "fetch_release", side_effect=lambda name, version="": releases[name, version]
    ):
        result = trace.audit_record(record, "arm64", PYTHON, set())

    assert result["platform"] == "arm64"
    assert "source" not in result
    assert (result["selected"]["status"], result["selected"]["support"]) == ("wheel", "not-declared")
    assert (result["latest"]["version"], result["latest"]["support"], result["latest"]["uploaded"]) == (
        "3.13.0",
        "declared",
        "2026-10-23",
    )


def test_audit_classifies_the_sdist_of_a_locked_release_without_target_wheels():
    data = release("1.0", "native-1.0-cp314-cp314-manylinux_2_28_x86_64.whl", "native-1.0.tar.gz")
    record = walk(
        member("apache-airflow-core", dependencies=[{"name": "native"}]), third_party("native", "1.0")
    )["native"]

    with (
        mock.patch.object(trace.audit_wheels, "fetch_release", return_value=data),
        mock.patch.object(trace.audit_wheels, "fetch_bytes", return_value=sdist_payload("native-1.0/ext.c")),
    ):
        result = trace.audit_record(record, "amd64", PYTHON, set())

    assert result["selected"]["disposition"] == "compiled-sources-review"
    assert trace.build_disposition(result, None) == "not yet available"


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ({"url": "https://example.com/pkg-1.0-py3-none-any.whl"}, "wheel-url"),
        ({"url": "https://example.com/pkg-1.0.tar.gz"}, "source-url-review"),
        ({"git": "https://example.com/pkg.git"}, "source-review"),
    ],
)
def test_non_registry_sources_are_classified_without_a_release_lookup(source, expected):
    record = walk(
        member("apache-airflow-core", dependencies=[{"name": "pkg"}]),
        {"name": "pkg", "version": "1.0", "source": source},
    )["pkg"]

    with mock.patch.object(
        trace.audit_wheels, "fetch_release", return_value=release("1.0", "pkg-1.0-py3-none-any.whl")
    ):
        result = trace.audit_record(record, "amd64", PYTHON, set())

    assert result["selected"]["status"] == expected


LOCK = """
version = 1
requires-python = ">=3.11"

[[package]]
name = "apache-airflow-core"
source = { editable = "airflow-core" }
dependencies = [
    { name = "asgiref" },
    { name = "missing" },
    { name = "ready" },
]

[[package]]
name = "asgiref"
version = "3.12.1"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "missing"
version = "1.0"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "ready"
version = "1.0"
source = { registry = "https://pypi.org/simple" }
"""


def run_main(tmp_path, *extra):
    lock = tmp_path / "uv.lock"
    lock.write_text(LOCK, encoding="utf-8")
    output, table = tmp_path / "trace.json", tmp_path / "trace.md"
    releases = {
        ("asgiref", "3.12.1"): release(
            "3.12.1",
            "asgiref-3.12.1-py3-none-any.whl",
            classifiers=["Programming Language :: Python :: 3.14"],
        ),
        ("asgiref", ""): release(
            "3.13.0",
            "asgiref-3.13.0-py3-none-any.whl",
            classifiers=["Programming Language :: Python :: 3.15"],
            uploaded="2026-10-23T08:00:00Z",
        ),
        ("ready", "1.0"): release("1.0", "ready-1.0-py3-none-any.whl"),
        ("ready", ""): release("1.0", "ready-1.0-py3-none-any.whl"),
    }

    def fake_fetch(name, version=""):
        if (name, version) not in releases:
            raise URLError("not found")
        return releases[name, version]

    with mock.patch.object(trace.audit_wheels, "fetch_release", side_effect=fake_fetch):
        exit_code = trace.main(
            [
                "--lock",
                str(lock),
                "--python-version",
                PYTHON,
                "--output",
                str(output),
                "--markdown",
                str(table),
                *extra,
            ]
        )
    return exit_code, json.loads(output.read_text()), table.read_text()


def test_main_writes_records_for_both_platforms_and_tables_only_gaps(tmp_path):
    exit_code, data, table = run_main(tmp_path, "--cooldown-days", "4", "--as-of", "2026-10-25")

    assert exit_code == 1
    assert {(record["name"], record["platform"]) for record in data["packages"]} == {
        (name, platform) for name in ("asgiref", "missing", "ready") for platform in ("amd64", "arm64")
    }
    assert "| asgiref | 3.12.1 | direct | amd64, arm64 |" in table
    assert "newer release declares target (3.13.0) (inside cooldown)" in table
    assert "| missing | 1.0 | direct | amd64, arm64 |" in table
    assert "lookup error" in table
    assert "| ready |" not in table


def test_main_package_filter_tables_every_requested_package_and_names_unreached_ones(tmp_path):
    exit_code, data, table = run_main(tmp_path, "--package", "Ready", "--package", "absent")

    assert exit_code == 0
    assert {record["name"] for record in data["packages"]} == {"ready"}
    assert data["unreached"] == ["absent"]
    assert "| ready | 1.0 | direct | amd64, arm64 | wheel, unknown |" in table
    assert "| ok |" in table


@pytest.mark.parametrize(
    "arguments",
    [
        ["--python-version", "3.15"],
        ["--python-version", PYTHON, "--cooldown-days", "4"],
        ["--python-version", PYTHON, "--as-of", "2026-10-25"],
    ],
)
def test_main_rejects_invalid_arguments(tmp_path, arguments):
    with pytest.raises(SystemExit):
        trace.main(["--lock", str(tmp_path / "uv.lock"), "--output", str(tmp_path / "out.json"), *arguments])
