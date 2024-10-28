#!/usr/bin/env python
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

import argparse
import functools
import json
import os
import shutil
from collections.abc import Iterator
from dataclasses import asdict, dataclass, fields
from itertools import groupby
from pathlib import Path
from typing import Any, Callable, Iterable
from uuid import NAMESPACE_OID, uuid5

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )


REQUIRED_FIELDS = (
    "category",
    "message",
    "node_id",
    "filename",
    "lineno",
    "group",
    "count",
    "when",
)
CONSOLE_SIZE = shutil.get_terminal_size((80, 20)).columns
# Use as prefix/suffix in report output
IMPORTANT_WARNING_SIGN = {
    "sqlalchemy.exc.RemovedIn20Warning": "!!!",
    "sqlalchemy.exc.MovedIn20Warning": "!!!",
    "sqlalchemy.exc.LegacyAPIWarning": "!!",
    "sqlalchemy.exc.SAWarning": "!!",
    "pydantic.warnings.PydanticDeprecatedSince20": "!!",
    "celery.exceptions.CPendingDeprecationWarning": "!!",
    "pytest.PytestWarning": "!!",
    "airflow.exceptions.RemovedInAirflow3Warning": "!",
    "airflow.exceptions.AirflowProviderDeprecationWarning": "!",
    "airflow.utils.context.AirflowContextDeprecationWarning": "!",
}
# Always print messages for these warning categories
ALWAYS_SHOW_WARNINGS = {
    "sqlalchemy.exc.RemovedIn20Warning",
    "sqlalchemy.exc.MovedIn20Warning",
    "sqlalchemy.exc.LegacyAPIWarning",
    "sqlalchemy.exc.SAWarning",
    "pytest.PytestWarning",
}


def warnings_filename(suffix: str) -> str:
    return f"warn-summary-{suffix}.txt"


WARNINGS_ALL = warnings_filename("all")
WARNINGS_BAD = warnings_filename("bad")


@functools.lru_cache(maxsize=None)
def _unique_key(*args: str | None) -> str:
    return str(uuid5(NAMESPACE_OID, "-".join(map(str, args))))


def sorted_groupby(it, grouping_key: Callable):
    """Helper for sort and group by."""
    for group, grouped_data in groupby(sorted(it, key=grouping_key), key=grouping_key):
        yield group, list(grouped_data)


def count_groups(
    records: Iterable, grouping_key: Callable, *, reverse=True, top: int = 0
) -> Iterator[tuple[Any, int]]:
    it = sorted_groupby(records, grouping_key)
    for ix, (group, r) in enumerate(
        sorted(it, key=lambda k: len(k[1]), reverse=reverse), start=1
    ):
        if top and top < ix:
            return
        yield group, len(r)


@dataclass
class CapturedWarnings:
    category: str
    message: str
    filename: str
    lineno: int
    when: str
    node_id: str | None

    @property
    def unique_warning(self) -> str:
        return _unique_key(self.category, self.message, self.filename, str(self.lineno))

    @property
    def unique_key(self) -> str:
        return _unique_key(
            self.node_id, self.category, self.message, self.filename, str(self.lineno)
        )

    @classmethod
    def from_dict(cls, d: dict) -> CapturedWarnings:
        fields_names = [f.name for f in fields(CapturedWarnings)]
        return cls(**{k: v for k, v in d.items() if k in fields_names})

    def output(self) -> str:
        return json.dumps(asdict(self))


def find_files(directory: Path, glob_pattern: str) -> Iterator[tuple[Path, str]]:
    print(
        f" Process directory {directory} with pattern {glob_pattern!r} ".center(
            CONSOLE_SIZE, "="
        )
    )
    directory = Path(directory)
    for filepath in directory.rglob(glob_pattern):
        yield from resolve_file(filepath, directory)


def resolve_file(
    filepath: Path, directory: Path | None = None
) -> Iterator[tuple[Path, str]]:
    if not filepath.is_file():
        raise SystemExit("Provided path {filepath} is not a file.")
    if directory:
        source_path = filepath.relative_to(directory).as_posix()
    else:
        source_path = filepath.as_posix()
    yield filepath, source_path


def merge_files(files: Iterator[tuple[Path, str]], output_directory: Path) -> Path:
    output_file = output_directory.joinpath(WARNINGS_ALL)
    output_bad = output_directory.joinpath(WARNINGS_BAD)

    records = bad_records = 0
    processed_files = 0
    with output_file.open(mode="w") as wfp, output_bad.open(mode="w") as badwfp:
        for filepath, source_filename in files:
            print(f"Process file: {filepath.as_posix()}")
            with open(filepath) as fp:
                for lineno, line in enumerate(fp, start=1):
                    if not (line := line.strip()):
                        continue
                    try:
                        record = json.loads(line)
                        if not isinstance(record, dict):
                            raise TypeError
                        elif not all(field in record for field in REQUIRED_FIELDS):
                            raise ValueError
                    except Exception:
                        bad_records += 1
                        dump = json.dumps(
                            {"source": source_filename, "lineno": lineno, "record": line}
                        )
                        badwfp.write(f"{dump}\n")
                    else:
                        records += 1
                        record["source"] = source_filename
                        wfp.write(f"{json.dumps(record)}\n")
                processed_files += 1

    print()
    print(
        f" Total processed lines {records + bad_records:,} in {processed_files:,} file(s) ".center(
            CONSOLE_SIZE, "-"
        )
    )
    print(f"Good Records: {records:,}. Saved into file {output_file.as_posix()}")
    if bad_records:
        print(f"Bad Records: {bad_records:,}. Saved into file {output_file.as_posix()}")
    else:
        output_bad.unlink()
    return output_file


def group_report_warnings(
    group, when: str, group_records, output_directory: Path
) -> None:
    output_filepath = output_directory / warnings_filename(f"{group}-{when}")

    group_warnings: dict[str, CapturedWarnings] = {}
    unique_group_warnings: dict[str, CapturedWarnings] = {}
    for record in group_records:
        cw = CapturedWarnings.from_dict(record)
        if cw.unique_key not in group_warnings:
            group_warnings[cw.unique_key] = cw
        if cw.unique_warning not in unique_group_warnings:
            unique_group_warnings[cw.unique_warning] = cw

    print(f" Group {group!r} on {when!r} ".center(CONSOLE_SIZE, "="))
    with output_filepath.open(mode="w") as fp:
        for cw in group_warnings.values():
            fp.write(f"{cw.output()}\n")
    print(f"Saved into file: {output_filepath.as_posix()}\n")

    if when == "runtest":  # Node id exists only during runtest
        print(f"Unique warnings within the test cases: {len(group_warnings):,}\n")
        print("Top 10 Tests Cases:")
        it = count_groups(
            group_warnings.values(),
            grouping_key=lambda cw: (cw.category, cw.node_id),
            top=10,
        )
        for (category, node_id), count in it:
            if suffix := IMPORTANT_WARNING_SIGN.get(category, ""):
                suffix = f" ({suffix})"
            print(f"  {category} {node_id} - {count:,}{suffix}")
        print()

    print(f"Unique warnings: {len(unique_group_warnings):,}\n")
    print("Warnings grouped by category:")
    for category, count in count_groups(
        unique_group_warnings.values(), grouping_key=lambda cw: cw.category
    ):
        if suffix := IMPORTANT_WARNING_SIGN.get(category, ""):
            suffix = f" ({suffix})"
        print(f"  {category} - {count:,}{suffix}")
    print()

    print("Top 10 Warnings:")
    it = count_groups(
        unique_group_warnings.values(),
        grouping_key=lambda cw: (cw.category, cw.filename, cw.lineno),
        top=10,
    )
    for (category, filename, lineno), count in it:
        if suffix := IMPORTANT_WARNING_SIGN.get(category, ""):
            suffix = f" ({suffix})"
        print(f"  {filename}:{lineno}:{category} - {count:,}{suffix}")
    print()

    always = list(
        filter(
            lambda w: w.category in ALWAYS_SHOW_WARNINGS, unique_group_warnings.values()
        )
    )
    if always:
        print(f" Always reported warnings {len(always):,}".center(CONSOLE_SIZE, "-"))
        for cw in always:
            print(f"{cw.filename}:{cw.lineno}")
            print(f"  {cw.category} - {cw.message}")
            print()


def split_by_groups(output_file: Path, output_directory: Path) -> None:
    records: list[dict] = []
    with output_file.open() as fp:
        records.extend(map(json.loads, fp))
    for (group, when), group_records in sorted_groupby(
        records, grouping_key=lambda record: (record["group"], record["when"])
    ):
        group_report_warnings(group, when, group_records, output_directory)


def main(_input: str, _output: str | None, pattern: str | None) -> int | str:
    cwd = Path(".").resolve()
    print(f"Current Working Directory: {cwd.as_posix()}")

    try:
        input_path = Path(os.path.expanduser(os.path.expandvars(_input))).resolve(
            strict=True
        )
    except OSError as ex:
        return f"Unable to resolve {_input!r} path. {type(ex).__name__}: {ex}"

    if not pattern:
        print(f" Process file {input_path} ".center(CONSOLE_SIZE, "="))
        if not input_path.is_file():
            return f"{input_path} is not a file."
        files = resolve_file(input_path, cwd if not input_path.is_absolute() else None)
    else:
        if not input_path.is_dir():
            return f"{input_path} is not a file."
        files = find_files(input_path, pattern)

    output_directory = Path(_output or cwd).resolve()
    output_directory.mkdir(parents=True, exist_ok=True)

    output_file = merge_files(files, output_directory)
    split_by_groups(output_file, output_directory)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Capture Warnings Summarizer")
    parser.add_argument("input", help="Input file/or directory path")
    parser.add_argument("-g", "--pattern", help="Glob pattern to filter warnings files")
    parser.add_argument("-o", "--output", help="Output directory")
    args = parser.parse_args()
    raise SystemExit(main(args.input, args.output, args.pattern))
