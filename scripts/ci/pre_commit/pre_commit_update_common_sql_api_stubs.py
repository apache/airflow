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

import difflib
import os
import shutil
import subprocess
import sys
import textwrap
from functools import lru_cache
from pathlib import Path

from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To execute this script, run ./{__file__} [FILE] ..."
    )

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()
PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"
COMMON_SQL_ROOT = PROVIDERS_ROOT / "common" / "sql"
OUT_DIR = AIRFLOW_SOURCES_ROOT / "out"

COMMON_SQL_PACKAGE_PREFIX = "airflow.providers.common.sql."

console = Console(width=400, color_system="standard")


@lru_cache(maxsize=None)
def black_mode():
    from black import Mode, parse_pyproject_toml, target_version_option_callback

    config = parse_pyproject_toml(os.path.join(AIRFLOW_SOURCES_ROOT, "pyproject.toml"))

    target_versions = set(
        target_version_option_callback(None, None, tuple(config.get("target_version", ()))),
    )

    return Mode(
        target_versions=target_versions,
        line_length=config.get("line_length", Mode.line_length),
        is_pyi=bool(config.get("is_pyi", Mode.is_pyi)),
        string_normalization=not bool(config.get("skip_string_normalization", not Mode.string_normalization)),
        experimental_string_processing=bool(
            config.get("experimental_string_processing", Mode.experimental_string_processing)
        ),
    )


def black_format(content) -> str:
    from black import format_str

    return format_str(content, mode=black_mode())


class ConsoleDiff(difflib.Differ):
    def _dump(self, tag, x, lo, hi):
        """Generate comparison results for a same-tagged range."""
        for i in range(lo, hi):
            if tag == "+":
                yield f"[green]{tag} {x[i]}[/]"
            elif tag == "-":
                yield f"[red]{tag} {x[i]}[/]"
            else:
                yield f"[white]{tag} {x[i]}[/]"


def summarize_changes(results: list[str]) -> tuple[int, int]:
    """
    Returns summary of the changes.

    :param results: results of comparison in the form of line of strings
    :return: Tuple: [number of removed lines, number of added lines]
    """
    removals, additions = 0, 0
    for line in results:
        if (
            line.startswith("+")
            or line.startswith("[green]+")
            and not (
                # Skip additions of comments in counting removals
                line.startswith("+#")
                or line.startswith("[green]+#")
            )
        ):
            additions += 1
        if (
            line.startswith("-")
            or line.startswith("[red]-")
            and not (
                # Skip removals of comments in counting removals
                line.startswith("-#")
                or line.startswith("[red]-#")
            )
        ):
            removals += 1
    return removals, additions


def post_process_historically_publicised_methods(stub_file_path: Path, line: str, new_lines: list[str]):
    if stub_file_path.relative_to(OUT_DIR) == Path("common") / "sql" / "operators" / "sql.pyi":
        if line.strip().startswith("parse_boolean: Incomplete"):
            # Handle Special case - historically we allow _parse_boolean to be part of the public API,
            # and we handle it via parse_boolean = _parse_boolean which produces Incomplete entry in the
            # stub - we replace the Incomplete method with both API methods that should be allowed
            #
            # We can remove those when we determine it is not risky for the community - when we determine
            # That most of the historically released providers have a way to easily update them, and they
            # are likely not used due to long time of the "min-airflow-version" presence. Note that
            # currently we have no "hard" criteria on when we can remove those - even if we bump common.sql
            # provider to 2.*, the old providers (mainly google providers) might still use them.
            new_lines.append("def _parse_boolean(val: str) -> str | bool: ...")
            new_lines.append("def parse_boolean(val: str) -> str | bool: ...")
        elif line.strip() == "class SQLExecuteQueryOperator(BaseSQLOperator):":
            # The "_raise_exception" method is really part of the public API and should not be removed
            new_lines.append(line)
            new_lines.append("    def _raise_exception(self, exception_string: str) -> Incomplete: ...")
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)


def post_process_generated_stub_file(stub_file_path: Path, lines: list[str], patch_historical_methods=False):
    """
    Post process the stub file - add the preamble and optionally patch historical methods.
    Adding preamble always, makes sure that we can update the preamble and have it automatically updated
    in generated files even if no API specification changes.

    :param stub_file_path: path of the stub fil
    :param lines: lines that were read from the file (with stripped comments)
    :param patch_historical_methods:  whether we should patch historical methods
    :return: resulting lines of the file after post-processing
    """
    new_lines = PREAMBLE.splitlines()
    for line in lines:
        if patch_historical_methods:
            post_process_historically_publicised_methods(stub_file_path, line, new_lines)
        else:
            new_lines.append(line)
    return new_lines


def read_pyi_file_content(pyi_file_path: Path, patch_historical_methods=False) -> list[str] | None:
    """
    Reads stub file content with post-processing and optionally patching historical methods. The comments
    are stripped and preamble is always added. It makes sure that we can update the preamble
    and have it automatically updated in generated files even if no API specification changes.

    If None is returned, the file should be deleted.

    :param pyi_file_path:
    :param patch_historical_methods:
    :return: list of lines of post-processed content or None if the file should be deleted.
    """
    lines = [
        line
        for line in pyi_file_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    if (pyi_file_path.name == "__init__.pyi") and lines == []:
        console.print(f"[yellow]Skip {pyi_file_path} as it is an empty stub for __init__.py file")
        return None
    return post_process_generated_stub_file(
        pyi_file_path, lines, patch_historical_methods=patch_historical_methods
    )


def compare_stub_files(generated_stub_path: Path, force_override: bool) -> tuple[int, int]:
    """
    Compare generated with stored files and returns True in case some modifications are needed.
    :param generated_stub_path: path of the stub that has been generated
    :param force_override: whether to override the API stubs even if there are removals
    :return: True if some updates were detected
    """
    _removals, _additions = 0, 0
    rel_path = generated_stub_path.relative_to(OUT_DIR)
    target_path = PROVIDERS_ROOT / rel_path
    generated_pyi_content = read_pyi_file_content(generated_stub_path, patch_historical_methods=True)
    if generated_pyi_content is None:
        os.unlink(generated_stub_path)
        if target_path.exists():
            console.print(
                f"[red]The {target_path} file is missing in generated files: but we are deleting it because"
                " it is an empty __init__.pyi file."
            )
            if _force_override:
                console.print(
                    f"[yellow]The file {target_path} has been removed as changes are force-overridden"
                )
                os.unlink(target_path)
            return 1, 0
        else:
            console.print(
                f"[blue]The {generated_stub_path} file is an empty __init__.pyi file, we just ignore it."
            )
            return 0, 0
    if not target_path.exists():
        console.print(f"[yellow]New file {target_path} has been missing. Treated as addition.")
        target_path.write_text("\n".join(generated_pyi_content), encoding="utf-8")
        return 0, 1
    target_pyi_content = read_pyi_file_content(target_path, patch_historical_methods=False)
    if target_pyi_content is None:
        target_pyi_content = []
    if generated_pyi_content != target_pyi_content:
        console.print(f"[yellow]The {target_path} has changed.")
        diff = ConsoleDiff()
        comparison_results = list(diff.compare(target_pyi_content, generated_pyi_content))
        _removals, _additions = summarize_changes(comparison_results)
        console.print(
            f"[bright_blue]Summary of the generated changes in common.sql stub API file {target_path}:[/]\n"
        )
        console.print(textwrap.indent("\n".join(comparison_results), " " * 4))
        if _removals == 0 or force_override:
            console.print(f"[yellow]The {target_path} has been updated\n")
            console.print(f"[yellow]* additions: {additions}[/]")
            console.print(f"[yellow]* removals: {removals}[/]")
            target_path.write_text("\n".join(generated_pyi_content), encoding="utf-8")
            console.print(
                f"\n[bright_blue]The {target_path} file has been updated automatically.[/]\n"
                "\n[yellow]Make sure to commit the changes.[/]"
            )
    else:
        if force_override:
            target_path.write_text("\n".join(generated_pyi_content), encoding="utf-8")
            console.print(
                f"\n[bright_blue]The {target_path} file has been updated automatically.[/]\n"
                "\n[yellow]Make sure to commit the changes.[/]"
            )
        else:
            console.print(f"[green]OK. The {target_path} has not changed.")
    return _removals, _additions


WHAT_TO_CHECK = """Those are the changes you should check:

    * for every removed method/class/function, check that is not used and that ALL ACTIVE PAST VERSIONS of
      released SQL providers do not use the method.

    * for every changed parameter of a method/function/class constructor, check that changes in the parameters
      are backwards compatible.
"""

PREAMBLE = """# Licensed to the Apache Software Foundation (ASF) under one
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
#
# This is automatically generated stub for the `common.sql` provider
#
# This file is generated automatically by the `update-common-sql-api stubs` pre-commit
# and the .pyi file represents part of the the "public" API that the
# `common.sql` provider exposes to other providers.
#
# Any, potentially breaking change in the stubs will require deliberate manual action from the contributor
# making a change to the `common.sql` provider. Those stubs are also used by MyPy automatically when checking
# if only public API of the common.sql provider is used by all the other providers.
#
# You can read more in the README_API.md file
#
"""


if __name__ == "__main__":
    shutil.rmtree(OUT_DIR, ignore_errors=True)

    subprocess.run(
        ["stubgen", *[os.fspath(path) for path in COMMON_SQL_ROOT.rglob("**/*.py")]], cwd=AIRFLOW_SOURCES_ROOT
    )
    removals, additions = 0, 0
    _force_override = os.environ.get("UPDATE_COMMON_SQL_API") == "1"
    if _force_override:
        console.print("\n[yellow]The committed stub APIs are force-updated\n")
    for stub_path in OUT_DIR.rglob("**/*.pyi"):
        stub_path.write_text(black_format(stub_path.read_text(encoding="utf-8")), encoding="utf-8")
    for stub_path in OUT_DIR.rglob("**/*.pyi"):
        _new_removals, _new_additions = compare_stub_files(stub_path, force_override=_force_override)
        removals += _new_removals
        additions += _new_additions
    for target_path in COMMON_SQL_ROOT.rglob("*.pyi"):
        generated_path = OUT_DIR / target_path.relative_to(PROVIDERS_ROOT)
        if not generated_path.exists():
            console.print(
                f"[red]The {target_path} file is missing in generated files:. "
                f"This is treated as breaking change."
            )
            removals += 1
            if _force_override:
                console.print(
                    f"[yellow]The file {target_path} has been removed as changes are force-overridden"
                )
                os.unlink(target_path)
    if not removals and not additions:
        console.print("\n[green]All OK. The common.sql APIs did not change[/]")
        sys.exit(0)
    if removals:
        if not _force_override:
            console.print(
                f"\n[red]ERROR! As you can see above, there are changes in the common.sql stub API files.\n"
                f"[red]* additions: {additions}[/]\n"
                f"[red]* removals: {removals}[/]\n"
            )
            console.print(
                "[bright_blue]Make sure to review the removals and changes for back-compatibility.[/]\n"
                "[bright_blue]If you are sure all the changes are justified, run:[/]"
            )
            console.print(
                "\n[magenta]UPDATE_COMMON_SQL_API=1 "
                "pre-commit run update-common-sql-api-stubs --all-files[/]\n"
            )
            console.print(WHAT_TO_CHECK)
            console.print("\n[yellow]Make sure to commit the changes after you update the API.[/]")
        else:
            console.print(
                f"\n[bright_blue]As you can see above, there are changes in the common.sql API:[/]\n\n"
                f"[bright_blue]* additions: {additions}[/]\n"
                f"[bright_blue]* removals: {removals}[/]\n"
            )
            console.print("[yellow]You've set UPDATE_COMMON_SQL_API to 1 to update the API.[/]\n\n")
            console.print("[yellow]So the files were updated automatically.")
    else:
        console.print(
            f"\n[yellow]There are only additions in the API extracted from the common.sql code[/]\n\n"
            f"[bright_blue]* additions: {additions}[/]\n"
        )
        console.print("[bright_blue]So the files were updated automatically.")
    sys.exit(1)
