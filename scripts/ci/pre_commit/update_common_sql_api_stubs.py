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
from pathlib import Path

import jinja2
from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To execute this script, run ./{__file__} [FILE] ..."
    )

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported

from common_precommit_black_utils import black_format
from common_precommit_utils import AIRFLOW_SOURCES_ROOT_PATH

PROVIDERS_ROOT = (AIRFLOW_SOURCES_ROOT_PATH / "providers" / "src" / "airflow" / "providers").resolve(
    strict=True
)
COMMON_SQL_ROOT = (PROVIDERS_ROOT / "common" / "sql").resolve(strict=True)
OUT_DIR = AIRFLOW_SOURCES_ROOT_PATH / "out"
OUT_DIR_PROVIDERS = OUT_DIR / PROVIDERS_ROOT.relative_to(AIRFLOW_SOURCES_ROOT_PATH)

COMMON_SQL_PACKAGE_PREFIX = "airflow.providers.common.sql."

console = Console(width=400, color_system="standard")


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
        if line.startswith(("+", "[green]+")) and not line.startswith(("+#", "[green]+#")):
            # Skip additions of comments in counting removals
            additions += 1
        if line.startswith(("-", "[red]+")) and not line.startswith(("-#", "[red]+#")):
            # Skip removals of comments in counting removals
            removals += 1
    return removals, additions


def post_process_line(stub_file_path: Path, line: str, new_lines: list[str]) -> None:
    """
    Post process line of the stub file.

    Stubgen is not a perfect tool for generating stub files, but it is good starting point. We have to
    modify the stub files to make them more useful for us (as the approach of stubgen developers is not
    very open to add more options or features that are not very generic).

    The patching that we currently perform:
      * we add noqa to Incomplete imports from _typeshed (IntelliJ _typeshed does not like it)
      * we add historically published methods
      * fixes missing Union imports (see https://github.com/python/mypy/issues/12929)


    :param stub_file_path: path of the file we process
    :param line: line to post-process
    :param new_lines: new_lines - this is where we add post-processed lines
    """
    if stub_file_path.relative_to(OUT_DIR_PROVIDERS) == Path("common") / "sql" / "operators" / "sql.pyi":
        stripped_line = line.strip()
        if stripped_line.startswith("parse_boolean: Incomplete"):
            # Handle Special case - historically we allow _parse_boolean to be part of the public API,
            # and we handle it via parse_boolean = _parse_boolean which produces Incomplete entry in the
            # stub - we replace the Incomplete method with both API methods that should be allowed.
            # We also strip empty lines to let black figure out where to add them.
            #
            # We can remove those when we determine it is not risky for the community - when we determine
            # That most of the historically released providers have a way to easily update them, and they
            # are likely not used due to long time of the "min-airflow-version" presence. Note that
            # currently we have no "hard" criteria on when we can remove those - even if we bump common.sql
            # provider to 2.*, the old providers (mainly google providers) might still use them.
            new_lines.append("def _parse_boolean(val: str) -> str | bool: ...")
            new_lines.append("def parse_boolean(val: str) -> str | bool: ...")
        elif stripped_line == "class SQLExecuteQueryOperator(BaseSQLOperator):":
            # The "_raise_exception" method is really part of the public API and should not be removed
            new_lines.append(line)
            new_lines.append("    def _raise_exception(self, exception_string: str) -> Incomplete: ...")
        elif stripped_line == "":
            pass
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)


def post_process_generated_stub_file(
    module_name: str, stub_file_path: Path, lines: list[str], patch_generated_file=False
):
    """
    Post process the stub file:
    * adding (or replacing) preamble (makes sure we can replace preamble with new one in old files)
    * optionally patch the generated file

    :param module_name: name of the module of the file
    :param stub_file_path: path of the stub fil
    :param lines: lines that were read from the file (with stripped comments)
    :param patch_generated_file:  whether we should patch generated file
    :return: resulting lines of the file after post-processing
    """
    template = jinja2.Template(PREAMBLE)
    new_lines = template.render(module_name=module_name).splitlines()
    for line in lines:
        if patch_generated_file:
            post_process_line(stub_file_path, line, new_lines)
        else:
            new_lines.append(line)
    return new_lines


def write_pyi_file(pyi_file_path: Path, content: str) -> None:
    """
    Writes the content to the file.

    :param pyi_file_path: path of the file to write
    :param content: content to write (will be properly formatted)
    """
    pyi_file_path.write_text(black_format(content, is_pyi=True), encoding="utf-8")


def read_pyi_file_content(
    module_name: str, pyi_file_path: Path, patch_generated_files=False
) -> list[str] | None:
    """
    Reads stub file content with post-processing and optionally patching historical methods. The comments
    and initial javadoc are stripped and preamble is always added. It makes sure that we can update
    the preamble and have it automatically updated in generated files even if no API specification changes.

    If None is returned, the file should be deleted.

    :param module_name: name of the module in question
    :param pyi_file_path: the path of the file to read
    :param patch_generated_files: whether the historical methods should be patched
    :return: list of lines of post-processed content or None if the file should be deleted.
    """
    lines_no_comments = [
        line
        for line in pyi_file_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    remove_docstring = False
    lines = []
    for line in lines_no_comments:
        if line.strip().startswith('"""'):
            remove_docstring = not remove_docstring
        elif not remove_docstring:
            lines.append(line)
    if (pyi_file_path.name == "__init__.pyi") and lines == []:
        console.print(f"[yellow]Skip {pyi_file_path} as it is an empty stub for __init__.py file")
        return None
    return post_process_generated_stub_file(
        module_name, pyi_file_path, lines, patch_generated_file=patch_generated_files
    )


def compare_stub_files(generated_stub_path: Path, force_override: bool) -> tuple[int, int]:
    """
    Compare generated with stored files and returns True in case some modifications are needed.
    :param generated_stub_path: path of the stub that has been generated
    :param force_override: whether to override the API stubs even if there are removals
    :return: True if some updates were detected
    """
    _removals, _additions = 0, 0
    rel_path = generated_stub_path.relative_to(OUT_DIR_PROVIDERS)
    stub_file_target_path = PROVIDERS_ROOT / rel_path
    if stub_file_target_path.name == "__init__.pyi":
        return _removals, _additions
    module_name = "airflow.providers." + os.fspath(rel_path.with_suffix("")).replace(os.path.sep, ".")
    generated_pyi_content = read_pyi_file_content(
        module_name, generated_stub_path, patch_generated_files=True
    )
    if generated_pyi_content is None:
        generated_stub_path.unlink()
        if stub_file_target_path.exists():
            console.print(
                f"[red]The {stub_file_target_path} file is missing in generated files: "
                "but we are deleting it because it is an empty __init__.pyi file."
            )
            if _force_override:
                console.print(
                    f"[yellow]The file {stub_file_target_path} has been removed "
                    "as changes are force-overridden"
                )
                stub_file_target_path.unlink()
            return 1, 0
        else:
            console.print(
                f"[blue]The {generated_stub_path} file is an empty __init__.pyi file, we just ignore it."
            )
            return 0, 0
    if not stub_file_target_path.exists():
        console.print(f"[yellow]New file {stub_file_target_path} has been missing. Treated as addition.")
        write_pyi_file(stub_file_target_path, "\n".join(generated_pyi_content) + "\n")
        return 0, 1
    target_pyi_content = read_pyi_file_content(
        module_name, stub_file_target_path, patch_generated_files=False
    )
    if target_pyi_content is None:
        target_pyi_content = []
    if generated_pyi_content != target_pyi_content:
        console.print(f"[yellow]The {stub_file_target_path} has changed.")
        diff = ConsoleDiff()
        comparison_results = list(diff.compare(target_pyi_content, generated_pyi_content))
        _removals, _additions = summarize_changes(comparison_results)
        console.print(
            "[bright_blue]Summary of the generated changes in common.sql "
            f"stub API file {stub_file_target_path}:[/]\n"
        )
        console.print(textwrap.indent("\n".join(comparison_results), " " * 4))
        if _removals == 0 or force_override:
            console.print(f"[yellow]The {stub_file_target_path} has been updated\n")
            console.print(f"[yellow]* additions: {total_additions}[/]")
            console.print(f"[yellow]* removals: {total_removals}[/]")
            write_pyi_file(stub_file_target_path, "\n".join(generated_pyi_content) + "\n")
            console.print(
                f"\n[bright_blue]The {stub_file_target_path} file has been updated automatically.[/]\n"
                "\n[yellow]Make sure to commit the changes.[/]"
            )
    else:
        if force_override:
            write_pyi_file(stub_file_target_path, "\n".join(generated_pyi_content) + "\n")
            console.print(
                f"\n[bright_blue]The {stub_file_target_path} file has been updated automatically.[/]\n"
                "\n[yellow]Make sure to commit the changes.[/]"
            )
        else:
            console.print(f"[green]OK. The {stub_file_target_path} has not changed.")
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
# and the .pyi file represents part of the "public" API that the
# `common.sql` provider exposes to other providers.
#
# Any, potentially breaking change in the stubs will require deliberate manual action from the contributor
# making a change to the `common.sql` provider. Those stubs are also used by MyPy automatically when checking
# if only public API of the common.sql provider is used by all the other providers.
#
# You can read more in the README_API.md file
#
\"\"\"
Definition of the public interface for {{ module_name }}
isort:skip_file
\"\"\"
"""


if __name__ == "__main__":
    shutil.rmtree(OUT_DIR, ignore_errors=True)

    subprocess.run(
        ["stubgen", f"--out={ OUT_DIR }", COMMON_SQL_ROOT],
        cwd=AIRFLOW_SOURCES_ROOT_PATH,
    )
    total_removals, total_additions = 0, 0
    _force_override = os.environ.get("UPDATE_COMMON_SQL_API") == "1"
    if _force_override:
        console.print("\n[yellow]The committed stub APIs are force-updated\n")
    # reformat the generated stubs first
    for stub_path in OUT_DIR_PROVIDERS.rglob("*.pyi"):
        write_pyi_file(stub_path, stub_path.read_text(encoding="utf-8"))
    for stub_path in OUT_DIR_PROVIDERS.rglob("*.pyi"):
        _new_removals, _new_additions = compare_stub_files(stub_path, force_override=_force_override)
        total_removals += _new_removals
        total_additions += _new_additions
    for target_path in COMMON_SQL_ROOT.rglob("*.pyi"):
        generated_path = OUT_DIR_PROVIDERS / target_path.relative_to(PROVIDERS_ROOT)
        if not generated_path.exists():
            console.print(
                f"[red]The {target_path} file is missing in generated files:. "
                f"This is treated as breaking change."
            )
            total_removals += 1
            if _force_override:
                console.print(
                    f"[yellow]The file {target_path} has been removed as changes are force-overridden"
                )
                target_path.unlink()
    if not total_removals and not total_additions:
        console.print("\n[green]All OK. The common.sql APIs did not change[/]")
        sys.exit(0)
    if total_removals:
        if not _force_override:
            console.print(
                f"\n[red]ERROR! As you can see above, there are changes in the common.sql stub API files.\n"
                f"[red]* additions: {total_additions}[/]\n"
                f"[red]* removals: {total_removals}[/]\n"
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
                f"[bright_blue]* additions: {total_additions}[/]\n"
                f"[bright_blue]* removals: {total_removals}[/]\n"
            )
            console.print("[yellow]You've set UPDATE_COMMON_SQL_API to 1 to update the API.[/]\n\n")
            console.print("[yellow]So the files were updated automatically.")
    else:
        console.print(
            f"\n[yellow]There are only additions in the API extracted from the common.sql code[/]\n\n"
            f"[bright_blue]* additions: {total_additions}[/]\n"
        )
        console.print("[bright_blue]So the files were updated automatically.")
    sys.exit(1)
