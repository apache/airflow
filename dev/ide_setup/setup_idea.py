#!/usr/bin/env python3
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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
#   "packaging>=24.0",
# ]
# ///
from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path

from packaging.specifiers import SpecifierSet
from packaging.version import Version
from rich import print
from rich.prompt import Confirm

ROOT_AIRFLOW_FOLDER_PATH = Path(__file__).parents[2]
IDEA_FOLDER_PATH = ROOT_AIRFLOW_FOLDER_PATH / ".idea"
AIRFLOW_IML_FILE = IDEA_FOLDER_PATH / "airflow.iml"
MODULES_XML_FILE = IDEA_FOLDER_PATH / "modules.xml"
MISC_XML_FILE = IDEA_FOLDER_PATH / "misc.xml"
IDEA_NAME_FILE = IDEA_FOLDER_PATH / ".name"
BREEZE_PATH = ROOT_AIRFLOW_FOLDER_PATH / "dev" / "breeze"

STATIC_MODULES: list[str] = [
    "airflow-core",
    "airflow-ctl",
    "task-sdk",
    "devel-common",
    "dev",
    "dev/breeze",
    "docker-tests",
    "kubernetes-tests",
    "helm-tests",
    "scripts",
    "task-sdk-integration-tests",
]

# Well-known module groups for --exclude.
MODULE_GROUPS: dict[str, str] = {
    "providers": "providers/",
    "shared": "shared/",
    "dev": "dev",
    "tests": "tests",
}

source_root_module_pattern: str = '<sourceFolder url="file://$MODULE_DIR$/{path}" isTestSource="{status}" />'

# ---------------------------------------------------------------------------
# Exclude configuration
# ---------------------------------------------------------------------------

# Directories excluded by pattern (matched recursively against directory names in all content roots).
# Derived from .gitignore entries that can appear at any directory level.
# NOTE: "dist" is intentionally NOT here — providers/fab and providers/edge3 have legitimate
# static asset dist/ directories whitelisted in .gitignore.
EXCLUDE_PATTERNS: list[str] = [
    # Python bytecode / packaging
    "__pycache__",
    "*.egg-info",
    ".eggs",
    "build",
    "develop-eggs",
    "eggs",
    "sdist",
    "wheels",
    "downloads",
    "pip-wheel-metadata",
    # Test / coverage / lint caches
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".ruff-cache",
    ".hypothesis",
    ".cache",
    ".tox",
    "htmlcov",
    # Node / frontend
    "node_modules",
    ".vite",
    ".pnpm-store",
    # Generated documentation
    "_build",
    "_doctree",
    "_inventory_cache",
    "_api",
    # Virtualenvs (recursive — root .venv is also in ROOT_EXCLUDE_FOLDERS)
    "venv",
    # Infrastructure / IaC
    ".terraform",
    "target",
    # IDE / editor directories
    ".vscode",
    ".cursor",
    # Legacy / misc
    ".scrapy",
    ".ropeproject",
    ".spyderproject",
    ".webassets-cache",
    ".ipynb_checkpoints",
]

# Directories excluded by explicit path (relative to $MODULE_DIR$, i.e. the project root).
# Derived from root-anchored .gitignore entries (those starting with /).
ROOT_EXCLUDE_FOLDERS: list[str] = [
    ".build",
    ".kube",
    ".venv",
    ".uv-cache",
    "dist",
    "files",
    "logs",
    "out",
    "tmp",
    "images",
    "hive_scratch_dir",
    "airflow-core/dist",
    "airflow-core/src/airflow/ui/coverage",
    "generated",
    "docker-context-files",
    "target-airflow",
    "dev/breeze/.venv",
    "dev/registry/output",
    "dev/registry/logos",
    "3rd-party-licenses",
    "licenses",
    "registry/src/_data/versions",
]

# ---------------------------------------------------------------------------
# Python version helpers
# ---------------------------------------------------------------------------

# All minor versions we consider.  Keep the upper bound a step ahead of the
# latest CPython release so newly released interpreters are recognised.
_ALL_MINOR_VERSIONS = [f"3.{m}" for m in range(9, 16)]


def _read_requires_python(pyproject_path: Path) -> str:
    """Return the ``requires-python`` value from *pyproject_path*."""
    text = pyproject_path.read_text()
    match = re.search(r'requires-python\s*=\s*"([^"]+)"', text)
    if not match:
        print(f"[red]Error:[/] could not find requires-python in {pyproject_path}")
        sys.exit(1)
    return match.group(1)


def get_supported_python_versions(pyproject_path: Path) -> list[str]:
    """Return the list of supported ``X.Y`` Python versions according to *pyproject_path*."""
    spec = SpecifierSet(_read_requires_python(pyproject_path))
    return [v for v in _ALL_MINOR_VERSIONS if Version(f"{v}.0") in spec]


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------


def _build_exclude_patterns_xml(indent: str = "      ") -> str:
    """Build XML lines for <excludePattern> entries."""
    return "\n".join(f'{indent}<excludePattern pattern="{pattern}" />' for pattern in EXCLUDE_PATTERNS)


def _build_exclude_folders_xml(
    folders: list[str], indent: str = "      ", url_prefix: str = "file://$MODULE_DIR$"
) -> str:
    """Build XML lines for <excludeFolder> entries."""
    return "\n".join(f'{indent}<excludeFolder url="{url_prefix}/{folder}" />' for folder in folders)


def _build_content_xml(
    source_lines: str,
    *,
    include_root_excludes: bool,
    indent: str = "    ",
    url: str = "file://$MODULE_DIR$",
) -> str:
    """Build a complete <content> element with sources, exclude folders, and exclude patterns."""
    parts = [f'{indent}<content url="{url}">']
    if source_lines:
        parts.append(source_lines)
    if include_root_excludes:
        parts.append(_build_exclude_folders_xml(ROOT_EXCLUDE_FOLDERS, indent=f"{indent}  "))
    parts.append(_build_exclude_patterns_xml(indent=f"{indent}  "))
    parts.append(f"{indent}</content>")
    return "\n".join(parts)


# --- Templates ---

_iml_common_components = """\
  <component name="PyDocumentationSettings">
    <option name="format" value="PLAIN" />
    <option name="myDocStringFormat" value="Plain" />
  </component>
  <component name="TemplatesService">
    <option name="TEMPLATE_FOLDERS">
      <list>
        <option value="$MODULE_DIR$/chart/templates" />
      </list>
    </option>
  </component>
  <component name="TestRunnerService">
    <option name="PROJECT_TEST_RUNNER" value="py.test" />
  </component>"""

single_module_modules_xml_template = """\
<?xml version="1.0" encoding="UTF-8"?>
<project version="4">
  <component name="ProjectModuleManager">
    <modules>
      <module fileurl="file://$PROJECT_DIR$/.idea/airflow.iml" filepath="$PROJECT_DIR$/.idea/airflow.iml" />
    </modules>
  </component>
</project>"""

multi_module_modules_xml_template = """\
<?xml version="1.0" encoding="UTF-8"?>
<project version="4">
  <component name="ProjectModuleManager">
    <modules>
      <module fileurl="file://$PROJECT_DIR$/.idea/airflow.iml" filepath="$PROJECT_DIR$/.idea/airflow.iml" />
      {MODULE_ENTRIES}
    </modules>
  </component>
</project>"""

multi_module_entry_template = (
    '<module fileurl="file://$PROJECT_DIR$/{iml_path}" filepath="$PROJECT_DIR$/{iml_path}" />'
)


def _build_root_iml(sdk_name: str, source_lines: str = "") -> str:
    """Build a complete root .iml file (with project-level excludes and common components)."""
    content = _build_content_xml(source_lines, include_root_excludes=True, indent="    ")
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<module type="PYTHON_MODULE" version="4">\n'
        '  <component name="NewModuleRootManager">\n'
        f"{content}\n"
        f'    <orderEntry type="jdk" jdkName="{sdk_name}" jdkType="Python SDK" />\n'
        '    <orderEntry type="sourceFolder" forTests="false" />\n'
        "  </component>\n"
        f"{_iml_common_components}\n"
        "</module>"
    )


def _build_sub_module_iml(source_lines: str, *, sdk_name: str = "") -> str:
    """Build a sub-module .iml file.

    When *sdk_name* is provided the module gets its own explicit Python SDK;
    otherwise it inherits the project SDK.
    """
    content = _build_content_xml(source_lines, include_root_excludes=False, indent="    ")
    if sdk_name:
        jdk_entry = f'    <orderEntry type="jdk" jdkName="{sdk_name}" jdkType="Python SDK" />'
    else:
        jdk_entry = '    <orderEntry type="inheritedJdk" />'
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<module type="PYTHON_MODULE" version="4">\n'
        '  <component name="NewModuleRootManager">\n'
        f"{content}\n"
        f"{jdk_entry}\n"
        '    <orderEntry type="sourceFolder" forTests="false" />\n'
        "  </component>\n"
        '  <component name="TestRunnerService">\n'
        '    <option name="PROJECT_TEST_RUNNER" value="py.test" />\n'
        "  </component>\n"
        "</module>"
    )


misc_xml_template = """\
<?xml version="1.0" encoding="UTF-8"?>
<project version="4">
  <component name="ProjectRootManager" version="2" languageLevel="JDK_25" project-jdk-name="{SDK_NAME}" project-jdk-type="Python SDK" />
</project>"""

# ---------------------------------------------------------------------------
# uv sync / SDK detection
# ---------------------------------------------------------------------------


def run_uv_sync(project_dir: Path, label: str, *, python_version: str = ""):
    """Run ``uv sync`` in *project_dir* to create / update its .venv.

    When *python_version* is given (e.g. ``"3.12"``), ``--python <version>``
    is passed to ``uv sync`` so that the venv is created with that interpreter.
    """
    cmd: list[str] = ["uv", "sync"]
    if python_version:
        cmd += ["--python", python_version]
    version_info = f" (python {python_version})" if python_version else ""
    print(f"[cyan]Running uv sync in {label}{version_info} ...[/]")
    env = {k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"}
    result = subprocess.run(cmd, cwd=project_dir, env=env, check=False)
    if result.returncode != 0:
        print(f"[red]Error:[/] uv sync failed in {label}. Check the output above.")
        sys.exit(1)
    print(f"[green]uv sync completed in {label}.[/]\n")


def get_sdk_name(venv_dir: Path, *, label: str = "") -> str:
    """Return an IntelliJ SDK name for the venv in *venv_dir*.

    Uses the ``uv (<label>)`` naming convention, matching PyCharm's
    auto-detected uv interpreter names.  When *label* is not given the
    directory name is used (e.g. ``uv (airflow-clone)``).
    """
    venv_python = venv_dir / ".venv" / "bin" / "python"
    if not venv_python.exists():
        print(f"[red]Error:[/] {venv_python} not found even after uv sync.")
        sys.exit(1)
    if not label:
        label = venv_dir.name
    return f"uv ({label})"


# ---------------------------------------------------------------------------
# Global JetBrains SDK registration
# ---------------------------------------------------------------------------

# Product directory prefixes recognised when scanning for JetBrains config dirs.
_JETBRAINS_PRODUCT_PREFIXES = ("IntelliJIdea", "PyCharm", "IU", "PC")

# Prefixes that identify IntelliJ IDEA (Ultimate / Community via Toolbox "IU" code).
_INTELLIJ_PREFIXES = ("IntelliJIdea", "IU")
# Prefixes that identify PyCharm (Professional / Community via Toolbox "PC" code).
_PYCHARM_PREFIXES = ("PyCharm", "PC")


def _detect_installed_ides(idea_path: Path | None = None) -> tuple[bool, bool]:
    """Detect which JetBrains IDEs are installed.

    Returns a ``(has_intellij, has_pycharm)`` tuple.  When *idea_path* is
    given, only that directory is inspected.
    """
    if idea_path is not None:
        # If pointing at a specific product dir, check its name.
        name = idea_path.name
        has_intellij = any(name.startswith(p) for p in _INTELLIJ_PREFIXES)
        has_pycharm = any(name.startswith(p) for p in _PYCHARM_PREFIXES)
        if has_intellij or has_pycharm:
            return has_intellij, has_pycharm
        # Treat as base directory — scan children.
        if idea_path.exists():
            for child in idea_path.iterdir():
                if any(child.name.startswith(p) for p in _INTELLIJ_PREFIXES):
                    has_intellij = True
                if any(child.name.startswith(p) for p in _PYCHARM_PREFIXES):
                    has_pycharm = True
            return has_intellij, has_pycharm
        return False, False

    base = _find_jetbrains_config_base()
    if base is None or not base.exists():
        return False, False
    has_intellij = False
    has_pycharm = False
    for child in base.iterdir():
        if any(child.name.startswith(p) for p in _INTELLIJ_PREFIXES):
            has_intellij = True
        if any(child.name.startswith(p) for p in _PYCHARM_PREFIXES):
            has_pycharm = True
    return has_intellij, has_pycharm


# Process names used by JetBrains IDEs (matched case-insensitively against running processes).
_JETBRAINS_PROCESS_KEYWORDS = ("idea", "pycharm", "intellij")


def _find_jetbrains_pids() -> list[tuple[int, str]]:
    """Return a list of ``(pid, command)`` tuples for running JetBrains IDE processes.

    Excludes the current process and its parent to avoid the script killing itself
    (since ``setup_idea.py`` contains "idea" which matches the process keyword filter).
    """
    own_pids = {os.getpid(), os.getppid()}
    system = platform.system()
    if system == "Darwin":
        # On macOS, look for .app bundles in the process list.
        try:
            result = subprocess.run(
                ["ps", "-eo", "pid,comm"],
                capture_output=True,
                text=True,
                check=True,
            )
        except (FileNotFoundError, subprocess.CalledProcessError):
            return []
        pids: list[tuple[int, str]] = []
        for line in result.stdout.strip().splitlines()[1:]:
            parts = line.strip().split(None, 1)
            if len(parts) != 2:
                continue
            pid_str, comm = parts
            comm_lower = comm.lower()
            if any(kw in comm_lower for kw in _JETBRAINS_PROCESS_KEYWORDS):
                try:
                    pid = int(pid_str)
                    if pid not in own_pids:
                        pids.append((pid, comm))
                except ValueError:
                    pass
        return pids
    if system == "Linux":
        try:
            result = subprocess.run(
                ["ps", "-eo", "pid,args"],
                capture_output=True,
                text=True,
                check=True,
            )
        except (FileNotFoundError, subprocess.CalledProcessError):
            return []
        pids = []
        for line in result.stdout.strip().splitlines()[1:]:
            parts = line.strip().split(None, 1)
            if len(parts) != 2:
                continue
            pid_str, args = parts
            args_lower = args.lower()
            if any(kw in args_lower for kw in _JETBRAINS_PROCESS_KEYWORDS):
                try:
                    pid = int(pid_str)
                    if pid not in own_pids:
                        pids.append((pid, args))
                except ValueError:
                    pass
        return pids
    return []


def _kill_jetbrains_ides(*, confirm: bool = False) -> bool:
    """Attempt to gracefully terminate running JetBrains IDE processes.

    Sends SIGTERM first and waits briefly, then SIGKILL if processes remain.
    Returns *True* if processes were found (whether or not they were killed).
    """
    pids = _find_jetbrains_pids()
    if not pids:
        return False
    print("[yellow]Detected running JetBrains IDE process(es):[/]")
    for pid, comm in pids:
        print(f"  PID {pid}: {comm}")
    if not confirm:
        should_kill = Confirm.ask("\nKill these processes to proceed?")
        if not should_kill:
            return True
    for pid, _comm in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
    # Wait up to 5 seconds for graceful shutdown.
    for _ in range(10):
        remaining = _find_jetbrains_pids()
        if not remaining:
            print("[green]All JetBrains IDE processes terminated.[/]\n")
            return True
        time.sleep(0.5)
    # Force-kill any remaining processes.
    remaining = _find_jetbrains_pids()
    for pid, _comm in remaining:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
    print("[green]JetBrains IDE processes force-killed.[/]\n")
    return True


def _open_ide(*, project_dir: Path, idea_path: Path | None = None) -> None:
    """Open IntelliJ IDEA or PyCharm in *project_dir*.

    On macOS uses ``open -a``, on Linux uses the IDE's launcher script.
    Prefers IntelliJ IDEA over PyCharm when both are installed.
    """
    system = platform.system()
    has_intellij, has_pycharm = _detect_installed_ides(idea_path)
    if system == "Darwin":
        # Try to find the .app bundle via common Toolbox / standalone paths.
        app_name = None
        if has_intellij:
            app_name = "IntelliJ IDEA"
        elif has_pycharm:
            app_name = "PyCharm"
        if app_name:
            print(f"[cyan]Opening {app_name}...[/]")
            subprocess.Popen(["open", "-a", app_name, str(project_dir)])
            return
    elif system == "Linux":
        # JetBrains Toolbox symlinks launchers into ~/.local/share/JetBrains/Toolbox/scripts/
        toolbox_scripts = Path.home() / ".local" / "share" / "JetBrains" / "Toolbox" / "scripts"
        for cmd_name, is_match in [("idea", has_intellij), ("pycharm", has_pycharm)]:
            if not is_match:
                continue
            script = toolbox_scripts / cmd_name
            if script.exists():
                label = "IntelliJ IDEA" if cmd_name == "idea" else "PyCharm"
                print(f"[cyan]Opening {label}...[/]")
                subprocess.Popen([str(script), str(project_dir)])
                return
        # Fall back to shell commands on PATH.
        for cmd_name, is_match in [("idea", has_intellij), ("pycharm", has_pycharm)]:
            if not is_match:
                continue
            cmd_path = shutil.which(cmd_name)
            if cmd_path:
                label = "IntelliJ IDEA" if cmd_name == "idea" else "PyCharm"
                print(f"[cyan]Opening {label}...[/]")
                subprocess.Popen([cmd_path, str(project_dir)])
                return
    print(
        "[yellow]Could not find IntelliJ IDEA or PyCharm to open automatically.[/]\n"
        "[yellow]Please open the IDE manually in:[/] " + str(project_dir)
    )


def _find_jetbrains_config_base() -> Path | None:
    """Return the base JetBrains configuration directory for the current platform."""
    system = platform.system()
    if system == "Darwin":
        return Path.home() / "Library" / "Application Support" / "JetBrains"
    if system == "Linux":
        xdg = os.environ.get("XDG_CONFIG_HOME", "")
        return Path(xdg) / "JetBrains" if xdg else Path.home() / ".config" / "JetBrains"
    if system == "Windows":
        appdata = os.environ.get("APPDATA", "")
        return Path(appdata) / "JetBrains" if appdata else None
    return None


def _find_all_jdk_table_xmls(idea_path: Path | None = None) -> list[Path]:
    """Find all ``jdk.table.xml`` files across installed JetBrains IDEs.

    When *idea_path* is given, it is used as the JetBrains configuration base
    directory (or a specific product directory) instead of auto-detecting.

    Returns paths sorted descending so the most recent version comes first.
    """
    if idea_path is not None:
        # Allow pointing directly at a product directory (e.g. IntelliJIdea2025.1)
        # or the parent JetBrains directory.
        direct = idea_path / "options" / "jdk.table.xml"
        if direct.exists():
            return [direct]
        # Treat as base directory — scan for product subdirectories.
        if idea_path.exists():
            candidates: list[Path] = []
            for prefix in _JETBRAINS_PRODUCT_PREFIXES:
                candidates.extend(idea_path.glob(f"{prefix}*"))
            result: list[Path] = []
            for config_dir in sorted(candidates, reverse=True):
                jdk_table = config_dir / "options" / "jdk.table.xml"
                if jdk_table.exists():
                    result.append(jdk_table)
            return result
        return []

    base = _find_jetbrains_config_base()
    if base is None or not base.exists():
        return []
    candidates = []
    for prefix in _JETBRAINS_PRODUCT_PREFIXES:
        candidates.extend(base.glob(f"{prefix}*"))
    result = []
    for config_dir in sorted(candidates, reverse=True):
        jdk_table = config_dir / "options" / "jdk.table.xml"
        if jdk_table.exists():
            result.append(jdk_table)
    return result


def _home_var_path(path: str) -> str:
    """Replace the user's home directory prefix with ``$USER_HOME$``."""
    home = str(Path.home())
    if path.startswith(home):
        return "$USER_HOME$" + path[len(home) :]
    return path


def _get_venv_python_paths(venv_python: Path) -> tuple[str, str, str]:
    """Return *(stdlib, lib_dynload, site_packages)* paths for *venv_python*."""
    result = subprocess.run(
        [
            str(venv_python),
            "-c",
            "import sysconfig, site; "
            "print(sysconfig.get_path('stdlib')); "
            "print(sysconfig.get_path('platstdlib')); "
            "print(site.getsitepackages()[0])",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    lines = result.stdout.strip().splitlines()
    stdlib = lines[0]
    lib_dynload = str(Path(lines[1]) / "lib-dynload")
    site_packages = lines[2]
    return stdlib, lib_dynload, site_packages


def _build_sdk_jdk_element(
    name: str,
    version_string: str,
    home_path: str,
    stdlib_path: str,
    lib_dynload_path: str,
    site_packages_path: str,
    working_dir: str,
    sdk_uuid: str | None = None,
) -> ET.Element:
    """Build an ``<jdk>`` XML element suitable for insertion into ``jdk.table.xml``."""
    if sdk_uuid is None:
        sdk_uuid = str(uuid.uuid4())

    jdk = ET.Element("jdk", version="2")
    ET.SubElement(jdk, "name", value=name)
    ET.SubElement(jdk, "type", value="Python SDK")
    ET.SubElement(jdk, "version", value=version_string)
    ET.SubElement(jdk, "homePath", value=_home_var_path(home_path))

    roots = ET.SubElement(jdk, "roots")

    # annotationsPath
    ann = ET.SubElement(roots, "annotationsPath")
    ET.SubElement(ann, "root", type="composite")

    # classPath — stdlib, lib-dynload, site-packages
    cp = ET.SubElement(roots, "classPath")
    cp_composite = ET.SubElement(cp, "root", type="composite")
    ET.SubElement(
        cp_composite,
        "root",
        url=f"file://{_home_var_path(stdlib_path)}",
        type="simple",
    )
    ET.SubElement(
        cp_composite,
        "root",
        url=f"file://{_home_var_path(lib_dynload_path)}",
        type="simple",
    )
    ET.SubElement(
        cp_composite,
        "root",
        url=f"file://{_home_var_path(site_packages_path)}",
        type="simple",
    )

    # javadocPath
    jd = ET.SubElement(roots, "javadocPath")
    ET.SubElement(jd, "root", type="composite")

    # sourcePath
    sp = ET.SubElement(roots, "sourcePath")
    ET.SubElement(sp, "root", type="composite")

    # additional — UV flavour metadata
    additional = ET.SubElement(
        jdk,
        "additional",
        SDK_UUID=sdk_uuid,
        IS_UV="true",
        UV_WORKING_DIR=_home_var_path(working_dir),
    )
    ET.SubElement(additional, "setting", name="FLAVOR_ID", value="UvSdkFlavor")
    ET.SubElement(additional, "setting", name="FLAVOR_DATA", value="{}")

    return jdk


def _register_sdk_in_file(
    jdk_table_path: Path,
    name: str,
    venv_python: Path,
    working_dir: Path,
) -> str:
    """Register or reuse an SDK in a single ``jdk.table.xml`` file.

    Returns a status string: ``"reused"``, ``"renamed"``, or ``"created"``.
    """
    home_path_var = _home_var_path(str(venv_python))

    tree = ET.parse(jdk_table_path)
    root = tree.getroot()
    component = root.find(".//component[@name='ProjectJdkTable']")
    if component is None:
        component = ET.SubElement(root, "component", name="ProjectJdkTable")

    # 1. Look for an existing SDK whose homePath matches the target venv python.
    #    If found, reuse it (preserving IntelliJ's stubs/typeshed entries) and
    #    rename it to the expected name if necessary.
    for jdk_elem in component.findall("jdk"):
        home_elem = jdk_elem.find("homePath")
        if home_elem is not None and home_elem.get("value") == home_path_var:
            name_elem = jdk_elem.find("name")
            current_name = name_elem.get("value") if name_elem is not None else ""
            if current_name == name:
                return "reused"
            # Rename the existing SDK to match the expected convention.
            if name_elem is not None:
                name_elem.set("value", name)
            ET.indent(tree, space="  ")
            tree.write(str(jdk_table_path), encoding="unicode", xml_declaration=False)
            return "renamed"

    # 2. No existing SDK matches — create a new bare-bones entry.
    #    IntelliJ will populate stubs/typeshed paths on first load.
    result = subprocess.run(
        [str(venv_python), "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    version_string = result.stdout.strip()
    stdlib, lib_dynload, site_packages = _get_venv_python_paths(venv_python)

    new_jdk = _build_sdk_jdk_element(
        name=name,
        version_string=version_string,
        home_path=str(venv_python),
        stdlib_path=stdlib,
        lib_dynload_path=lib_dynload,
        site_packages_path=site_packages,
        working_dir=str(working_dir),
    )
    component.append(new_jdk)

    ET.indent(tree, space="  ")
    tree.write(str(jdk_table_path), encoding="unicode", xml_declaration=False)
    return "created"


def register_sdk(name: str, venv_dir: Path, working_dir: Path, *, idea_path: Path | None = None) -> bool:
    """Register (or reuse) a Python SDK in all global ``jdk.table.xml`` files.

    If an SDK already exists whose ``homePath`` matches the target venv, it is
    reused as-is (preserving IntelliJ's classPath entries such as python_stubs
    and typeshed) and renamed to *name* if necessary.  A new bare-bones entry
    is only created when no matching SDK exists.

    When *idea_path* is given, only ``jdk.table.xml`` files under that path
    are considered instead of auto-detecting all installed JetBrains IDEs.

    Returns *True* if at least one ``jdk.table.xml`` was processed, *False* if
    none could be found (no JetBrains IDE installed).
    """
    jdk_table_paths = _find_all_jdk_table_xmls(idea_path=idea_path)
    if not jdk_table_paths:
        print(
            f"[yellow]Warning:[/] Could not find jdk.table.xml — "
            f"SDK [bold]{name}[/] not registered globally. "
            f"You can register it manually in PyCharm settings."
        )
        return False

    venv_python = venv_dir / ".venv" / "bin" / "python"

    for jdk_table_path in jdk_table_paths:
        status = _register_sdk_in_file(jdk_table_path, name, venv_python, working_dir)
        ide_version = jdk_table_path.parent.parent.name
        if status == "reused":
            print(f"[green]SDK already registered:[/] [bold]{name}[/] in {ide_version}")
        elif status == "renamed":
            print(f"[green]Renamed existing SDK to:[/] [bold]{name}[/] in {ide_version}")
        else:
            print(f"[green]Created SDK:[/] [bold]{name}[/] in {ide_version}")
    return True


# ---------------------------------------------------------------------------
# Module discovery
# ---------------------------------------------------------------------------

EXCLUDED_PREFIXES = ("out/", ".build/", ".claude/", "dist/", ".venv/", "generated/")


def discover_modules(*, exclude_modules: set[str] | None = None) -> list[str]:
    """Discover all modules (static + providers + shared) and return sorted list.

    *exclude_modules* is an optional set of module paths (e.g. ``{"dev/breeze"}``)
    or prefixes (e.g. ``{"providers/"}``) to skip.  Prefix entries end with ``/``
    and match any module whose path starts with that prefix.
    """
    exclude_modules = exclude_modules or set()

    def _is_excluded(module: str) -> bool:
        if module in exclude_modules:
            return True
        return any(module.startswith(prefix) for prefix in exclude_modules if prefix.endswith("/"))

    modules = list(STATIC_MODULES)
    for pyproject_toml_file in ROOT_AIRFLOW_FOLDER_PATH.rglob("providers/**/pyproject.toml"):
        relative_path = pyproject_toml_file.relative_to(ROOT_AIRFLOW_FOLDER_PATH).parent.as_posix()
        if any(relative_path.startswith(prefix) for prefix in EXCLUDED_PREFIXES):
            continue
        modules.append(relative_path)
    for pyproject_toml_file in ROOT_AIRFLOW_FOLDER_PATH.rglob("shared/*/pyproject.toml"):
        relative_path = pyproject_toml_file.relative_to(ROOT_AIRFLOW_FOLDER_PATH).parent.as_posix()
        if any(relative_path.startswith(prefix) for prefix in EXCLUDED_PREFIXES):
            continue
        modules.append(relative_path)
    modules.sort()
    if exclude_modules:
        before = len(modules)
        modules = [m for m in modules if not _is_excluded(m)]
        skipped = before - len(modules)
        if skipped:
            print(f"[yellow]Excluded {skipped} module(s)[/]")
    return modules


def get_module_name(module_path: str) -> str:
    """Convert a module path to an IntelliJ module name (e.g. providers/amazon -> providers-amazon)."""
    return module_path.replace("/", "-")


# ---------------------------------------------------------------------------
# Cleanup of previously generated files
# ---------------------------------------------------------------------------

# Directories that are never scanned for leftover .iml files.
_CLEANUP_SKIP_DIRS = {".idea", ".claude", "node_modules", ".venv", ".git", ".build", "out", "dist"}


def _find_previous_iml_files() -> list[Path]:
    """Find ``.iml`` files created by a previous run of this script.

    Scans the project tree for ``*.iml`` files, skipping directories that
    are known to contain unrelated ``.iml`` files (e.g. ``node_modules``,
    ``.idea``).
    """
    results: list[Path] = []
    for iml_file in ROOT_AIRFLOW_FOLDER_PATH.rglob("*.iml"):
        rel = iml_file.relative_to(ROOT_AIRFLOW_FOLDER_PATH)
        if any(part in _CLEANUP_SKIP_DIRS for part in rel.parts):
            continue
        results.append(iml_file)
    return sorted(results)


def cleanup_previous_setup() -> None:
    """Remove files created by a previous run of this script.

    Deletes:
    * Sub-module ``.iml`` files scattered across the project tree.
    * The four ``.idea/`` files managed by the script: ``airflow.iml``,
      ``modules.xml``, ``misc.xml``, and ``.name``.

    Prints the number of files found and deleted.
    """
    managed_idea_files = [AIRFLOW_IML_FILE, MODULES_XML_FILE, MISC_XML_FILE, IDEA_NAME_FILE]
    previous_iml_files = _find_previous_iml_files()

    files_to_delete: list[Path] = []
    for f in managed_idea_files:
        if f.exists():
            files_to_delete.append(f)
    files_to_delete.extend(previous_iml_files)

    if not files_to_delete:
        print("[green]No files from a previous setup found.[/]\n")
        return

    print(f"[yellow]Found {len(files_to_delete)} file(s) from a previous setup — deleting:[/]")
    idea_count = sum(1 for f in files_to_delete if f.parent == IDEA_FOLDER_PATH)
    iml_count = len(files_to_delete) - idea_count
    if idea_count:
        print(f"  [dim]·[/] {idea_count} managed file(s) in .idea/")
    if iml_count:
        print(f"  [dim]·[/] {iml_count} sub-module .iml file(s)")
    for f in files_to_delete:
        f.unlink()
    print(f"[green]Deleted {len(files_to_delete)} file(s).[/]\n")


# ---------------------------------------------------------------------------
# Setup modes
# ---------------------------------------------------------------------------


def setup_idea_single_module(sdk_name: str, project_name: str, modules: list[str]):
    """Set up a single IntelliJ module with all source roots (original behaviour)."""
    all_module_paths: list[str] = []

    for module in modules:
        print(f"[green]Adding[/] source root: [blue]{module}[/]")
        if (ROOT_AIRFLOW_FOLDER_PATH / module / "src").exists():
            all_module_paths.append(source_root_module_pattern.format(path=f"{module}/src", status="false"))
        if (ROOT_AIRFLOW_FOLDER_PATH / module / "tests").exists():
            all_module_paths.append(source_root_module_pattern.format(path=f"{module}/tests", status="true"))
        if module == "dev":
            all_module_paths.append(source_root_module_pattern.format(path=module, status="false"))

    source_lines = "\n".join(f"      {line}" for line in all_module_paths)
    iml_content = _build_root_iml(sdk_name, source_lines=source_lines)

    IDEA_FOLDER_PATH.mkdir(exist_ok=True)
    AIRFLOW_IML_FILE.write_text(iml_content)
    MODULES_XML_FILE.write_text(single_module_modules_xml_template)
    MISC_XML_FILE.write_text(misc_xml_template.format(SDK_NAME=sdk_name))
    IDEA_NAME_FILE.write_text(f"{project_name}\n")

    print(f"\n[green]Updated:[/] {AIRFLOW_IML_FILE}")
    print(f"[green]Updated:[/] {MODULES_XML_FILE}")
    print(f"[green]Updated:[/] {MISC_XML_FILE}")
    print(f"[green]Updated:[/] {IDEA_NAME_FILE}")


def setup_idea_multi_module(sdk_name: str, project_name: str, breeze_sdk_name: str, modules: list[str]):
    """Set up multiple IntelliJ modules -- one per distribution/package."""
    module_entries: list[str] = []
    created_iml_files: list[Path] = []

    for module in modules:
        module_name = get_module_name(module)
        source_folders: list[str] = []

        if (ROOT_AIRFLOW_FOLDER_PATH / module / "src").exists():
            source_folders.append('<sourceFolder url="file://$MODULE_DIR$/src" isTestSource="false" />')
        if (ROOT_AIRFLOW_FOLDER_PATH / module / "tests").exists():
            source_folders.append('<sourceFolder url="file://$MODULE_DIR$/tests" isTestSource="true" />')
        if module == "dev":
            source_folders.append('<sourceFolder url="file://$MODULE_DIR$" isTestSource="false" />')

        if not source_folders:
            continue

        print(f"[green]Adding[/] module: [blue]{module_name}[/]")

        source_lines = "\n".join(f"      {line}" for line in source_folders)
        module_sdk = breeze_sdk_name if module == "dev/breeze" else ""
        iml_content = _build_sub_module_iml(source_lines, sdk_name=module_sdk)

        iml_path = ROOT_AIRFLOW_FOLDER_PATH / module / f"{module_name}.iml"
        iml_path.write_text(iml_content)
        created_iml_files.append(iml_path)

        relative_iml_path = f"{module}/{module_name}.iml"
        module_entries.append(multi_module_entry_template.format(iml_path=relative_iml_path))

    # Root module with excludes only
    root_iml_content = _build_root_iml(sdk_name)

    IDEA_FOLDER_PATH.mkdir(exist_ok=True)
    AIRFLOW_IML_FILE.write_text(root_iml_content)

    modules_xml_content = multi_module_modules_xml_template.format(
        MODULE_ENTRIES="\n      ".join(module_entries),
    )
    MODULES_XML_FILE.write_text(modules_xml_content)
    MISC_XML_FILE.write_text(misc_xml_template.format(SDK_NAME=sdk_name))
    IDEA_NAME_FILE.write_text(f"{project_name}\n")

    print(f"\n[green]Updated:[/] {AIRFLOW_IML_FILE} (root module)")
    print(f"[green]Updated:[/] {MODULES_XML_FILE}")
    print(f"[green]Updated:[/] {MISC_XML_FILE}")
    print(f"[green]Updated:[/] {IDEA_NAME_FILE}")
    print(f"[green]Created:[/] {len(created_iml_files)} sub-module .iml files")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Set up PyCharm/IntelliJ IDEA project configuration for Airflow."
    )
    module_group = parser.add_mutually_exclusive_group()
    module_group.add_argument(
        "--multi-module",
        action="store_true",
        default=None,
        help="Create separate IntelliJ modules for each distribution/package "
        "instead of a single module with multiple source roots. "
        "This is the default when IntelliJ IDEA is detected (multi-module "
        "is not supported by PyCharm).",
    )
    module_group.add_argument(
        "--single-module",
        action="store_true",
        default=None,
        help="Create a single IntelliJ module with all source roots. "
        "This is the default when only PyCharm is detected or when no "
        "IDE can be detected. Use this to override auto-detection.",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Automatically answer yes to all confirmation prompts (IDE close, "
        "process kill, file overwrite). Useful for non-interactive or scripted runs.",
    )
    parser.add_argument(
        "--open-ide",
        action="store_true",
        help="Open IntelliJ IDEA or PyCharm in the project directory after "
        "setup completes. Uses the detected IDE installation.",
    )
    parser.add_argument(
        "--no-kill",
        action="store_true",
        help="Do not attempt to detect and kill running PyCharm/IntelliJ IDEA "
        "processes. By default the script looks for running IDE processes, "
        "asks for confirmation, sends SIGTERM and falls back to SIGKILL if "
        "they don't exit within 5 seconds.",
    )
    parser.add_argument(
        "--idea-path",
        metavar="PATH",
        help="Path to the JetBrains configuration directory to update instead of "
        "auto-detecting all installed IDEs. Can point to the base JetBrains "
        "directory (e.g. '~/Library/Application Support/JetBrains') or a "
        "specific product directory (e.g. '.../JetBrains/IntelliJIdea2025.1').",
    )

    # --- Python version ---
    parser.add_argument(
        "--python",
        metavar="VERSION",
        help="Python minor version to use for the venv, e.g. '3.12'. "
        "Passed as --python to 'uv sync'. Must be compatible with the "
        "project's requires-python. By default uv picks the version.",
    )

    # --- Module exclusion ---
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="MODULE_OR_GROUP",
        help="Exclude a module or module group from the project.  Can be "
        "specified multiple times.  A module is a path relative to the "
        "project root (e.g. 'providers/amazon', 'dev/breeze').  "
        f"Recognised groups: {', '.join(sorted(MODULE_GROUPS))} "
        "(e.g. '--exclude providers' excludes all providers).",
    )
    return parser


def _resolve_excludes(raw: list[str]) -> set[str]:
    """Expand group names and return a set of module paths / prefixes to exclude."""
    result: set[str] = set()
    for item in raw:
        if item in MODULE_GROUPS:
            prefix = MODULE_GROUPS[item]
            # Groups that map to a directory prefix get a trailing "/" so
            # the discover_modules prefix matching works; exact module
            # names (like "dev") are kept as-is.
            result.add(prefix if prefix.endswith("/") else prefix)
        else:
            result.add(item)
    return result


def _validate_python_version(version: str) -> None:
    """Validate that *version* is supported by the project's ``requires-python``."""
    supported = get_supported_python_versions(ROOT_AIRFLOW_FOLDER_PATH / "pyproject.toml")
    if version not in supported:
        print(
            f"[red]Error:[/] Python {version} is not compatible with the project's "
            f"requires-python constraint.\n"
            f"Supported versions: [bold]{', '.join(supported)}[/]"
        )
        sys.exit(1)


def main():
    parser = _build_parser()
    args = parser.parse_args()

    # --- Validate --python early ---
    python_version: str = args.python or ""
    if python_version:
        _validate_python_version(python_version)

    # --- Resolve --idea-path ---
    idea_path: Path | None = Path(args.idea_path).expanduser() if args.idea_path else None

    # --- Resolve multi-module mode ---
    if args.multi_module:
        multi_module = True
    elif args.single_module:
        multi_module = False
    else:
        # Auto-detect based on installed IDE(s).
        has_intellij, has_pycharm = _detect_installed_ides(idea_path)
        if has_intellij:
            multi_module = True
            print("[cyan]Detected IntelliJ IDEA installation — defaulting to multi-module mode.[/]")
        elif has_pycharm:
            multi_module = False
            print(
                "[cyan]Detected PyCharm installation — "
                "defaulting to single-module mode "
                "(multi-module is not supported by PyCharm).[/]"
            )
        else:
            multi_module = False
            print("[cyan]No JetBrains IDE detected — defaulting to single-module mode.[/]")
        print("[dim]Use --multi-module or --single-module to override.[/]\n")

    # --- Show available versions on request ---
    supported = get_supported_python_versions(ROOT_AIRFLOW_FOLDER_PATH / "pyproject.toml")
    print(f"[cyan]Supported Python versions:[/] {', '.join(supported)}")

    # --- Kill or confirm IDE is closed ---
    if not args.no_kill:
        pids = _find_jetbrains_pids()
        if pids:
            _kill_jetbrains_ides(confirm=args.confirm)
        else:
            print("[green]No running IntelliJ IDEA / PyCharm processes detected — safe to proceed.[/]\n")
    elif not args.confirm:
        print(
            "\n[yellow]Warning:[/] PyCharm/IntelliJ IDEA must be closed before running this script, "
            "otherwise the IDE may overwrite the changes on exit.\n"
        )
        ide_closed = Confirm.ask("Have you closed PyCharm/IntelliJ IDEA?")
        if not ide_closed:
            print("[yellow]Please close PyCharm/IntelliJ IDEA and run this script again.[/]\n")
            return

    # --- uv sync ---
    run_uv_sync(ROOT_AIRFLOW_FOLDER_PATH, "project root", python_version=python_version)
    sdk_name = get_sdk_name(ROOT_AIRFLOW_FOLDER_PATH)
    project_name = f"[airflow]:{ROOT_AIRFLOW_FOLDER_PATH.name}"
    print(f"[cyan]Detected Python SDK:[/] [bold]{sdk_name}[/]")

    run_uv_sync(BREEZE_PATH, "dev/breeze", python_version=python_version)
    breeze_sdk_name = get_sdk_name(BREEZE_PATH, label=f"{ROOT_AIRFLOW_FOLDER_PATH.name}:breeze")
    print(f"[cyan]Detected Breeze SDK:[/] [bold]{breeze_sdk_name}[/]")

    # --- Module discovery ---
    exclude_set = _resolve_excludes(args.exclude)
    modules = discover_modules(exclude_modules=exclude_set)

    print(f"[cyan]Mode:[/] [bold]{'multi-module' if multi_module else 'single-module'}[/]")
    print(f"[cyan]Modules:[/] [bold]{len(modules)}[/]\n")

    files_to_update = [AIRFLOW_IML_FILE, MODULES_XML_FILE, MISC_XML_FILE, IDEA_NAME_FILE]
    print("[yellow]Warning!!![/] This script will update the PyCharm/IntelliJ IDEA configuration files:\n")
    for f in files_to_update:
        print(f"* {f}")
    if multi_module:
        print("* <module>/<module>.iml for each discovered module\n")
    else:
        print()

    previous_iml_files = _find_previous_iml_files()
    managed_to_delete = [f for f in files_to_update if f.exists()]
    if previous_iml_files or managed_to_delete:
        total = len(previous_iml_files) + len(managed_to_delete)
        print(
            f"[yellow]Note:[/] {total} file(s) from a previous setup will also be [bold]deleted[/] "
            "before writing the new configuration:"
        )
        if managed_to_delete:
            print(f"  [dim]·[/] {len(managed_to_delete)} managed file(s) in .idea/")
        if previous_iml_files:
            print(f"  [dim]·[/] {len(previous_iml_files)} sub-module .iml file(s)")
        print()

    if not args.confirm:
        should_continue = Confirm.ask("Overwrite the files?")
        if not should_continue:
            print("[yellow]Skipped\n")
            return

    print()
    cleanup_previous_setup()
    if multi_module:
        setup_idea_multi_module(sdk_name, project_name, breeze_sdk_name, modules)
    else:
        setup_idea_single_module(sdk_name, project_name, modules)

    # --- Register SDKs in global JetBrains configuration ---
    print()
    register_sdk(sdk_name, ROOT_AIRFLOW_FOLDER_PATH, ROOT_AIRFLOW_FOLDER_PATH, idea_path=idea_path)
    register_sdk(breeze_sdk_name, BREEZE_PATH, BREEZE_PATH, idea_path=idea_path)

    print("\n[green]Success[/]\n")
    if args.open_ide:
        _open_ide(project_dir=ROOT_AIRFLOW_FOLDER_PATH, idea_path=idea_path)
    else:
        print("[yellow]Important:[/] Restart PyCharm/IntelliJ IDEA to pick up the new configuration.\n")


if __name__ == "__main__":
    main()
