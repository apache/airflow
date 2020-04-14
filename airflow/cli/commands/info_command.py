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
"""Config sub-commands"""
import getpass
import locale
import os
import platform
import subprocess
import sys
import textwrap
from typing import Optional

from typing_extensions import Protocol

from airflow import configuration
from airflow.version import version as airflow_version


class Anonymizer(Protocol):
    """Anonymizer protocol."""

    def process_path(self, value):
        """Remove pii from paths"""


class NullAnonymizer(Anonymizer):
    """Do nothing."""

    def process_path(self, value):
        return value


class PiiAnonymizer(Anonymizer):
    """Remove personally identifiable info from path."""

    def __init__(self):
        home_path = os.path.expanduser("~")
        username = getpass.getuser()
        self._path_replacements = {home_path: "${HOME}", username: "${USER}"}

    def process_path(self, value):
        if not value:
            return value
        for src, target in self._path_replacements.items():
            value = value.replace(src, target)
        return value


class OperatingSystem:
    """Operating system"""

    WINDOWS = "Windows"
    LINUX = "Linux"
    MACOSX = "Mac OS"
    CYGWIN = "Cygwin"

    @staticmethod
    def get_current() -> Optional[str]:
        """Get current operating system"""
        if os.name == "nt":
            return OperatingSystem.WINDOWS
        elif "linux" in sys.platform:
            return OperatingSystem.LINUX
        elif "darwin" in sys.platform:
            return OperatingSystem.MACOSX
        elif "cygwin" in sys.platform:
            return OperatingSystem.CYGWIN
        return None


class Architecture:
    """Compute architecture"""

    X86_64 = "x86_64"
    X86 = "x86"
    PPC = "ppc"
    ARM = "arm"

    @staticmethod
    def get_current():
        """Get architecture"""
        return _MACHINE_TO_ARCHITECTURE.get(platform.machine().lower())


_MACHINE_TO_ARCHITECTURE = {
    "amd64": Architecture.X86_64,
    "x86_64": Architecture.X86_64,
    "i686-64": Architecture.X86_64,
    "i386": Architecture.X86,
    "i686": Architecture.X86,
    "x86": Architecture.X86,
    "ia64": Architecture.X86,  # Itanium is different x64 arch, treat it as the common x86.
    "powerpc": Architecture.PPC,
    "power macintosh": Architecture.PPC,
    "ppc64": Architecture.PPC,
    "armv6": Architecture.ARM,
    "armv6l": Architecture.ARM,
    "arm64": Architecture.ARM,
    "armv7": Architecture.ARM,
    "armv7l": Architecture.ARM,
}


class AirflowInfo:
    """All information related to Airflow, system and other."""

    def __init__(self, anonymizer: Anonymizer):
        self.airflow_version = airflow_version
        self.system = SystemInfo(anonymizer)
        self.tools = ToolsInfo(anonymizer)
        self.paths = PathsInfo(anonymizer)
        self.config = ConfigInfo(anonymizer)

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                Apache Airflow [{version}]

                {system}

                {tools}

                {paths}

                {config}
                """
            )
            .strip()
            .format(
                version=airflow_version,
                system=self.system,
                tools=self.tools,
                paths=self.paths,
                config=self.config,
            )
        )


class SystemInfo:
    """Basic system and python information"""

    def __init__(self, anonymizer: Anonymizer):
        self.operating_system = OperatingSystem.get_current()
        self.arch = Architecture.get_current()
        self.uname = platform.uname()
        self.locale = locale.getdefaultlocale()
        self.python_location = anonymizer.process_path(sys.executable)
        self.python_version = sys.version.replace("\n", " ")

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                Platform: [{os}, {arch}] {uname}
                Locale: {locale}
                Python Version: [{python_version}]
                Python Location: [{python_location}]
                """
            )
            .strip()
            .format(
                version=airflow_version,
                os=self.operating_system or "NOT AVAILABLE",
                arch=self.arch or "NOT AVAILABLE",
                uname=self.uname,
                locale=self.locale,
                python_version=self.python_version,
                python_location=self.python_location,
            )
        )


class PathsInfo:
    """Path informaation"""

    def __init__(self, anonymizer: Anonymizer):
        system_path = os.environ.get("PATH", "").split(os.pathsep)

        self.airflow_home = anonymizer.process_path(configuration.get_airflow_home())
        self.system_path = [anonymizer.process_path(p) for p in system_path]
        self.python_path = [anonymizer.process_path(p) for p in sys.path]
        self.airflow_on_path = any(
            os.path.exists(os.path.join(path_elem, "airflow")) for path_elem in system_path
        )

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                Airflow Home: [{airflow_home}]
                System PATH: [{system_path}]
                Python PATH: [{python_path}]
                airflow on PATH: {airflow_on_path}
                """
            )
            .strip()
            .format(
                airflow_home=self.airflow_home,
                system_path=os.pathsep.join(self.system_path),
                python_path=os.pathsep.join(self.python_path),
                airflow_on_path=self.airflow_on_path,
            )
        )


class ConfigInfo:
    """"Most critical config properties"""

    def __init__(self, anonymizer: Anonymizer):
        self.dags_folder = anonymizer.process_path(
            configuration.conf.get("core", "dags_folder", fallback="NOT AVAILABLE")
        )
        self.plugins_folder = anonymizer.process_path(
            configuration.conf.get("core", "plugins_folder", fallback="NOT AVAILABLE")
        )
        self.base_log_folder = anonymizer.process_path(
            configuration.conf.get("logging", "base_log_folder", fallback="NOT AVAILABLE")
        )
        self.executor = configuration.conf.get("core", "executor")

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                DAGS Folder: [{dags_folder}]
                Plugins Folder: [{plugins_folder}]
                Base Log Folder: [{base_log_folder}]
                Executor: [{executor}]
                """
            )
            .strip()
            .format(
                dags_folder=self.dags_folder,
                plugins_folder=self.plugins_folder,
                base_log_folder=self.base_log_folder,
                executor=self.executor,
            )
        )


class ToolsInfo:
    """The versions of the tools that Airflow uses"""

    def __init__(self, anonymize: Anonymizer):
        del anonymize  # Nothing to anonymize here.
        self.git_version = self._get_version(["git", "--version"])
        self.ssh_version = self._get_version(["ssh", "-V"])
        self.kubectl_version = self._get_version(["kubectl", "version", "--short=True", "--client=True"])
        self.gcloud_version = self._get_version(["gcloud", "version"], grep=b"Google Cloud SDK")
        self.cloud_sql_proxy_version = self._get_version(["cloud_sql_proxy", "--version"])
        self.mysql_version = self._get_version(["mysql", "--version"])
        self.sqlite3_version = self._get_version(["sqlite3", "--version"])
        self.psql_version = self._get_version(["psql", "--version"])

    def _get_version(self, cmd, grep=None):
        """Return tools version."""
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        except OSError:
            return "NOT AVAILABLE"
        stdoutdata, _ = proc.communicate()
        data = [f for f in stdoutdata.split(b"\n") if f]
        if grep:
            data = [line for line in data if grep in line]
        if len(data) != 1:
            return "NOT AVAILABLE"
        else:
            return data[0].decode()

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                git: [{git}]
                ssh: [{ssh}]
                kubectl: [{kubectl}]
                gcloud: [{gcloud}]
                cloud_sql_proxy: [{cloud_sql_proxy}]
                mysql: [{mysql}]
                sqlite3: [{sqlite3}]
                psql: [{psql}]
                """
            )
            .strip()
            .format(
                git=self.git_version,
                ssh=self.ssh_version,
                kubectl=self.kubectl_version,
                gcloud=self.gcloud_version,
                cloud_sql_proxy=self.cloud_sql_proxy_version,
                mysql=self.mysql_version,
                sqlite3=self.sqlite3_version,
                psql=self.psql_version,
            )
        )


def show_info(args):
    """
    Show information related to Airflow, system and other.
    """
    anonymizer = PiiAnonymizer() if args.anonymize else NullAnonymizer()
    print(AirflowInfo(anonymizer))
