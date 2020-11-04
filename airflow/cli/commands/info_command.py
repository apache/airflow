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
import logging
import os
import platform
import subprocess
import sys
import textwrap
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

import requests
import tenacity
from typing_extensions import Protocol

from airflow import configuration
from airflow.version import version as airflow_version

log = logging.getLogger(__name__)


class Anonymizer(Protocol):
    """Anonymizer protocol."""

    def process_path(self, value):
        """Remove pii from paths"""

    def process_username(self, value):
        """Remove pii from username"""

    def process_url(self, value):
        """Remove pii from URL"""


class NullAnonymizer(Anonymizer):
    """Do nothing."""

    def _identity(self, value):
        return value

    process_path = process_username = process_url = _identity

    del _identity


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

    def process_username(self, value):
        if not value:
            return value
        return value[0] + "..." + value[-1]

    def process_url(self, value):
        if not value:
            return value

        url_parts = urlsplit(value)
        netloc = None
        if url_parts.netloc:
            # unpack
            userinfo = None
            host = None
            username = None
            password = None

            if "@" in url_parts.netloc:
                userinfo, _, host = url_parts.netloc.partition("@")
            else:
                host = url_parts.netloc
            if userinfo:
                if ":" in userinfo:
                    username, _, password = userinfo.partition(":")
                else:
                    username = userinfo

            # anonymize
            username = self.process_username(username) if username else None
            password = "PASSWORD" if password else None

            # pack
            if username and password and host:
                netloc = username + ":" + password + "@" + host
            elif username and host:
                netloc = username + "@" + host
            elif password and host:
                netloc = ":" + password + "@" + host
            elif host:
                netloc = host
            else:
                netloc = ""

        return urlunsplit((url_parts.scheme, netloc, url_parts.path, url_parts.query, url_parts.fragment))


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
                version=self.airflow_version,
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
                os=self.operating_system or "NOT AVAILABLE",
                arch=self.arch or "NOT AVAILABLE",
                uname=self.uname,
                locale=self.locale,
                python_version=self.python_version,
                python_location=self.python_location,
            )
        )


class PathsInfo:
    """Path information"""

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
                airflow on PATH: [{airflow_on_path}]
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
    """Most critical config properties"""

    def __init__(self, anonymizer: Anonymizer):
        self.executor = configuration.conf.get("core", "executor")
        self.sql_alchemy_conn = anonymizer.process_url(
            configuration.conf.get("core", "SQL_ALCHEMY_CONN", fallback="NOT AVAILABLE")
        )
        self.dags_folder = anonymizer.process_path(
            configuration.conf.get("core", "dags_folder", fallback="NOT AVAILABLE")
        )
        self.plugins_folder = anonymizer.process_path(
            configuration.conf.get("core", "plugins_folder", fallback="NOT AVAILABLE")
        )
        self.base_log_folder = anonymizer.process_path(
            configuration.conf.get("logging", "base_log_folder", fallback="NOT AVAILABLE")
        )
        self.remote_base_log_folder = anonymizer.process_path(
            configuration.conf.get("logging", "remote_base_log_folder", fallback="NOT AVAILABLE")
        )

    @property
    def task_logging_handler(self):
        """Returns task logging handler."""

        def get_fullname(o):
            module = o.__class__.__module__
            if module is None or module == str.__class__.__module__:
                return o.__class__.__name__  # Avoid reporting __builtin__
            else:
                return module + '.' + o.__class__.__name__

        try:
            handler_names = [get_fullname(handler) for handler in logging.getLogger('airflow.task').handlers]
            return ", ".join(handler_names)
        except Exception:  # noqa pylint: disable=broad-except
            return "NOT AVAILABLE"

    def __str__(self):
        return (
            textwrap.dedent(
                """\
                Executor: [{executor}]
                Task Logging Handlers: [{task_logging_handler}]
                SQL Alchemy Conn: [{sql_alchemy_conn}]
                DAGS Folder: [{dags_folder}]
                Plugins Folder: [{plugins_folder}]
                Base Log Folder: [{base_log_folder}]
                """
            )
            .strip()
            .format(
                executor=self.executor,
                task_logging_handler=self.task_logging_handler,
                sql_alchemy_conn=self.sql_alchemy_conn,
                dags_folder=self.dags_folder,
                plugins_folder=self.plugins_folder,
                base_log_folder=self.base_log_folder,
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


class FileIoException(Exception):
    """Raises when error happens in FileIo.io integration"""


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_exponential(multiplier=1, max=10),
    retry=tenacity.retry_if_exception_type(FileIoException),
    before=tenacity.before_log(log, logging.DEBUG),
    after=tenacity.after_log(log, logging.DEBUG),
)
def _upload_text_to_fileio(content):
    """Upload text file to File.io service and return lnk"""
    resp = requests.post("https://file.io", files={"file": ("airflow-report.txt", content)})
    if not resp.ok:
        raise FileIoException("Failed to send report to file.io service.")
    try:
        return resp.json()["link"]
    except ValueError as e:
        log.debug(e)
        raise FileIoException("Failed to send report to file.io service.")


def _send_report_to_fileio(info):
    print("Uploading report to file.io service.")
    try:
        link = _upload_text_to_fileio(str(info))
        print("Report uploaded.")
        print()
        print("Link:\t", link)
        print()
    except FileIoException as ex:
        print(str(ex))


def show_info(args):
    """Show information related to Airflow, system and other."""
    # Enforce anonymization, when file_io upload is tuned on.
    anonymizer = PiiAnonymizer() if args.anonymize or args.file_io else NullAnonymizer()
    info = AirflowInfo(anonymizer)
    if args.file_io:
        _send_report_to_fileio(info)
    else:
        print(info)
