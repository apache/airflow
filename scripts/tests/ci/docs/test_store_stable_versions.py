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

import yaml
from ci.docs.store_stable_versions import (
    get_airflow_version,
    get_helm_chart_version,
    get_package_version,
    get_version_from_init_py,
    get_version_from_provider_yaml,
    main,
)


class TestGetAirflowVersion:
    def test_airflow_3x_location(self, tmp_path):
        init_file = tmp_path / "airflow-core" / "src" / "airflow" / "__init__.py"
        init_file.parent.mkdir(parents=True)
        init_file.write_text('__version__ = "3.2.0"\n')

        assert get_airflow_version(tmp_path) == "3.2.0"

    def test_airflow_2x_fallback(self, tmp_path):
        init_file = tmp_path / "airflow" / "__init__.py"
        init_file.parent.mkdir(parents=True)
        init_file.write_text('__version__ = "2.10.1"\n')

        assert get_airflow_version(tmp_path) == "2.10.1"

    def test_prefers_3x_over_2x(self, tmp_path):
        init_3x = tmp_path / "airflow-core" / "src" / "airflow" / "__init__.py"
        init_3x.parent.mkdir(parents=True)
        init_3x.write_text('__version__ = "3.0.0"\n')

        init_2x = tmp_path / "airflow" / "__init__.py"
        init_2x.parent.mkdir(parents=True)
        init_2x.write_text('__version__ = "2.0.0"\n')

        assert get_airflow_version(tmp_path) == "3.0.0"

    def test_missing_file(self, tmp_path):
        assert get_airflow_version(tmp_path) is None

    def test_no_version_in_file(self, tmp_path):
        init_file = tmp_path / "airflow-core" / "src" / "airflow" / "__init__.py"
        init_file.parent.mkdir(parents=True)
        init_file.write_text("# no version here\n")

        assert get_airflow_version(tmp_path) is None


class TestGetVersionFromProviderYaml:
    def test_reads_first_version(self, tmp_path):
        provider_yaml = tmp_path / "provider.yaml"
        provider_yaml.write_text(yaml.dump({"versions": ["2.3.0", "2.2.0", "2.1.0"]}))

        assert get_version_from_provider_yaml(provider_yaml) == "2.3.0"

    def test_missing_file(self, tmp_path):
        assert get_version_from_provider_yaml(tmp_path / "provider.yaml") is None

    def test_empty_versions_list(self, tmp_path):
        provider_yaml = tmp_path / "provider.yaml"
        provider_yaml.write_text(yaml.dump({"versions": []}))

        assert get_version_from_provider_yaml(provider_yaml) is None

    def test_no_versions_key(self, tmp_path):
        provider_yaml = tmp_path / "provider.yaml"
        provider_yaml.write_text(yaml.dump({"name": "some-provider"}))

        assert get_version_from_provider_yaml(provider_yaml) is None

    def test_numeric_version_converted_to_string(self, tmp_path):
        provider_yaml = tmp_path / "provider.yaml"
        # YAML will parse "1.0" as float 1.0
        provider_yaml.write_text("versions:\n  - 1.0\n")

        assert get_version_from_provider_yaml(provider_yaml) == "1.0"


class TestGetHelmChartVersion:
    def test_reads_version(self, tmp_path):
        chart_yaml = tmp_path / "Chart.yaml"
        chart_yaml.write_text("version: 1.16.0\nappVersion: 3.2.0\n")

        assert get_helm_chart_version(chart_yaml) == "1.16.0"

    def test_missing_file(self, tmp_path):
        assert get_helm_chart_version(tmp_path / "Chart.yaml") is None

    def test_no_version_field(self, tmp_path):
        chart_yaml = tmp_path / "Chart.yaml"
        chart_yaml.write_text("name: airflow\n")

        assert get_helm_chart_version(chart_yaml) is None


class TestGetVersionFromInitPy:
    def test_reads_version_double_quotes(self, tmp_path):
        init_py = tmp_path / "__init__.py"
        init_py.write_text('__version__ = "0.1.3"\n')

        assert get_version_from_init_py(init_py) == "0.1.3"

    def test_reads_version_single_quotes(self, tmp_path):
        init_py = tmp_path / "__init__.py"
        init_py.write_text("__version__ = '1.2.0'\n")

        assert get_version_from_init_py(init_py) == "1.2.0"

    def test_missing_file(self, tmp_path):
        assert get_version_from_init_py(tmp_path / "__init__.py") is None

    def test_no_version_in_file(self, tmp_path):
        init_py = tmp_path / "__init__.py"
        init_py.write_text("# just a comment\n")

        assert get_version_from_init_py(init_py) is None

    def test_version_with_extra_content(self, tmp_path):
        init_py = tmp_path / "__init__.py"
        init_py.write_text('"""Module docstring."""\n\n__version__ = "1.0.0"\n\nsome_var = 42\n')

        assert get_version_from_init_py(init_py) == "1.0.0"


class TestGetPackageVersion:
    def test_apache_airflow(self, tmp_path):
        init_file = tmp_path / "airflow-core" / "src" / "airflow" / "__init__.py"
        init_file.parent.mkdir(parents=True)
        init_file.write_text('__version__ = "3.2.0"\n')

        assert get_package_version("apache-airflow", tmp_path) == "3.2.0"

    def test_apache_airflow_ctl(self, tmp_path):
        init_file = tmp_path / "airflow-ctl" / "src" / "airflowctl" / "__init__.py"
        init_file.parent.mkdir(parents=True)
        init_file.write_text('__version__ = "0.1.3"\n')

        assert get_package_version("apache-airflow-ctl", tmp_path) == "0.1.3"

    def test_task_sdk(self, tmp_path):
        init_file = tmp_path / "task-sdk" / "src" / "airflow" / "sdk" / "__init__.py"
        init_file.parent.mkdir(parents=True)
        init_file.write_text('__version__ = "1.2.0"\n')

        assert get_package_version("task-sdk", tmp_path) == "1.2.0"

    def test_helm_chart(self, tmp_path):
        chart_yaml = tmp_path / "chart" / "Chart.yaml"
        chart_yaml.parent.mkdir(parents=True)
        chart_yaml.write_text("version: 1.16.0\n")

        assert get_package_version("helm-chart", tmp_path) == "1.16.0"

    def test_provider_3x_location(self, tmp_path):
        provider_yaml = tmp_path / "providers" / "google" / "provider.yaml"
        provider_yaml.parent.mkdir(parents=True)
        provider_yaml.write_text(yaml.dump({"versions": ["10.5.0"]}))

        assert get_package_version("apache-airflow-providers-google", tmp_path) == "10.5.0"

    def test_provider_2x_fallback(self, tmp_path):
        provider_yaml = tmp_path / "airflow" / "providers" / "amazon" / "provider.yaml"
        provider_yaml.parent.mkdir(parents=True)
        provider_yaml.write_text(yaml.dump({"versions": ["9.1.0"]}))

        assert get_package_version("apache-airflow-providers-amazon", tmp_path) == "9.1.0"

    def test_provider_with_hyphen_in_name(self, tmp_path):
        provider_yaml = tmp_path / "providers" / "apache" / "hive" / "provider.yaml"
        provider_yaml.parent.mkdir(parents=True)
        provider_yaml.write_text(yaml.dump({"versions": ["8.2.0"]}))

        assert get_package_version("apache-airflow-providers-apache-hive", tmp_path) == "8.2.0"

    def test_unknown_package(self, tmp_path):
        assert get_package_version("unknown-package", tmp_path) is None

    def test_missing_ctl_init_py(self, tmp_path):
        assert get_package_version("apache-airflow-ctl", tmp_path) is None

    def test_missing_task_sdk_init_py(self, tmp_path):
        assert get_package_version("task-sdk", tmp_path) is None


class TestMain:
    def _create_fake_airflow_root(self, tmp_path):
        """Create a minimal fake airflow root with version files."""
        airflow_root = tmp_path / "airflow_root"
        # apache-airflow version
        init_file = airflow_root / "airflow-core" / "src" / "airflow" / "__init__.py"
        init_file.parent.mkdir(parents=True)
        init_file.write_text('__version__ = "3.2.0"\n')
        # airflow-ctl version
        ctl_init = airflow_root / "airflow-ctl" / "src" / "airflowctl" / "__init__.py"
        ctl_init.parent.mkdir(parents=True)
        ctl_init.write_text('__version__ = "0.1.3"\n')
        return airflow_root

    def _create_docs_build_dir(self, tmp_path, packages):
        """Create a fake docs build dir with stable subdirs for given package names."""
        docs_dir = tmp_path / "docs_build"
        for pkg in packages:
            stable_dir = docs_dir / pkg / "stable"
            stable_dir.mkdir(parents=True)
            (stable_dir / "index.html").write_text("<html></html>")
        return docs_dir

    def test_creates_stable_txt(self, tmp_path, monkeypatch):
        airflow_root = self._create_fake_airflow_root(tmp_path)
        docs_dir = self._create_docs_build_dir(tmp_path, ["apache-airflow", "apache-airflow-ctl"])

        monkeypatch.setenv("DOCS_BUILD_DIR", str(docs_dir))
        monkeypatch.setenv("AIRFLOW_ROOT", str(airflow_root))

        result = main()

        assert result == 0
        assert (docs_dir / "apache-airflow" / "stable.txt").read_text() == "3.2.0\n"
        assert (docs_dir / "apache-airflow-ctl" / "stable.txt").read_text() == "0.1.3\n"

    def test_creates_versioned_directory(self, tmp_path, monkeypatch):
        airflow_root = self._create_fake_airflow_root(tmp_path)
        docs_dir = self._create_docs_build_dir(tmp_path, ["apache-airflow"])

        monkeypatch.setenv("DOCS_BUILD_DIR", str(docs_dir))
        monkeypatch.setenv("AIRFLOW_ROOT", str(airflow_root))

        main()

        version_dir = docs_dir / "apache-airflow" / "3.2.0"
        assert version_dir.is_dir()
        assert (version_dir / "index.html").exists()

    def test_skips_non_versioned_packages(self, tmp_path, monkeypatch):
        airflow_root = self._create_fake_airflow_root(tmp_path)
        docs_dir = self._create_docs_build_dir(tmp_path, ["apache-airflow-providers"])

        monkeypatch.setenv("DOCS_BUILD_DIR", str(docs_dir))
        monkeypatch.setenv("AIRFLOW_ROOT", str(airflow_root))

        result = main()

        assert result == 0
        assert not (docs_dir / "apache-airflow-providers" / "stable.txt").exists()

    def test_skips_packages_without_stable_dir(self, tmp_path, monkeypatch):
        airflow_root = self._create_fake_airflow_root(tmp_path)
        docs_dir = tmp_path / "docs_build"
        (docs_dir / "some-package").mkdir(parents=True)

        monkeypatch.setenv("DOCS_BUILD_DIR", str(docs_dir))
        monkeypatch.setenv("AIRFLOW_ROOT", str(airflow_root))

        result = main()

        assert result == 0
        assert not (docs_dir / "some-package" / "stable.txt").exists()

    def test_returns_1_when_no_docs_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DOCS_BUILD_DIR", str(tmp_path / "nonexistent"))
        monkeypatch.setenv("AIRFLOW_ROOT", str(tmp_path))

        assert main() == 1
