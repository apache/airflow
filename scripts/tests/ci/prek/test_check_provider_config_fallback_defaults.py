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

import re
import textwrap
from pathlib import Path

import check_provider_config_fallback_defaults as hook
import pytest
import yaml
from check_provider_config_fallback_defaults import (
    FALLBACK_CFG_PATH,
    PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK,
    ProviderOption,
    find_drift,
    load_fallback_cfg,
    load_provider_config_options,
)
from common_prek_utils import get_all_provider_yaml_files

CELERY_YAML = Path("providers/celery/provider.yaml")
CNCF_YAML = Path("providers/cncf/kubernetes/provider.yaml")


def _write_cfg(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "provider_config_fallback_defaults.cfg"
    path.write_text(textwrap.dedent(content))
    return path


def _write_provider_yaml(tmp_path: Path, provider_id: str, config: dict) -> Path:
    path = tmp_path / "providers" / provider_id / "provider.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        yaml.dump(
            {"package-name": f"apache-airflow-providers-{provider_id}", "state": "ready", "config": config}
        )
    )
    return path


def _declared(provider_yaml: Path, **defaults: str | None) -> dict[tuple[str, str], ProviderOption]:
    """Build ``{("celery", option): ProviderOption}`` for the given ``option=default`` pairs."""
    return {
        ("celery", option): ProviderOption(default, provider_yaml) for option, default in defaults.items()
    }


class TestLoadFallbackCfg:
    def test_lower_cases_sections_and_options_and_keeps_empty_values(self, tmp_path):
        cfg_path = _write_cfg(
            tmp_path,
            """
            [Celery]
            Pool = prefork
            flower_url_prefix =

            [elasticsearch]
            log_id_template = {dag_id}-{task_id}-{run_id}-{map_index}-{try_number}
            """,
        )
        assert load_fallback_cfg(cfg_path) == {
            "celery": {"pool": "prefork", "flower_url_prefix": ""},
            "elasticsearch": {"log_id_template": "{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}"},
        }

    def test_percent_signs_are_not_interpolated(self, tmp_path):
        cfg_path = _write_cfg(tmp_path, "[logging]\nfmt = %(asctime)s\n")
        assert load_fallback_cfg(cfg_path) == {"logging": {"fmt": "%(asctime)s"}}


class TestLoadProviderConfigOptions:
    def test_collects_defaults_across_files(self, tmp_path):
        celery_yaml = _write_provider_yaml(
            tmp_path,
            "celery",
            {
                "celery": {"options": {"pool": {"default": "prefork"}, "result_backend": {"default": None}}},
                "celery_kubernetes_executor": {"options": {"kubernetes_queue": {"default": "kubernetes"}}},
            },
        )
        cncf_yaml = _write_provider_yaml(
            tmp_path,
            "cncf/kubernetes",
            {"kubernetes_executor": {"options": {"tcp_keep_idle": {"default": 120}}}},
        )
        assert load_provider_config_options([celery_yaml, cncf_yaml]) == {
            ("celery", "pool"): ProviderOption("prefork", celery_yaml),
            ("celery", "result_backend"): ProviderOption(None, celery_yaml),
            ("celery_kubernetes_executor", "kubernetes_queue"): ProviderOption("kubernetes", celery_yaml),
            ("kubernetes_executor", "tcp_keep_idle"): ProviderOption("120", cncf_yaml),
        }

    def test_provider_without_config_is_ignored(self, tmp_path):
        path = tmp_path / "provider.yaml"
        path.write_text(yaml.dump({"package-name": "apache-airflow-providers-sqlite"}))
        assert load_provider_config_options([path]) == {}


class TestFindDrift:
    def test_in_sync_cfg_has_no_errors(self):
        cfg = {"celery": {"pool": "prefork", "flower_url_prefix": ""}}
        assert find_drift(cfg, _declared(CELERY_YAML, pool="prefork", flower_url_prefix="")) == []

    def test_section_nobody_declares_is_reported_once(self):
        cfg = {"atlas": {"host": "", "port": "21000"}, "celery": {"pool": "prefork"}}
        errors = find_drift(cfg, _declared(CELERY_YAML, pool="prefork"))
        assert len(errors) == 1
        assert errors[0].startswith("[atlas]:")
        assert "remove the whole section" in errors[0]

    def test_option_nobody_declares_is_reported(self):
        cfg = {"celery": {"pool": "prefork", "worker_precheck": "False"}}
        errors = find_drift(cfg, _declared(CELERY_YAML, pool="prefork"))
        assert errors == [
            "[celery] worker_precheck: no provider.yaml declares this option any more - remove it"
        ]

    def test_different_default_is_reported_with_both_values(self):
        cfg = {"celery": {"worker_concurrency": "16"}}
        errors = find_drift(cfg, _declared(CELERY_YAML, worker_concurrency="32"))
        assert errors == [
            "[celery] worker_concurrency = '16': providers/celery/provider.yaml declares default '32'"
        ]

    def test_provider_declaring_no_default_is_reported(self):
        cfg = {"celery": {"result_backend": ""}}
        errors = find_drift(cfg, _declared(CELERY_YAML, result_backend=None))
        assert errors == [
            "[celery] result_backend = '': providers/celery/provider.yaml declares no default for it"
        ]

    def test_intentional_override_is_accepted(self):
        cfg = {"celery": {"celery_app_name": "old.module"}}
        declared = _declared(CELERY_YAML, celery_app_name="new.module")
        overrides = [("celery", "celery_app_name", "new.module", "old.module")]
        assert find_drift(cfg, declared, overrides) == []

    @pytest.mark.parametrize(
        ("provider_default", "cfg_value"),
        [
            pytest.param("renamed.module", "old.module", id="provider-default-changed"),
            pytest.param("new.module", "edited.module", id="cfg-value-changed"),
        ],
    )
    def test_stale_intentional_override_is_reported(self, provider_default, cfg_value):
        cfg = {"celery": {"celery_app_name": cfg_value}}
        declared = _declared(CELERY_YAML, celery_app_name=provider_default)
        overrides = [("celery", "celery_app_name", "new.module", "old.module")]
        errors = find_drift(cfg, declared, overrides)
        assert len(errors) == 1
        assert "intentional override is out of date" in errors[0]
        assert "PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK" in errors[0]

    def test_intentional_override_does_not_resurrect_removed_option(self):
        cfg = {"celery": {"celery_app_name": "old.module"}}
        overrides = [("celery", "celery_app_name", "new.module", "old.module")]
        errors = find_drift(cfg, _declared(CELERY_YAML, pool="prefork"), overrides)
        assert errors == [
            "[celery] celery_app_name: no provider.yaml declares this option any more - remove it"
        ]

    def test_all_problems_are_reported_together(self):
        cfg = {
            "atlas": {"host": ""},
            "celery": {"pool": "prefork", "worker_precheck": "False", "worker_concurrency": "16"},
        }
        declared = _declared(CELERY_YAML, pool="prefork", worker_concurrency="32")
        errors = find_drift(cfg, declared)
        assert [error.split(":")[0] for error in errors] == [
            "[atlas]",
            "[celery] worker_precheck",
            "[celery] worker_concurrency = '16'",
        ]


class TestMain:
    def _run_main(self, monkeypatch, tmp_path, cfg_content: str, config: dict) -> int:
        cfg_path = _write_cfg(tmp_path, cfg_content)
        provider_yaml = _write_provider_yaml(tmp_path, "celery", config)
        monkeypatch.setattr(hook, "FALLBACK_CFG_PATH", cfg_path)
        monkeypatch.setattr(hook, "get_all_provider_yaml_files", lambda: [provider_yaml])
        return hook.main()

    def test_returns_zero_when_in_sync(self, monkeypatch, tmp_path):
        config = {"celery": {"options": {"pool": {"default": "prefork"}}}}
        assert self._run_main(monkeypatch, tmp_path, "[celery]\npool = prefork\n", config) == 0

    def test_returns_one_on_drift(self, monkeypatch, tmp_path, capsys):
        config = {"celery": {"options": {"pool": {"default": "prefork"}}}}
        assert (
            self._run_main(
                monkeypatch, tmp_path, "[celery]\npool = prefork\nworker_precheck = False\n", config
            )
            == 1
        )
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", capsys.readouterr().out)
        assert "[celery] worker_precheck: no provider.yaml declares this option any more" in plain_output


class TestRepositoryState:
    def test_fallback_cfg_is_in_sync_with_provider_yaml_files(self):
        cfg = load_fallback_cfg(FALLBACK_CFG_PATH)
        provider_options = load_provider_config_options(get_all_provider_yaml_files())
        assert find_drift(cfg, provider_options) == []

    def test_intentional_overrides_match_tests_common(self):
        from tests_common.test_utils.config import (
            PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK as TESTS_COMMON_OVERRIDES,
        )

        assert sorted(PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK) == sorted(TESTS_COMMON_OVERRIDES)
