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

from airflow.api_fastapi.core_api.services.ui.connections import HookMetaService

from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker


class TestMockOptional:
    def test_mock_optional_is_callable(self):
        """MockOptional instances must be callable to satisfy WTForms validator checks."""
        validator = HookMetaService.MockOptional()
        assert callable(validator)

    def test_mock_optional_call_is_noop(self):
        """Calling MockOptional should be a no-op (returns None)."""
        validator = HookMetaService.MockOptional()
        result = validator(None, None)
        assert result is None


class TestHookMetaServiceFabWidgetSafety:
    @skip_if_force_lowest_dependencies_marker
    def test_hook_meta_data_does_not_patch_fab_widgets(self):
        import wtforms  # noqa: F401
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget as widget_before

        HookMetaService.hook_meta_data.cache_clear()
        HookMetaService.hook_meta_data()

        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget as widget_after

        assert widget_before is widget_after

    @skip_if_force_lowest_dependencies_marker
    def test_get_hooks_with_mocked_fab_skips_mocks_when_wtforms_loaded(self):
        import wtforms  # noqa: F401
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget as widget_before

        HookMetaService._get_hooks_with_mocked_fab()

        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget as widget_after

        assert widget_before is widget_after
