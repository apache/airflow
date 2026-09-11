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
"""Unit tests for dev/registry/extract_parameters.py."""

from __future__ import annotations

import abc
import builtins
import json
import types
from unittest.mock import patch

import pytest
import yaml
from extract_parameters import (
    Module,
    _get_source_line,
    _parse_requested_providers,
    _resolve_dotted_path,
    _should_skip_class,
    compare_with_ast,
    discover_classes_from_provider,
    get_category,
    is_durable_capable,
    load_resumable_job_mixin,
    supports_deferrable,
)


# ---------------------------------------------------------------------------
# get_category
# ---------------------------------------------------------------------------
class TestGetCategory:
    def test_simple_name(self):
        assert get_category("Amazon S3") == "amazon-s3"

    def test_parentheses_stripped(self):
        assert get_category("Google Cloud (GCS)") == "google-cloud-gcs"

    def test_special_chars_removed(self):
        assert get_category("Apache Kafka™") == "apache-kafka"

    def test_empty_string(self):
        assert get_category("") == ""


# ---------------------------------------------------------------------------
# _should_skip_class
# ---------------------------------------------------------------------------
class TestClassFiltering:
    def test_skips_private_classes(self):
        assert _should_skip_class("_InternalHelper") is True

    def test_skips_dunder_prefix(self):
        assert _should_skip_class("__Meta") is True

    def test_skips_base_prefix(self):
        assert _should_skip_class("BaseOperator") is True
        assert _should_skip_class("BaseSQLOperator") is True

    def test_skips_abstract_in_name(self):
        assert _should_skip_class("AbstractHook") is True
        assert _should_skip_class("MyAbstractSensor") is True

    def test_skips_mixin_in_name(self):
        assert _should_skip_class("AwsMixin") is True
        assert _should_skip_class("MixinHelper") is True

    def test_allows_normal_class(self):
        assert _should_skip_class("S3CopyObjectOperator") is False
        assert _should_skip_class("RedshiftHook") is False

    def test_allows_class_containing_base_not_at_start(self):
        assert _should_skip_class("FirebaseHook") is False

    def test_allows_class_with_database_in_name(self):
        # "Database" contains "base" but not at position 0
        assert _should_skip_class("DatabaseOperator") is False


# ---------------------------------------------------------------------------
# _get_source_line
# ---------------------------------------------------------------------------
class TestGetSourceLine:
    def test_returns_line_for_real_class(self):
        # _should_skip_class is a function in the same file, but any real class works
        line = _get_source_line(TestGetSourceLine)
        assert line is not None
        assert isinstance(line, int)
        assert line > 0

    def test_returns_none_for_dynamic_class(self):
        DynamicClass = type("DynamicClass", (), {})
        assert _get_source_line(DynamicClass) is None


# ---------------------------------------------------------------------------
# load_resumable_job_mixin
# ---------------------------------------------------------------------------
class TestLoadResumableJobMixin:
    def test_returns_none_when_airflow_sdk_unavailable(self):
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "airflow.sdk":
                raise ImportError("no airflow.sdk here")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            assert load_resumable_job_mixin() is None


# ---------------------------------------------------------------------------
# is_durable_capable
# ---------------------------------------------------------------------------
class FakeResumableJobMixin(abc.ABC):
    """Stand-in for airflow.sdk.ResumableJobMixin's abstract-method contract."""

    @abc.abstractmethod
    def submit_job(self, context):
        raise NotImplementedError

    @abc.abstractmethod
    def get_job_status(self, external_id, context):
        raise NotImplementedError

    @abc.abstractmethod
    def is_job_active(self, status):
        raise NotImplementedError

    @abc.abstractmethod
    def is_job_succeeded(self, status):
        raise NotImplementedError

    @abc.abstractmethod
    def poll_until_complete(self, external_id, context):
        raise NotImplementedError

    @abc.abstractmethod
    def get_job_result(self, external_id, context):
        raise NotImplementedError

    def execute_resumable(self, context):
        raise NotImplementedError


class FullyImplementedResumableOperator(FakeResumableJobMixin):
    def execute(self, context):
        return self.execute_resumable(context)

    def submit_job(self, context):
        return "job-1"

    def get_job_status(self, external_id, context):
        return "RUNNING"

    def is_job_active(self, status):
        return status == "RUNNING"

    def is_job_succeeded(self, status):
        return status == "SUCCEEDED"

    def poll_until_complete(self, external_id, context):
        return None

    def get_job_result(self, external_id, context):
        return None


class PartiallyImplementedResumableOperator(FakeResumableJobMixin):
    """Retry-path methods left unoverridden -- would only blow up on an actual crash-recovery retry."""

    def execute(self, context):
        return self.execute_resumable(context)

    def submit_job(self, context):
        return "job-1"

    def poll_until_complete(self, external_id, context):
        return None

    def get_job_result(self, external_id, context):
        return None


class UnwiredResumableOperator(FakeResumableJobMixin):
    """execute() never calls execute_resumable -- dead capability, never exercised."""

    def execute(self, context):
        return self.submit_job(context)

    def submit_job(self, context):
        return "job-1"

    def get_job_status(self, external_id, context):
        return "RUNNING"

    def is_job_active(self, status):
        return status == "RUNNING"

    def is_job_succeeded(self, status):
        return status == "SUCCEEDED"

    def poll_until_complete(self, external_id, context):
        return None

    def get_job_result(self, external_id, context):
        return None


class PlainOperator:
    def execute(self, context):
        return None


class ManuallyDurableOperator:
    """Implements durable execution directly (e.g. via task_state_store), without
    ResumableJobMixin -- mirrors KubernetesPodOperator/AgentOperator."""

    __supports_durable_execution = True

    def execute(self, context):
        return None


class ManuallyDurableSubclass(ManuallyDurableOperator):
    """Overrides execute() itself -- must NOT inherit the parent's declaration,
    since nothing here verifies it preserves the reconnect behavior."""

    def execute(self, context):
        return "something else entirely"


class ManuallyDurableSubclassNoOverride(ManuallyDurableOperator):
    """Adds no execute() override at all. Mirrors GKEStartPodOperator, which relies
    entirely on KubernetesPodOperator.execute(). Must still qualify."""

    def some_other_method(self):
        return None


class ManuallyDurableSubclassDelegating(ManuallyDurableOperator):
    """Overrides execute() but delegates back via super().execute(). Mirrors
    EksPodOperator/SparkKubernetesOperator's non-deferrable path. Must still qualify."""

    def execute(self, context):
        self.log_something()
        return super().execute(context)

    def log_something(self):
        return None


class ManuallyDurableSubclassDelegatingMultiHop(ManuallyDurableSubclassDelegating):
    """A second layer of super().execute() delegation on top of ManuallyDurableSubclassDelegating.
    Must still qualify."""

    def execute(self, context):
        return super().execute(context)


class DecoratorMixin:
    """Mirrors DecoratedOperator: a mixin whose own MRO has no relationship to the marker class."""

    def execute(self, context):
        return super().execute(context)


class DecoratedDurableSubclass(DecoratorMixin, ManuallyDurableOperator):
    """Combines DecoratorMixin with the marker class via multiple inheritance (e.g. @task.kubernetes)."""

    def execute(self, context):
        return super().execute(context)


class DecoratedDurableSubclassNoOwnExecute(DecoratorMixin, ManuallyDurableOperator):
    """Same combined MRO as above, but inherits DecoratorMixin's execute() unchanged."""


class ExplicitParentDelegatingSubclass(ManuallyDurableOperator):
    """Mirrors @task.agent's _AgentDecoratedOperator: names the parent class directly instead
    of using super(). Must still qualify."""

    def execute(self, context):
        return ManuallyDurableOperator.execute(self, context)


class TestIsDurableCapable:
    def test_fully_implemented_and_wired_qualifies(self):
        assert is_durable_capable(FullyImplementedResumableOperator, FakeResumableJobMixin) is True

    def test_missing_retry_path_overrides_disqualifies(self):
        assert is_durable_capable(PartiallyImplementedResumableOperator, FakeResumableJobMixin) is False

    def test_implemented_but_not_called_from_execute_disqualifies(self):
        assert is_durable_capable(UnwiredResumableOperator, FakeResumableJobMixin) is False

    def test_no_mixin_in_mro_disqualifies(self):
        assert is_durable_capable(PlainOperator, FakeResumableJobMixin) is False

    def test_mixin_unavailable_disqualifies(self):
        assert is_durable_capable(FullyImplementedResumableOperator, None) is False

    def test_manual_durable_marker_qualifies_without_mixin(self):
        assert is_durable_capable(ManuallyDurableOperator, None) is True

    def test_subclass_not_redeclaring_marker_disqualifies(self):
        assert is_durable_capable(ManuallyDurableSubclass, FakeResumableJobMixin) is False

    def test_subclass_with_no_execute_override_inherits_marker(self):
        assert is_durable_capable(ManuallyDurableSubclassNoOverride, FakeResumableJobMixin) is True

    def test_subclass_delegating_via_super_execute_qualifies(self):
        assert is_durable_capable(ManuallyDurableSubclassDelegating, FakeResumableJobMixin) is True

    def test_subclass_delegating_via_super_execute_multi_hop_qualifies(self):
        assert is_durable_capable(ManuallyDurableSubclassDelegatingMultiHop, FakeResumableJobMixin) is True

    def test_multiple_inheritance_decorator_mixin_qualifies(self):
        assert is_durable_capable(DecoratedDurableSubclass, FakeResumableJobMixin) is True

    def test_multiple_inheritance_no_own_execute_qualifies(self):
        assert is_durable_capable(DecoratedDurableSubclassNoOwnExecute, FakeResumableJobMixin) is True

    def test_mixin_subclass_delegating_via_super_execute_qualifies(self):
        class MixinSubclassDelegating(FullyImplementedResumableOperator):
            def execute(self, context):
                return super().execute(context)

        assert is_durable_capable(MixinSubclassDelegating, FakeResumableJobMixin) is True

    def test_explicit_parent_class_delegation_qualifies(self):
        assert is_durable_capable(ExplicitParentDelegatingSubclass, FakeResumableJobMixin) is True

    def test_mixin_subclass_delegating_via_explicit_parent_call_qualifies(self):
        class MixinSubclassExplicitDelegating(FullyImplementedResumableOperator):
            def execute(self, context):
                return FullyImplementedResumableOperator.execute(self, context)

        assert is_durable_capable(MixinSubclassExplicitDelegating, FakeResumableJobMixin) is True

    def test_declaring_class_with_leading_underscore_is_found(self):
        """Python strips leading underscores from the class name when mangling, so the
        lookup must too, or a declaring class like `_FooOperator` is never found."""

        class _UnderscoreOperator:
            __supports_durable_execution = True

            def execute(self, context):
                return None

        assert is_durable_capable(_UnderscoreOperator, FakeResumableJobMixin) is True


# ---------------------------------------------------------------------------
# supports_deferrable
# ---------------------------------------------------------------------------
class DeferrableOperator:
    """Mirrors HttpOperator: reads self.deferrable directly in its own execute()."""

    def __init__(self, *, deferrable: bool = False):
        self.deferrable = deferrable

    def execute(self, context):
        if self.deferrable:
            return self.execute_async(context)
        return self.execute_sync(context)

    def execute_async(self, context):
        return None

    def execute_sync(self, context):
        return None


class NonDeferrableOperator:
    def __init__(self, *, some_other_param: str = ""):
        self.some_other_param = some_other_param

    def execute(self, context):
        return None


class OverridesExecuteWithoutDeferring(DeferrableOperator):
    """Mirrors DiscordWebhookOperator: inherits the deferrable param but overrides
    execute() with synchronous code that never reads it -- an inherited, dead knob."""

    def execute(self, context):
        return self.hook_call(context)

    def hook_call(self, context):
        return None


class InheritsDeferringExecute(DeferrableOperator):
    """Mirrors TimeDeltaSensorAsync: doesn't override execute() at all -- still
    defers because the inherited method is what actually runs."""


class AlwaysDeferringOperator:
    """Mirrors DateTimeSensorAsync: defers unconditionally, no toggle to find."""

    def execute(self, context):
        self.defer()

    def defer(self, *args, **kwargs):
        return None


class DefersThroughHelperMethod:
    """Mirrors TriggerDagRunOperator: execute() delegates to a same class helper
    that is the one actually calling self.defer()."""

    def execute(self, context):
        return self._trigger_dag_af_2(context)

    def _trigger_dag_af_2(self, context):
        return self.defer()

    def defer(self, *args, **kwargs):
        return None


class DefersThroughUnrelatedHelperMethod:
    """The helper the walk follows doesn't defer at all."""

    def execute(self, context):
        return self._do_the_thing(context)

    def _do_the_thing(self, context):
        return None


class BaseWithDeferringExecute:
    """Mirrors KubernetesPodOperator: its own execute() calls self.defer()."""

    def execute(self, context):
        if self.pod is None:
            self.defer()
        return None

    def defer(self, *args, **kwargs):
        return None


class SubclassDelegatingToSuperExecute(BaseWithDeferringExecute):
    """Mirrors EksPodOperator: overrides execute() to do setup, then calls
    super().execute(), which is where the actual self.defer() call lives."""

    def execute(self, context):
        self.setup(context)
        return super().execute(context)

    def setup(self, context):
        return None


class SubclassDelegatingToExplicitParentExecute(BaseWithDeferringExecute):
    """Mirrors @task.agent's _AgentDecoratedOperator: names the parent class directly
    instead of using super()."""

    def execute(self, context):
        return BaseWithDeferringExecute.execute(self, context)


class DeferringDecoratorMixin:
    """Mirrors DecoratedOperator: a mixin whose own MRO has no relationship to BaseWithDeferringExecute."""

    def execute(self, context):
        return super().execute(context)


class DecoratedDeferringSubclass(DeferringDecoratorMixin, BaseWithDeferringExecute):
    """Combines DeferringDecoratorMixin with the deferring class via multiple inheritance."""

    def execute(self, context):
        return super().execute(context)


class RaisesTaskDeferredDirectly:
    """Mirrors VespaIngestOperator: raises TaskDeferred directly, with no
    self.defer()/self.deferrable reference anywhere in the call chain."""

    def execute(self, context):
        raise TaskDeferred(trigger=None, method_name="execute_complete")


class TaskDeferred(Exception):
    """Placeholder for actual TaskDeferred exception."""

    def __init__(self, *, trigger, method_name):
        self.trigger = trigger
        self.method_name = method_name


class DefersForApprovalOnly:
    def execute(self, context):
        return self.defer_for_approval(context)

    def defer_for_approval(self, context):
        return None


class TestSupportsDeferrable:
    def test_reads_deferrable_directly_qualifies(self):
        assert supports_deferrable(DeferrableOperator) is True

    def test_no_deferral_reference_disqualifies(self):
        assert supports_deferrable(NonDeferrableOperator) is False

    def test_inherited_param_but_overridden_execute_disqualifies(self):
        """The Discord regression case: an inherited deferrable knob that the
        subclass's own execute() never reads must not count as deferrable."""
        assert supports_deferrable(OverridesExecuteWithoutDeferring) is False

    def test_inherited_unmodified_execute_qualifies(self):
        assert supports_deferrable(InheritsDeferringExecute) is True

    def test_unconditional_defer_with_no_param_qualifies(self):
        assert supports_deferrable(AlwaysDeferringOperator) is True

    def test_defers_through_helper_method_qualifies(self):
        """The TriggerDagRunOperator case: found by walking into the helper the
        exception list used to hardcode, without needing a class-name lookup."""
        assert supports_deferrable(DefersThroughHelperMethod) is True

    def test_helper_method_without_deferral_disqualifies(self):
        assert supports_deferrable(DefersThroughUnrelatedHelperMethod) is False

    def test_defers_through_super_execute_qualifies(self):
        """The EksPodOperator case: the override calls super().execute(), and the
        actual self.defer() call lives in the parent's execute()."""
        assert supports_deferrable(SubclassDelegatingToSuperExecute) is True

    def test_defers_through_super_execute_with_multiple_inheritance_qualifies(self):
        """A @task.kubernetes-shaped case: the chain must resolve against the subclass's MRO."""
        assert supports_deferrable(DecoratedDeferringSubclass) is True

    def test_defers_through_explicit_parent_class_call_qualifies(self):
        """A @task.agent-shaped case: delegation names the parent class instead of super()."""
        assert supports_deferrable(SubclassDelegatingToExplicitParentExecute) is True

    def test_raises_task_deferred_directly_qualifies(self):
        """The VespaIngestOperator case: no self.defer()/self.deferrable anywhere,
        just a direct TaskDeferred raise."""
        assert supports_deferrable(RaisesTaskDeferredDirectly) is True

    def test_defer_for_approval_does_not_false_match(self):
        """self.defer_for_approval(...) is HITL approval, not deferral; the old
        substring check on "self.defer" matched it by accident."""
        assert supports_deferrable(DefersForApprovalOnly) is False


# ---------------------------------------------------------------------------
# Module dataclass
# ---------------------------------------------------------------------------
class TestModuleDataclass:
    def test_has_all_13_fields(self):
        m = Module(
            id="amazon-s3-S3Hook",
            name="S3Hook",
            type="hook",
            import_path="airflow.providers.amazon.aws.hooks.s3.S3Hook",
            module_path="airflow.providers.amazon.aws.hooks.s3",
            short_description="Interact with Amazon S3.",
            docs_url="https://example.com/docs",
            source_url="https://github.com/apache/airflow/blob/main/providers/amazon/src/...",
            category="amazon-s3",
            provider_id="amazon",
            provider_name="Amazon",
            supports_durable_execution=False,
            supports_deferrable=False,
        )
        assert m.id == "amazon-s3-S3Hook"
        assert m.provider_name == "Amazon"


# ---------------------------------------------------------------------------
# Helpers: fake classes and modules
# ---------------------------------------------------------------------------
class FakeBaseOperator:
    """Fake base operator."""


class FakeBaseSensorOperator(FakeBaseOperator):
    """Fake base sensor."""


class FakeBaseHook:
    """Fake base hook."""


class FakeOperator(FakeBaseOperator):
    """Copy objects in S3."""

    __module__ = "airflow.providers.amazon.aws.operators.s3"


class FakeSensor(FakeBaseSensorOperator):
    """Wait for S3 key."""

    __module__ = "airflow.providers.amazon.aws.sensors.s3"


class FakeHook(FakeBaseHook):
    """Interact with S3."""

    __module__ = "airflow.providers.amazon.aws.hooks.s3"


class FakeReexportedClass(FakeBaseOperator):
    """This class is defined in another module."""

    __module__ = "airflow.providers.amazon.aws.operators._internal"


class _FakePrivateClass(FakeBaseOperator):
    """Private helper class."""

    __module__ = "airflow.providers.amazon.aws.operators.s3"


class BaseCustomOperator(FakeBaseOperator):
    """A base class that should be skipped."""

    __module__ = "airflow.providers.amazon.aws.operators.s3"


class AbstractThing(FakeBaseOperator):
    """An abstract class."""

    __module__ = "airflow.providers.amazon.aws.operators.s3"


class OperatorMixin(FakeBaseOperator):
    """A mixin."""

    __module__ = "airflow.providers.amazon.aws.operators.s3"


class FakeNotifier:
    """Chime notifier."""

    __module__ = "airflow.providers.amazon.aws.notifications.chime"


class FakeExecutor:
    """ECS executor."""

    __module__ = "airflow.providers.amazon.aws.executors.ecs"


class FakeSecretBackend:
    """Secrets Manager backend."""

    __module__ = "airflow.providers.amazon.aws.secrets.secrets_manager"


class FakePlugin:
    """S3 plugin."""

    __module__ = "airflow.providers.amazon.aws.plugins.s3"


class FakeDialect:
    """Redshift dialect."""

    __module__ = "airflow.providers.amazon.aws.dialects.redshift"


def _make_module(name: str, members: dict) -> types.ModuleType:
    """Create a fake module with given members."""
    mod = types.ModuleType(name)
    mod.__name__ = name
    for attr_name, attr_val in members.items():
        setattr(mod, attr_name, attr_val)
    return mod


FAKE_PROVIDER_YAML = {
    "package-name": "apache-airflow-providers-amazon",
    "name": "Amazon",
    "operators": [
        {
            "integration-name": "Amazon S3",
            "python-modules": ["airflow.providers.amazon.aws.operators.s3"],
        },
    ],
    "sensors": [
        {
            "integration-name": "Amazon S3",
            "python-modules": ["airflow.providers.amazon.aws.sensors.s3"],
        },
    ],
    "hooks": [
        {
            "integration-name": "Amazon S3",
            "python-modules": ["airflow.providers.amazon.aws.hooks.s3"],
        },
    ],
    "transfers": [],
    "triggers": [],
    "notifications": [
        "airflow.providers.amazon.aws.notifications.chime.FakeNotifier",
    ],
    "secrets-backends": [
        "airflow.providers.amazon.aws.secrets.secrets_manager.FakeSecretBackend",
    ],
    "executors": [
        "airflow.providers.amazon.aws.executors.ecs.FakeExecutor",
    ],
    "plugins": [
        {
            "name": "AmazonS3Plugin",
            "plugin-class": "airflow.providers.amazon.aws.plugins.s3.FakePlugin",
        },
        "not-a-dict-entry",
    ],
    "dialects": [
        {
            "dialect-type": "redshift",
            "dialect-class-name": "airflow.providers.amazon.aws.dialects.redshift.FakeDialect",
        },
        "not-a-dict-entry",
    ],
    "task-decorators": [],
}


# ---------------------------------------------------------------------------
# TestDiscoverClassesFromProvider
# ---------------------------------------------------------------------------
class TestDiscoverClassesFromProvider:
    @pytest.fixture
    def provider_yaml_path(self, tmp_path):
        import yaml

        # The provider.yaml must be under a directory structure relative to PROVIDERS_DIR
        # to make relative_to() work. We patch PROVIDERS_DIR to tmp_path.
        provider_dir = tmp_path / "amazon"
        provider_dir.mkdir()
        yaml_path = provider_dir / "provider.yaml"
        yaml_path.write_text(yaml.dump(FAKE_PROVIDER_YAML))
        return yaml_path

    @pytest.fixture
    def base_classes(self):
        return {
            "operator": FakeBaseOperator,
            "sensor": FakeBaseSensorOperator,
            "hook": FakeBaseHook,
        }

    def _mock_import(self, module_name):
        """Return fake modules for known paths."""
        modules = {
            "airflow.providers.amazon.aws.operators.s3": _make_module(
                "airflow.providers.amazon.aws.operators.s3",
                {
                    "FakeOperator": FakeOperator,
                    "FakeReexportedClass": FakeReexportedClass,
                    "_FakePrivateClass": _FakePrivateClass,
                    "BaseCustomOperator": BaseCustomOperator,
                    "AbstractThing": AbstractThing,
                    "OperatorMixin": OperatorMixin,
                },
            ),
            "airflow.providers.amazon.aws.sensors.s3": _make_module(
                "airflow.providers.amazon.aws.sensors.s3",
                {"FakeSensor": FakeSensor},
            ),
            "airflow.providers.amazon.aws.hooks.s3": _make_module(
                "airflow.providers.amazon.aws.hooks.s3",
                {"FakeHook": FakeHook},
            ),
            "airflow.providers.amazon.aws.notifications.chime": _make_module(
                "airflow.providers.amazon.aws.notifications.chime",
                {"FakeNotifier": FakeNotifier},
            ),
            "airflow.providers.amazon.aws.secrets.secrets_manager": _make_module(
                "airflow.providers.amazon.aws.secrets.secrets_manager",
                {"FakeSecretBackend": FakeSecretBackend},
            ),
            "airflow.providers.amazon.aws.executors.ecs": _make_module(
                "airflow.providers.amazon.aws.executors.ecs",
                {"FakeExecutor": FakeExecutor},
            ),
            "airflow.providers.amazon.aws.plugins.s3": _make_module(
                "airflow.providers.amazon.aws.plugins.s3",
                {"FakePlugin": FakePlugin},
            ),
            "airflow.providers.amazon.aws.dialects.redshift": _make_module(
                "airflow.providers.amazon.aws.dialects.redshift",
                {"FakeDialect": FakeDialect},
            ),
        }
        if module_name in modules:
            return modules[module_name]
        raise ImportError(f"No module named {module_name!r}")

    def test_discovers_operator(self, provider_yaml_path, base_classes):
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        operators = [r for r in result if r["type"] == "operator"]
        assert len(operators) == 1
        assert operators[0]["name"] == "FakeOperator"
        assert operators[0]["import_path"] == "airflow.providers.amazon.aws.operators.s3.FakeOperator"
        assert operators[0]["provider_id"] == "amazon"

    def test_discovers_sensor(self, provider_yaml_path, base_classes):
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        sensors = [r for r in result if r["type"] == "sensor"]
        assert len(sensors) == 1
        assert sensors[0]["name"] == "FakeSensor"

    def test_discovers_hook(self, provider_yaml_path, base_classes):
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        hooks = [r for r in result if r["type"] == "hook"]
        assert len(hooks) == 1
        assert hooks[0]["name"] == "FakeHook"

    def test_discovers_plugin(self, provider_yaml_path, base_classes):
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        plugins = [r for r in result if r["type"] == "plugin"]
        assert len(plugins) == 1
        assert plugins[0]["name"] == "FakePlugin"
        assert plugins[0]["category"] == "plugins"

    def test_discovers_dialect(self, provider_yaml_path, base_classes):
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        dialects = [r for r in result if r["type"] == "dialect"]
        assert len(dialects) == 1
        assert dialects[0]["name"] == "FakeDialect"
        assert dialects[0]["category"] == "dialects"

    def test_skips_non_dict_plugin_and_dialect_entries(self, provider_yaml_path, base_classes):
        """FAKE_PROVIDER_YAML's plugins/dialects each carry a bare-string entry
        alongside the valid dict entry; it must be skipped, not raise."""
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        assert len([r for r in result if r["type"] == "plugin"]) == 1
        assert len([r for r in result if r["type"] == "dialect"]) == 1

    def test_filters_reexported_classes(self, provider_yaml_path, base_classes):
        """Classes where cls.__module__ != the module being scanned should be excluded."""
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        names = {r["name"] for r in result}
        assert "FakeReexportedClass" not in names

    def test_filters_private_base_abstract_mixin(self, provider_yaml_path, base_classes):
        """Private, Base*, Abstract*, and Mixin classes should all be excluded."""
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        names = {r["name"] for r in result}
        assert "_FakePrivateClass" not in names
        assert "BaseCustomOperator" not in names
        assert "AbstractThing" not in names
        assert "OperatorMixin" not in names

    def test_extracts_short_description(self, provider_yaml_path, base_classes):
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        operators = [r for r in result if r["type"] == "operator"]
        assert operators[0]["short_description"] == "Copy objects in S3."

    def test_all_13_fields_present(self, provider_yaml_path, base_classes):
        """Every discovered entry has all 13 Module fields."""
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        required_fields = [
            "id",
            "name",
            "type",
            "import_path",
            "module_path",
            "short_description",
            "docs_url",
            "source_url",
            "category",
            "provider_id",
            "provider_name",
            "supports_durable_execution",
            "supports_deferrable",
        ]
        for entry in result:
            missing = [f for f in required_fields if f not in entry]
            assert not missing, f"Missing fields {missing} in {entry['name']}"

    def test_id_format(self, provider_yaml_path, base_classes):
        """ID follows the pattern {provider_id}-{module_name}-{class_name}."""
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        operators = [r for r in result if r["type"] == "operator"]
        assert operators[0]["id"] == "amazon-s3-FakeOperator"

    def test_provider_name_from_yaml(self, provider_yaml_path, base_classes):
        """provider_name comes from the YAML 'name' field."""
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        for entry in result:
            assert entry["provider_name"] == "Amazon"

    def test_category_from_integration_name(self, provider_yaml_path, base_classes):
        """Category is slugified from the integration-name in provider.yaml."""
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        operators = [r for r in result if r["type"] == "operator"]
        assert operators[0]["category"] == "amazon-s3"

    def test_docs_url_uses_inventory(self, provider_yaml_path, base_classes):
        """When inventory contains the class, docs_url uses the inventory path."""
        inventory = {
            "airflow.providers.amazon.aws.hooks.s3.FakeHook": "_api/hooks/s3.html#airflow.providers.amazon.aws.hooks.s3.FakeHook",
        }
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes, inventory=inventory)

        hooks = [r for r in result if r["type"] == "hook"]
        assert "_api/hooks/s3.html#airflow.providers.amazon.aws.hooks.s3.FakeHook" in hooks[0]["docs_url"]

    def test_docs_url_fallback_without_inventory(self, provider_yaml_path, base_classes):
        """Without inventory, docs_url falls back to manual construction."""
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes)

        hooks = [r for r in result if r["type"] == "hook"]
        assert "/_api/airflow/providers/amazon/aws/hooks/s3/index.html" in hooks[0]["docs_url"]

    def test_source_url_contains_github(self, provider_yaml_path, base_classes):
        """source_url points to GitHub."""
        with (
            patch("extract_parameters.PROVIDERS_DIR", provider_yaml_path.parent.parent),
            patch("extract_parameters.importlib.import_module", side_effect=self._mock_import),
        ):
            result = discover_classes_from_provider(provider_yaml_path, base_classes, version="1.0.0")

        operators = [r for r in result if r["type"] == "operator"]
        assert "github.com/apache/airflow/blob/" in operators[0]["source_url"]
        assert "providers-amazon/1.0.0" in operators[0]["source_url"]


# ---------------------------------------------------------------------------
# TestSensorNotClassifiedAsOperator
# ---------------------------------------------------------------------------
class TestSensorNotClassifiedAsOperator:
    """A sensor inherits from BaseOperator via BaseSensorOperator.

    Since discovery iterates provider.yaml sections, a sensor should be classified
    as "sensor" (from the sensors section) not "operator", even though it passes
    issubclass(cls, BaseOperator).
    """

    def test_sensor_in_sensor_section_is_sensor_type(self, tmp_path):
        import yaml

        # Sensor listed only in the sensors section
        provider_yaml = {
            "package-name": "apache-airflow-providers-test",
            "operators": [],
            "sensors": [
                {
                    "integration-name": "Test",
                    "python-modules": ["airflow.providers.test.sensors.my_sensor"],
                }
            ],
        }
        provider_dir = tmp_path / "test"
        provider_dir.mkdir()
        yaml_path = provider_dir / "provider.yaml"
        yaml_path.write_text(yaml.dump(provider_yaml))

        class MySensor(FakeBaseSensorOperator):
            """My custom sensor."""

            __module__ = "airflow.providers.test.sensors.my_sensor"

        mod = _make_module(
            "airflow.providers.test.sensors.my_sensor",
            {"MySensor": MySensor},
        )

        base_classes = {
            "sensor": FakeBaseSensorOperator,
            "operator": FakeBaseOperator,
        }

        with (
            patch("extract_parameters.PROVIDERS_DIR", tmp_path),
            patch("extract_parameters.importlib.import_module", return_value=mod),
        ):
            result = discover_classes_from_provider(yaml_path, base_classes)

        assert len(result) == 1
        assert result[0]["type"] == "sensor"
        assert result[0]["name"] == "MySensor"


# ---------------------------------------------------------------------------
# TestDiscoverClassesFromProvider: supports_durable_execution wiring
# ---------------------------------------------------------------------------
class TestDiscoverClassesFromProviderDurableExecution:
    def test_marks_durable_capable_and_plain_operators(self, tmp_path):
        class ResumableOperator(FullyImplementedResumableOperator):
            __module__ = "airflow.providers.test.operators.spark"

        class PlainProviderOperator(PlainOperator):
            __module__ = "airflow.providers.test.operators.spark"

        provider_yaml = {
            "package-name": "apache-airflow-providers-test",
            "name": "Test",
            "operators": [
                {
                    "integration-name": "Test",
                    "python-modules": ["airflow.providers.test.operators.spark"],
                },
            ],
        }
        provider_dir = tmp_path / "test"
        provider_dir.mkdir()
        yaml_path = provider_dir / "provider.yaml"
        yaml_path.write_text(yaml.dump(provider_yaml))

        mod = _make_module(
            "airflow.providers.test.operators.spark",
            {
                "ResumableOperator": ResumableOperator,
                "PlainProviderOperator": PlainProviderOperator,
            },
        )

        with (
            patch("extract_parameters.PROVIDERS_DIR", tmp_path),
            patch("extract_parameters.importlib.import_module", return_value=mod),
        ):
            result = discover_classes_from_provider(
                yaml_path, base_classes={}, resumable_mixin=FakeResumableJobMixin
            )

        by_name = {r["name"]: r for r in result}
        assert by_name["ResumableOperator"]["supports_durable_execution"] is True
        assert by_name["PlainProviderOperator"]["supports_durable_execution"] is False

    def test_defaults_to_false_when_mixin_unavailable(self, tmp_path):
        class ResumableOperator(FullyImplementedResumableOperator):
            __module__ = "airflow.providers.test.operators.spark"

        provider_yaml = {
            "package-name": "apache-airflow-providers-test",
            "name": "Test",
            "operators": [
                {
                    "integration-name": "Test",
                    "python-modules": ["airflow.providers.test.operators.spark"],
                },
            ],
        }
        provider_dir = tmp_path / "test"
        provider_dir.mkdir()
        yaml_path = provider_dir / "provider.yaml"
        yaml_path.write_text(yaml.dump(provider_yaml))

        mod = _make_module(
            "airflow.providers.test.operators.spark",
            {"ResumableOperator": ResumableOperator},
        )

        with (
            patch("extract_parameters.PROVIDERS_DIR", tmp_path),
            patch("extract_parameters.importlib.import_module", return_value=mod),
        ):
            result = discover_classes_from_provider(yaml_path, base_classes={})

        assert result[0]["supports_durable_execution"] is False


class TestDiscoverClassesFromProviderDeferrable:
    def test_marks_deferrable_and_plain_operators(self, tmp_path):
        class DeferrableProviderOperator(DeferrableOperator):
            __module__ = "airflow.providers.test.operators.spark"

        class PlainProviderOperator(PlainOperator):
            __module__ = "airflow.providers.test.operators.spark"

        provider_yaml = {
            "package-name": "apache-airflow-providers-test",
            "name": "Test",
            "operators": [
                {
                    "integration-name": "Test",
                    "python-modules": ["airflow.providers.test.operators.spark"],
                },
            ],
        }
        provider_dir = tmp_path / "test"
        provider_dir.mkdir()
        yaml_path = provider_dir / "provider.yaml"
        yaml_path.write_text(yaml.dump(provider_yaml))

        mod = _make_module(
            "airflow.providers.test.operators.spark",
            {
                "DeferrableProviderOperator": DeferrableProviderOperator,
                "PlainProviderOperator": PlainProviderOperator,
            },
        )

        with (
            patch("extract_parameters.PROVIDERS_DIR", tmp_path),
            patch("extract_parameters.importlib.import_module", return_value=mod),
        ):
            result = discover_classes_from_provider(yaml_path, base_classes={})

        by_name = {r["name"]: r for r in result}
        assert by_name["DeferrableProviderOperator"]["supports_deferrable"] is True
        assert by_name["PlainProviderOperator"]["supports_deferrable"] is False


# ---------------------------------------------------------------------------
# TestDiscoverClassLevelEntries
# ---------------------------------------------------------------------------
class TestDiscoverClassLevelEntries:
    """Verify notifications, secrets-backends, executors are discovered from full class paths."""

    def test_discovers_notifier(self, tmp_path):
        import yaml

        provider_yaml = {
            "package-name": "apache-airflow-providers-amazon",
            "notifications": [
                "airflow.providers.amazon.aws.notifications.chime.ChimeNotifier",
            ],
        }
        provider_dir = tmp_path / "amazon"
        provider_dir.mkdir()
        yaml_path = provider_dir / "provider.yaml"
        yaml_path.write_text(yaml.dump(provider_yaml))

        class ChimeNotifier:
            """Send Chime notification."""

            __module__ = "airflow.providers.amazon.aws.notifications.chime"

        mod = _make_module(
            "airflow.providers.amazon.aws.notifications.chime",
            {"ChimeNotifier": ChimeNotifier},
        )

        with (
            patch("extract_parameters.PROVIDERS_DIR", tmp_path),
            patch("extract_parameters.importlib.import_module", return_value=mod),
        ):
            result = discover_classes_from_provider(yaml_path, {})

        notifiers = [r for r in result if r["type"] == "notifier"]
        assert len(notifiers) == 1
        assert notifiers[0]["name"] == "ChimeNotifier"
        assert notifiers[0]["import_path"] == (
            "airflow.providers.amazon.aws.notifications.chime.ChimeNotifier"
        )
        assert notifiers[0]["category"] == "notifications"

    def test_discovers_secret_backend(self, tmp_path):
        import yaml

        provider_yaml = {
            "package-name": "apache-airflow-providers-amazon",
            "secrets-backends": [
                "airflow.providers.amazon.aws.secrets.sm.SecretsManagerBackend",
            ],
        }
        provider_dir = tmp_path / "amazon"
        provider_dir.mkdir()
        yaml_path = provider_dir / "provider.yaml"
        yaml_path.write_text(yaml.dump(provider_yaml))

        class SecretsManagerBackend:
            """Secrets Manager backend."""

        mod = _make_module(
            "airflow.providers.amazon.aws.secrets.sm",
            {"SecretsManagerBackend": SecretsManagerBackend},
        )

        with (
            patch("extract_parameters.PROVIDERS_DIR", tmp_path),
            patch("extract_parameters.importlib.import_module", return_value=mod),
        ):
            result = discover_classes_from_provider(yaml_path, {})

        secrets = [r for r in result if r["type"] == "secret"]
        assert len(secrets) == 1
        assert secrets[0]["name"] == "SecretsManagerBackend"
        assert secrets[0]["category"] == "secrets"

    def test_discovers_executor(self, tmp_path):
        import yaml

        provider_yaml = {
            "package-name": "apache-airflow-providers-amazon",
            "executors": [
                "airflow.providers.amazon.aws.executors.ecs.EcsExecutor",
            ],
        }
        provider_dir = tmp_path / "amazon"
        provider_dir.mkdir()
        yaml_path = provider_dir / "provider.yaml"
        yaml_path.write_text(yaml.dump(provider_yaml))

        class EcsExecutor:
            """ECS executor."""

        mod = _make_module(
            "airflow.providers.amazon.aws.executors.ecs",
            {"EcsExecutor": EcsExecutor},
        )

        with (
            patch("extract_parameters.PROVIDERS_DIR", tmp_path),
            patch("extract_parameters.importlib.import_module", return_value=mod),
        ):
            result = discover_classes_from_provider(yaml_path, {})

        executors = [r for r in result if r["type"] == "executor"]
        assert len(executors) == 1
        assert executors[0]["name"] == "EcsExecutor"
        assert executors[0]["category"] == "executors"

    def test_discovers_logging_handler(self, tmp_path):
        import yaml

        provider_yaml = {
            "package-name": "apache-airflow-providers-amazon",
            "logging": [
                "airflow.providers.amazon.aws.log.s3.S3TaskHandler",
            ],
        }
        provider_dir = tmp_path / "amazon"
        provider_dir.mkdir()
        yaml_path = provider_dir / "provider.yaml"
        yaml_path.write_text(yaml.dump(provider_yaml))

        class S3TaskHandler:
            """S3 task log handler."""

        mod = _make_module(
            "airflow.providers.amazon.aws.log.s3",
            {"S3TaskHandler": S3TaskHandler},
        )

        with (
            patch("extract_parameters.PROVIDERS_DIR", tmp_path),
            patch("extract_parameters.importlib.import_module", return_value=mod),
        ):
            result = discover_classes_from_provider(yaml_path, {})

        logging_entries = [r for r in result if r["type"] == "logging"]
        assert len(logging_entries) == 1
        assert logging_entries[0]["name"] == "S3TaskHandler"
        assert logging_entries[0]["category"] == "logging"


# ---------------------------------------------------------------------------
# TestCompareWithAst
# ---------------------------------------------------------------------------
class TestCompareWithAst:
    def test_detects_phantom_miss_and_mismatch(self, tmp_path):
        # AST modules.json has A, B, C
        ast_data = {
            "modules": [
                {
                    "import_path": "airflow.providers.x.A",
                    "type": "operator",
                    "name": "A",
                    "provider_id": "x",
                },
                {
                    "import_path": "airflow.providers.x.B",
                    "type": "hook",
                    "name": "B",
                    "provider_id": "x",
                },
                {
                    "import_path": "airflow.providers.x.C",
                    "type": "operator",
                    "name": "C",
                    "provider_id": "x",
                },
            ]
        }
        modules_json = tmp_path / "modules.json"
        modules_json.write_text(json.dumps(ast_data))

        # Runtime has B (type mismatch), C (matches), D (AST miss)
        runtime = [
            {
                "import_path": "airflow.providers.x.B",
                "type": "sensor",
                "name": "B",
                "provider_id": "x",
            },
            {
                "import_path": "airflow.providers.x.C",
                "type": "operator",
                "name": "C",
                "provider_id": "x",
            },
            {
                "import_path": "airflow.providers.x.D",
                "type": "trigger",
                "name": "D",
                "provider_id": "x",
            },
        ]

        stats = compare_with_ast(runtime, modules_json)

        # A is in AST but not runtime = phantom
        assert stats["ast_phantoms"] == 1
        assert "airflow.providers.x.A" in stats["phantom_paths"]

        # D is in runtime but not AST = miss
        assert stats["ast_misses"] == 1
        assert "airflow.providers.x.D" in stats["miss_paths"]

        # B has type mismatch: AST=hook, runtime=sensor
        assert stats["type_mismatches"] == 1
        assert stats["mismatch_details"][0]["ast_type"] == "hook"
        assert stats["mismatch_details"][0]["runtime_type"] == "sensor"

    def test_no_discrepancies_when_matching(self, tmp_path):
        ast_data = {
            "modules": [
                {
                    "import_path": "airflow.providers.x.A",
                    "type": "operator",
                    "name": "A",
                    "provider_id": "x",
                },
            ]
        }
        modules_json = tmp_path / "modules.json"
        modules_json.write_text(json.dumps(ast_data))

        runtime = [
            {
                "import_path": "airflow.providers.x.A",
                "type": "operator",
                "name": "A",
                "provider_id": "x",
            },
        ]

        stats = compare_with_ast(runtime, modules_json)
        assert stats["ast_phantoms"] == 0
        assert stats["ast_misses"] == 0
        assert stats["type_mismatches"] == 0

    def test_empty_ast_all_are_misses(self, tmp_path):
        ast_data = {"modules": []}
        modules_json = tmp_path / "modules.json"
        modules_json.write_text(json.dumps(ast_data))

        runtime = [
            {
                "import_path": "airflow.providers.x.A",
                "type": "operator",
                "name": "A",
                "provider_id": "x",
            },
        ]

        stats = compare_with_ast(runtime, modules_json)
        assert stats["ast_phantoms"] == 0
        assert stats["ast_misses"] == 1

    def test_empty_runtime_all_are_phantoms(self, tmp_path):
        ast_data = {
            "modules": [
                {
                    "import_path": "airflow.providers.x.A",
                    "type": "operator",
                    "name": "A",
                    "provider_id": "x",
                },
            ]
        }
        modules_json = tmp_path / "modules.json"
        modules_json.write_text(json.dumps(ast_data))

        stats = compare_with_ast([], modules_json)
        assert stats["ast_phantoms"] == 1
        assert stats["ast_misses"] == 0


# ---------------------------------------------------------------------------
# _parse_requested_providers
# ---------------------------------------------------------------------------
class TestParseRequestedProviders:
    def test_none_argument_returns_none(self):
        assert _parse_requested_providers(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_requested_providers("") is None

    def test_whitespace_only_returns_empty_set(self):
        # Empty set is falsy, so downstream `if requested_providers` skips
        # the filter just like None does.
        assert _parse_requested_providers("   ") == set()

    def test_single_provider(self):
        assert _parse_requested_providers("amazon") == {"amazon"}

    def test_multiple_providers_space_separated(self):
        assert _parse_requested_providers("amazon google snowflake") == {
            "amazon",
            "google",
            "snowflake",
        }

    def test_extra_whitespace_is_tolerated(self):
        assert _parse_requested_providers("  amazon   google  ") == {"amazon", "google"}

    def test_duplicate_providers_collapsed(self):
        assert _parse_requested_providers("amazon amazon google") == {"amazon", "google"}


class TestResolveDottedPath:
    @pytest.mark.parametrize(
        "class_path",
        (
            "no_dot_here",
            "this.module.does.not.exist.at.all.Foo",
        ),
    )
    def test__resolve_dotted_path_returns_none(self, class_path: str):
        assert _resolve_dotted_path(class_path) is None

    @pytest.mark.parametrize(
        ("class_path", "expected"),
        (
            (
                "extract_parameters.ThisAttrDoesNotExist",
                ("extract_parameters", "ThisAttrDoesNotExist", None),
            ),
            (
                "extract_parameters.get_category",
                ("extract_parameters", "get_category", get_category),
            ),
        ),
    )
    def test__resolve_dotted_path(self, class_path: str, expected: tuple[str, str, object]):
        assert _resolve_dotted_path(class_path) == expected
