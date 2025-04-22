import jmespath
from chart_utils.helm_template_generator import render_chart


def _test_git_sync_presence(
    git_sync_values: dict,
    dags_persistence_enabled: bool,
    template_path: str,
    extra_values: dict = None,
    in_init_containers: bool = True,
    in_containers: bool = False
):
    """Helper to test git-sync container presence in specific locations.

    Args:
        git_sync_values: The chart values for `dags.gitSync`.
        template_path: Path to the template to test.
        in_init_containers: Whether git-sync should be present in init containers.
        in_containers: Whether git-sync should be present in regular containers.
    """

    values = {
        "dags": {
            "gitSync": git_sync_values,
            "persistence": {"enabled": dags_persistence_enabled},
        }
    }

    if extra_values:
        values.update(extra_values)

    docs = render_chart(
        values=values,
        show_only=[template_path],
    )

    # Check containers
    containers = jmespath.search("spec.template.spec.containers", docs[0])
    init_containers = jmespath.search("spec.template.spec.initContainers", docs[0]) or []
    container_names = [c["name"] for c in containers]
    init_container_names = [c["name"] for c in init_containers]

    if in_containers:
        assert "git-sync" in container_names
    else:
        assert "git-sync" not in container_names

    if in_init_containers:
        assert "git-sync-init" in init_container_names
    else:
        assert "git-sync-init" not in init_container_names


def _get_git_sync_test_params_for_no_containers(
    component_name, include_enabled_git_sync_and_persistence_case: bool = True
):
    """Generate test parameters for git sync tests.

    Args:
        component_name: Name of the component to test (e.g. "migrateDatabaseJob", "createUserJob")
        include_enabled_git_sync_and_persistence_case: Whether to include the case where git sync and
            persistence are both enabled.
    """
    params = [
        (
            {"enabled": False},
            False,
        ),
        (
            {"enabled": False},
            True,
        ),
        (
            {"enabled": True, "components": {component_name: False}},
            False,
        )
    ]
    if include_enabled_git_sync_and_persistence_case:
        params.append(
            (
                {"enabled": True, "components": {component_name: True}},
                True,
            )
        )
    return params


def _get_enabled_git_sync_test_params(component_name):
    """Generate test parameters for git sync tests.

    Args:
        component_name: Name of the component to test (e.g. "migrateDatabaseJob", "createUserJob")
    """
    return [
        (
            {"enabled": True, "components": {component_name: True}},
            False,
        ),
    ]
