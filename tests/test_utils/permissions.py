from airflow.security import permissions


def _resource_name(dag_id: str, resource_name: str) -> str:
    """
    This method is to keep compatibility with new FAB versions
    running with old airflow versions.
    """
    if hasattr(permissions, "resource_name"):
        return getattr(permissions, "resource_name")(dag_id, resource_name)
    if resource_name == permissions.RESOURCE_DAG:
        return getattr(permissions, "resource_name_for_dag")(dag_id)
    raise Exception('Only DAG resource is supported in this Airflow version.')
