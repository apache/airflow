import attrs
from airflow.models import TaskInstance
from openlineage.client.facet import BaseFacet


@attrs.define(slots=False)
class MyCustomRunFacet(BaseFacet):
    """Define a custom run facet."""

    name: str
    jobState: str
    uniqueName: str
    displayName: str
    dagId: str
    taskId: str
    cluster: str


def get_additional_test_facet(task_instance: TaskInstance):
    job_unique_name = f"TEST.{task_instance.dag_id}.{task_instance.task_id}"
    return {
        "additional_run_facet": attrs.asdict(
            MyCustomRunFacet(
                name="test-lineage-namespace",
                jobState=task_instance.state,
                uniqueName=job_unique_name,
                displayName=f"{task_instance.dag_id}.{task_instance.task_id}",
                dagId=task_instance.dag_id,
                taskId=task_instance.task_id,
                cluster="TEST",
            )
        )
    }


def get_duplicate_test_facet_key(task_instance: TaskInstance):
    job_unique_name = f"TEST.{task_instance.dag_id}.{task_instance.task_id}"
    return {
        "additional_run_facet": attrs.asdict(
            MyCustomRunFacet(
                name="test-lineage-namespace",
                jobState=task_instance.state,
                uniqueName=job_unique_name,
                displayName=f"{task_instance.dag_id}.{task_instance.task_id}",
                dagId=task_instance.dag_id,
                taskId=task_instance.task_id,
                cluster="TEST",
            )
        )
    }


def get_another_test_facet(task_instance: TaskInstance):
    return {"another_run_facet": {"name": "another-lineage-namespace"}}


def return_type_is_not_dict(task_instance: TaskInstance):
    return "return type is not dict"
