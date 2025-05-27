import pytest
from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Dummy launcher so no real K8s calls are made
class DummyLauncher:
    def __init__(self):
        # pod_spec could be anything; we only care about sidecars in template_body
        self.pod_spec = {}

    def start_spark_job(self, *args, **kwargs):
        # return a dummy Pod object and empty spec
        return k8s.V1Pod(), {}

@pytest.fixture(autouse=True)
def patch_launcher_and_execute(monkeypatch):
    # Force get_or_create_spark_crd to return a dummy pod
    monkeypatch.setattr(
        SparkKubernetesOperator,
        "get_or_create_spark_crd",
        lambda self, context: k8s.V1Pod(),
    )
    # Inject our dummy launcher
    monkeypatch.setattr(
        SparkKubernetesOperator,
        "launcher",
        DummyLauncher(),
    )
    # Stub out the parent execute so it doesn't try to call real K8s
    monkeypatch.setattr(
        KubernetesPodOperator,
        "execute",
        lambda self, context: None,
    )
    yield

def test_xcom_sidecar_injection():
    # 1. Instantiate with minimal template_spec and do_xcom_push=True
    op = SparkKubernetesOperator(
        task_id="test_spark_xcom",
        template_spec={"spark": {}},
        do_xcom_push=True,
        name="test-spark",
    )

    # 2. Execute (this runs our injection block, then stubbed super.execute)
    op.execute(context={})

    # 3. Inspect the mutated template_body
    spec = op.template_body["spec"]

    # Assert the 'xcom' volume was added
    volumes = spec.get("volumes", [])
    assert any(v.name == "xcom" for v in volumes), "Missing xcom volume"

    # Assert driver mounts include the xcom mount
    driver = spec.get("driver", {})
    mounts = driver.get("volumeMounts", [])
    assert any(m.name == "xcom" and m.mount_path == "/airflow/xcom" for m in mounts), \
        "Missing xcom volumeMount in driver"

    # Assert the sidecar container is present
    sidecars = driver.get("sidecars", [])
    assert any(c.name == "airflow-xcom-sidecar" for c in sidecars), \
        "Missing airflow-xcom-sidecar container"
