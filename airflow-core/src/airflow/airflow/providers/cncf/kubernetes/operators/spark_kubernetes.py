# airflow/providers/cncf/kubernetes/operators/spark_kubernetes.py

from kubernetes.client import V1EnvVar
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import BaseOperator
from typing import Optional

class SparkKubernetesOperator(BaseOperator):
    """
    SparkKubernetesOperator launches Spark driver and executor pods in Kubernetes.
    This version adds a SPARK_APPLICATION_NAME environment variable to both pods.
    """

    def __init__(
        self,
        *,
        application_name: str,
        namespace: str = "default",
        # other arguments
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.application_name = application_name
        self.namespace = namespace
        # Initialize other needed fields

    def execute(self, context):
        """
        Build and submit the driver and executor pods.
        """
        dag_run = context.get("dag_run")
        self.dag_run = dag_run  # store DAG run to use in _get_spark_app_name

        # --- Example: create driver pod spec ---
        driver_spec = self._build_driver_pod_spec()

        # Add SPARK_APPLICATION_NAME env variable to driver pod
        driver_env = driver_spec['spec']['containers'][0].env or []
        driver_env.append(
            V1EnvVar(
                name="SPARK_APPLICATION_NAME",
                value=self._get_spark_app_name()
            )
        )
        driver_spec['spec']['containers'][0].env = driver_env

        # --- Example: create executor pod spec ---
        executor_spec = self._build_executor_pod_spec()

        # Add SPARK_APPLICATION_NAME env variable to executor pod
        executor_env = executor_spec['spec']['containers'][0].env or []
        executor_env.append(
            V1EnvVar(
                name="SPARK_APPLICATION_NAME",
                value=self._get_spark_app_name()
            )
        )
        executor_spec['spec']['containers'][0].env = executor_env

        # Submit driver and executor pods
        self._submit_driver(driver_spec)
        self._submit_executors(executor_spec)

        # Other existing logic...

    def _get_spark_app_name(self) -> str:
        """
        Returns the Spark application name for this DAG run.
        Combines the base application name with DAG run ID for deterministic uniqueness.
        """
        suffix = getattr(self, "dag_run", None).run_id if hasattr(self, "dag_run") else "manual"
        return f"{self.application_name}-{suffix}"

    # Placeholder methods for building and submitting pods
    def _build_driver_pod_spec(self):
        # Existing logic to create driver pod spec
        return {
            "spec": {
                "containers": [
                    {
                        "name": "spark-driver",
                        "env": []
                    }
                ]
            }
        }

    def _build_executor_pod_spec(self):
        # Existing logic to create executor pod spec
        return {
            "spec": {
                "containers": [
                    {
                        "name": "spark-executor",
                        "env": []
                    }
                ]
            }
        }

    def _submit_driver(self, driver_spec):
        # Existing submission logic
        pass

    def _submit_executors(self, executor_spec):
        # Existing submission logic
        pass
