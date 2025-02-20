from airflow.models.dag import DAG

class SdkDAG(DAG):
    """
    Custom SDK-based DAG class for improved modularity.
    Inherits from Airflow's DAG class.
    """
    def __init__(self, dag_id, **kwargs):
        super().__init__(dag_id=dag_id, **kwargs)
