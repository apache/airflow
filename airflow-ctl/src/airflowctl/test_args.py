# test_args.py
# For testing pause/unpause commands locally without needing real Airflow credentials

from types import SimpleNamespace
from typing import Literal

# --- Fake API client to simulate real behavior ---
class FakeDAGResponse:
    def __init__(self, dag_id: str, is_paused: bool):
        self.dag_id = dag_id
        self.is_paused = is_paused

    # Provide model_dump to mimic real API response
    def model_dump(self):
        return {"dag_id": self.dag_id, "is_paused": self.is_paused}

class FakeAPIClient:
    class dags:
        @staticmethod
        def update(dag_id: str, dag_body):
            # Simulate updating DAG
            return FakeDAGResponse(dag_id=dag_id, is_paused=dag_body.is_paused)

# --- Dummy FieldValidationError for testing ---
class FieldValidationError(Exception):
    pass

# --- DAGPatchBody mock ---
class DAGPatchBody:
    def __init__(self, is_paused: bool):
        self.is_paused = is_paused

# --- Functions from dag_command.py ---
def update_dag_state(dag_id: str, operation: Literal["pause", "unpause"], api_client, output: str):
    response = api_client.dags.update(dag_id=dag_id, dag_body=DAGPatchBody(is_paused=(operation == "pause")))
    response_dict = response.model_dump()
    print(f"DAG {operation} successful: {dag_id}")
    print("Further DAG details:")
    print([response_dict])
    return response_dict

def pause(args, api_client):
    if not args.dag_id:
        raise FieldValidationError(
            "Missing required parameter(s): --dag-id\n"
            "Use 'airflowctl dags pause --help' for usage information."
        )
    return update_dag_state(args.dag_id, "pause", api_client, args.output)

def unpause(args, api_client):
    if not args.dag_id:
        raise FieldValidationError(
            "Missing required parameter(s): --dag-id\n"
            "Use 'airflowctl dags unpause --help' for usage information."
        )
    return update_dag_state(args.dag_id, "unpause", api_client, args.output)

# --- Testing ---
def run_tests():
    fake_client = FakeAPIClient()

    test_cases = [
        ("pause missing dag_id", pause, None),
        ("pause with dag_id", pause, "my_test_dag"),
        ("unpause missing dag_id", unpause, None),
        ("unpause with dag_id", unpause, "my_test_dag"),
    ]

    for desc, func, dag_id in test_cases:
        print(f"\n--- Running test: {desc} ---")
        args = SimpleNamespace(dag_id=dag_id, output="json")
        try:
            result = func(args, api_client=fake_client)
            print(f"[SUCCESS] Result: {result}")
        except FieldValidationError as e:
            print(f"[EXPECTED ERROR] {e}")

if __name__ == "__main__":
    run_tests()
