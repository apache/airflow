from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import HTTPException, status

if TYPE_CHECKING:
    from airflow.models.dag import DAG


def get_dag_from_dag_bag(dag_bag, dag_id: str) -> DAG:
    """
    Retrieve a DAG from dag_bag with consistent error handling.

    Raises:
        HTTPException: with appropriate status code and message on error.
    """
    try:
        return dag_bag.get_dag(dag_id)
    except RuntimeError as err:
        if "airflow dags reserialize" in str(err):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"An unexpected error occurred while trying to load DAG '{dag_id}'.",
            )
        raise
