# Dockerfile for DAG Triage Assistant development environment.
# Extends the official Airflow image with the triage plugin pre-installed.
FROM apache/airflow:3.0.1-python3.10

USER airflow

# Install the dag-triage plugin package (editable for live code changes).
COPY --chown=airflow:root src/ /opt/airflow/dag-triage-src/src/
COPY --chown=airflow:root pyproject.toml README.md /opt/airflow/dag-triage-src/
COPY --chown=airflow:root data/ /opt/airflow/dag-triage-src/data/
RUN pip install --no-cache-dir -e /opt/airflow/dag-triage-src/

# Copy the plugin entry point so Airflow discovers it automatically.
COPY --chown=airflow:root plugins/dag_triage_plugin.py /home/airflow/.local/lib/python3.10/site-packages/dag_triage_plugin.py

# Copy sample DAGs for manual triage testing.
COPY --chown=airflow:root dags/ /opt/airflow/dags/
