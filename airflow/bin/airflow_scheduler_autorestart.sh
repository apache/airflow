while echo "Running"; do
    airflow scheduler -n 5
    echo "Scheduler crashed with exit code $?.  Respawning.." >&2
    date >> /tmp/airflow_scheduler_errrors.txt
    sleep 1
done
