SELECT
    dataset.id AS dataset_id,
    dataset.uri AS dataset_uri,
    max(dataset_event.timestamp) AS last_dataset_update,
    count(dataset_event.id) AS total_updates,
    count(dataset.id = dataset_task_ref.dataset_id) AS producing_task_count,
    count(dataset.id = dataset_dag_ref.dataset_id) AS consuming_dag_count
FROM
    dataset
    LEFT OUTER JOIN dataset_event ON dataset_event.dataset_id = dataset.id
    LEFT OUTER JOIN dataset_dag_ref ON dataset.id = dataset_dag_ref.dataset_id
    LEFT OUTER JOIN dataset_task_ref ON dataset.id = dataset_task_ref.dataset_id
GROUP BY
    dataset.id,
    dataset.uri
ORDER BY
    dataset.uri ASC;

SELECT
    dataset.id AS dataset_id,
    dataset.uri AS dataset_uri,
    count(dataset_event.id) AS total_updates,
    count(dataset_task_ref.dataset_id) AS producing_task_count
FROM
    dataset
    LEFT OUTER JOIN dataset_event ON dataset_event.dataset_id = dataset.id
    LEFT OUTER JOIN dataset_task_ref ON dataset.id = dataset_task_ref.dataset_id
GROUP BY
    dataset.id,
    dataset.uri
ORDER BY
    dataset.uri ASC
