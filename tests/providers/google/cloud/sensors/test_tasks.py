import unittest
from unittest import mock

from google.cloud.tasks_v2.types import Task
from airflow.providers.google.cloud.sensors.tasks import TaskQueueEmptySensor

API_RESPONSE = {}  # type: Dict[Any, Any]
PROJECT_ID = "test-project"
LOCATION = "asia-east2"
FULL_LOCATION_PATH = "projects/test-project/locations/asia-east2"
QUEUE_ID = "test-queue"
FULL_QUEUE_PATH = "projects/test-project/locations/asia-east2/queues/test-queue"
TASK_NAME = "test-task"
FULL_TASK_PATH = "projects/test-project/locations/asia-east2/queues/test-queue/tasks/test-task"


class TestCloudTasksEmptySensor(unittest.TestCase):

    @mock.patch('airflow.providers.google.cloud.sensors.tasks.CloudTasksHook')
    def test_queue_empty(self, mock_hook):

        operator = TaskQueueEmptySensor(
            task_id=TASK_NAME,
            location=LOCATION,
            project_id=PROJECT_ID,
            queue_name=QUEUE_ID,
            poke_interval=0
        )

        result = operator.poke(mock.MagicMock)

        assert result is True

    @mock.patch('airflow.providers.google.cloud.sensors.tasks.CloudTasksHook')
    def test_queue_not_empty(self, mock_hook):
        mock_hook.return_value.list_tasks.return_value = [Task(name=FULL_TASK_PATH)]

        operator = TaskQueueEmptySensor(
            task_id=TASK_NAME,
            location=LOCATION,
            project_id=PROJECT_ID,
            queue_name=QUEUE_ID,
            poke_interval=0
        )

        result = operator.poke(mock.MagicMock)

        assert result is False
