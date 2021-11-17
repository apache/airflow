import unittest
import boto3

from airflow.providers.amazon.aws.operators.redshift_pause_cluster import RedshiftPauseClusterOperator

try:
    from moto import mock_redshift
except ImportError:
    mock_redshift = None


class TestPauseClusterOperator(unittest.TestCase):
    @staticmethod
    def _create_clusters():
        client = boto3.client('redshift', region_name='us-east-1')
        client.create_cluster(
            ClusterIdentifier='test_cluster_to_pause',
            NodeType='dc1.large',
            MasterUsername='admin',
            MasterUserPassword='mock_password',
        )
        client.create_cluster(
            ClusterIdentifier='test_cluster_to_resume',
            NodeType='dc1.large',
            MasterUsername='admin',
            MasterUserPassword='mock_password',
        )
        if not client.describe_clusters()['Clusters']:
            raise ValueError('AWS not properly mocked')

    def test_init(self):
        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
            check_interval=3,
        )
        assert redshift_operator.task_id == "task_test"
        assert redshift_operator.cluster_identifier == "test_cluster"
        assert redshift_operator.aws_conn_id == "aws_conn_test"
        assert redshift_operator.check_interval == 3
