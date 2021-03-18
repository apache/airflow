import os

from azure.mgmt.datafactory.models import AzureStorageLinkedService, Factory, SecureString

from airflow.decorators import dag, task
from airflow.providers.microsoft.azure.hooks.azure_data_factory import AzureDataFactoryHook
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2))
def data_factory_pipeline():
    """
    Azure Data Factory hook example DAG.

    To work, the hook needs at least the following connection details:

        {
            "conn_type": "azure_data_factory",
            "login": "[service principal id]",
            "password": "[service principal secret]",
            "extra": {
                "subscriptionId": "[subscription id]",
                "tenantId": "[tenant id]"
            }
        }

    If your connection always targets the same factory, you can optionnaly add the following extras:

        {
            "resourceGroup": "[resource group name]",
            "factory": "[factory name]"
        }
    """

    hook = AzureDataFactoryHook()

    @task
    def create_factory():
        factory = Factory(location="westeurope")

        hook.create_factory(factory)

    @task
    def create_linked_service():
        storage_conn_str = os.environ["STORAGE_CONN_STR"]
        linked_service = AzureStorageLinkedService(connection_string=SecureString(value=storage_conn_str))

        hook.create_linked_service("AzureStorage", linked_service)

    create_factory() >> create_linked_service()


adf_dag = data_factory_pipeline()
