from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


# File detector by prefix sensor for azure data lake storage

class DataLakePrefixSensor(BaseSensorOperator):

    """
    Interacts with Azure Data Lake:

    Client ID and client secret should be in user and password parameters.
    Tenant and account name should be extra field as
    {"tenant": "<TENANT>", "account_name": "ACCOUNT_NAME"}.

    :param azure_data_lake_conn_id: Reference to the Azure Data Lake connection

    DataLake_path : directory of the file

    prefix : file name

    """

    @apply_defaults
    def __init__(
        self,
        DataLake_path,
        prefix,
        azure_data_lake_conn_id="azure_data_lake_default",
        check_options=None,
        *args,
        **kwargs
    ):
        super(DataLakePrefixSensor, self).__init__(*args, **kwargs)
        if check_options is None:
            check_options = {}
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.DataLake_path = DataLake_path
        self.prefix = prefix
        self.check_options = check_options

    def poke(self, context):
        self.log.info(
            "Poking for prefix: %s in Data Lake path: %s",
            self.prefix,
            self.DataLake_path,
        )
        hook = AzureDataLakeHook(azure_data_lake_conn_id=self.azure_data_lake_conn_id)
        return len(hook.list(self.DataLake_path + "/" + self.prefix)) > 0

        # return TRUE => 1 ou more file detected
        # return FALSE => No file detected
