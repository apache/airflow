from apiclient.discovery import build
from airflow.contrib.hooks.gc_base_hook import GoogleCloudBaseHook

class DatastoreHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud Datastore. Connections must be defined with an
    extras JSON field containing:

    {
        "project": "<google project ID>",
        "service_account": "<google service account email>",
        "key_path": "<p12 key path>"
    }

    If you have used ``gcloud auth`` to authenticate on the machine that's
    running Airflow, you can exclude the service_account and key_path
    parameters.

    This object is not threads safe. If you want to make multiple requests simultaniously, you will need to create
    a hook per thread.
    """

    conn_name_attr = 'datastore_conn_id'

    def __init__(self,
                 scope=['https://www.googleapis.com/auth/datastore',
                        'https://www.googleapis.com/auth/userinfo.email'],
                 datastore_conn_id='google_cloud_datastore_default',
                 delegate_to=None):
        super(DatastoreHook, self).__init__(scope, datastore_conn_id, delegate_to)
        # datasetId is the same as the project name
        self.dataset_id = self._extras_dejson().get('project')
        self.connection = self.get_conn()

    def get_conn(self):
        """
        Returns a Google Cloud Storage service object.
        """
        http_authorized = self._authorize()
        return build('datastore', 'v1beta2', http=http_authorized)

    def allocate_ids(self, partialKeys):
        """
        Allocate IDs for incomplete keys.
        see https://cloud.google.com/datastore/docs/apis/v1beta2/datasets/allocateIds

        :param partialKeys: a list of partial keys
        :return: a list of full keys.
        """
        resp = self.connection.datasets().allocateIds(datasetId=self.dataset_id, body={'keys': partialKeys}).execute()
        return resp['keys']

    def begin_transaction(self):
        """
        Get a new transaction handle
        see https://cloud.google.com/datastore/docs/apis/v1beta2/datasets/beginTransaction

        :return: a transaction handle
        """
        resp = self.connection.datasets().beginTransaction(datasetId=self.dataset_id, body={}).execute()
        return resp['transaction']

    def commit(self, body):
        """
        Commit a transaction, optionally creating, deleting or modifying some entities.
        see https://cloud.google.com/datastore/docs/apis/v1beta2/datasets/commit

        :param body: the body of the commit request
        :return: the response body of the commit request
        """
        resp = self.connection.datasets().commit(datasetId=self.dataset_id, body=body).execute()
        return resp

    def lookup(self, keys, read_consistency=None, transaction=None):
        """
        Lookup some entities by key
        see https://cloud.google.com/datastore/docs/apis/v1beta2/datasets/lookup
        :param keys: the keys to lookup
        :param read_consistency: the read consistency to use. default, strong or eventual.
                Cannot be used with a transaction.
        :param transaction: the transaction to use, if any.
        :return: the response body of the lookup request.
        """
        body = {'keys': keys}
        if read_consistency:
            body['readConsistency'] = read_consistency
        if transaction:
            body['transaction'] = transaction
        return self.connection.datasets().lookup(datasetId=self.dataset_id, body=body).execute()

    def rollback(self, transaction):
        """
        Roll back a transaction
        see https://cloud.google.com/datastore/docs/apis/v1beta2/datasets/rollback
        :param transaction: the transaction to roll back
        """
        self.connection.datasets().rollback(datasetId=self.dataset_id, body={'transaction': transaction})\
            .execute()

    def run_query(self, body):
        """
        Run a query for entities.
        see https://cloud.google.com/datastore/docs/apis/v1beta2/datasets/runQuery
        :param body: the body of the query request
        :return: the batch of query results.
        """
        resp = self.connection.datasets().runQuery(datasetId=self.dataset_id, body=body).execute()
        return resp['batch']
