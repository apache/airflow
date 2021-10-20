import json

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException


ENDPOINT_UPLOAD_DATA = "api.notion.com/v1/pages"
ENDPOINT_CREATE_DB = "api.notion.com/v1/databases/"
ENDPOINT_RETRIEVE_DB = "api.notion.com/v1/databases/"


class NotionHook(HttpHook):
    """
    Interacts with Notion databases and pages.

    :param conn_id: ID of your airflow connection, containing
        Notion token in the 'password' field, and 'https' in the 'schema' field
    :type conn_id: str
    """

    def __init__(self, conn_id=None, *args, **kwargs):
        self.notion_token = self._get_notion_token(conn_id)
        self.conn_id = conn_id
        super(NotionHook, self).__init__(
            http_conn_id=self.conn_id, method="POST", *args, **kwargs
        )

    def _get_notion_token(self, conn_id=None):
        """
        Retrieves Notion token either via param conn_id.

        :param conn_id: ID of your airflow connection, containing
            Notion token in the 'password' field, and 'https' in the 'schema' field
        :type conn_id: str
        """
        if conn_id:
            connection = BaseHook.get_connection(conn_id)
            password = connection.password
            return password
        else:
            raise AirflowException(
                "Cannot get token: No valid Notion token or conn_id supplied"
            )

    def _upload_data(self, data=None):
        """
        Uploads data to existing notion table (full page).

        :param data: data to be uploaded
        :type data: dict
        """
        self.data = data
        self.headers = {
            "Authorization": f"Bearer {self.notion_token}",
            "Content-Type": "application/json",
            "Notion-Version": "2021-08-16",
        }
        request = super(NotionHook, self).run(
            endpoint=ENDPOINT_UPLOAD_DATA,
            data=json.dumps(self.data),
            headers=self.headers,
        )
        return request

    def _create_db(self, data=None):
        """
        Create a database. It needs an already existing blank page and its ID in the param `data`

        :param data: properties of the database to create
        :type data: dict
        """
        self.data = data
        self.headers = {
            "Authorization": f"Bearer {self.notion_token}",
            "Content-Type": "application/json",
            "Notion-Version": "2021-08-16",
        }
        request = super(NotionHook, self).run(
            endpoint=ENDPOINT_CREATE_DB,
            data=json.dumps(self.data),
            headers=self.headers,
        )
        return request

    def _retrieve_db(self, db_id=None):
        """
        Retrieve a database (properties)

        :param db_id: id of the database
        :type db_id: str
        """
        self.method = "GET"
        self.headers = {
            "Authorization": f"Bearer {self.notion_token}",
            "Notion-Version": "2021-08-16",
        }
        url = ENDPOINT_RETRIEVE_DB + str(db_id)
        request = super(NotionHook, self).run(endpoint=url, headers=self.headers)
        return request
