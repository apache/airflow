import telegram
from time import sleep
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


def telegram_retry(func, *args, **kwargs):
    max_retries = kwargs.pop("max_retries", 5)
    retry_sleep = kwargs.pop("retry_sleep", 0)
    # perform request with retry
    for retry in range(max_retries):
        try:
            return func(*args, **kwargs)
        except (telegram.error.TimedOut,) as e:
            if retry_sleep:
                sleep(retry_sleep)


class TelegramHook(BaseHook):
    """
       Interact with Telegram, using python-telegram-bot library.
    """

    def __init__(self, token=None, telegram_conn_id=None, chat_id=None):
        """
        Takes both telegram bot API token directly and connection that has telegram bot API token.
        If both supplied, telegram API token will be used.
        
        :param token: telegram API token
        :type token: str
        :param telegram_conn_id: connection that has telegram API token in the password field
        :type telegram_conn_id: str
        :param chat_id: Telegram public or private channel id (optional).
        Check https://stackoverflow.com/a/33862907 to see how you can obtain chat_id for private
        channel
        :type chat_id: str
        """
        self.token = self.__get_token(token, telegram_conn_id)
        self.chat_id = self.__get_chat_id(chat_id, telegram_conn_id)

    def __get_token(self, token, telegram_conn_id):
        if token is not None:
            return token
        elif telegram_conn_id is not None:
            conn = self.get_connection(telegram_conn_id)

            if not getattr(conn, "password", None):
                raise AirflowException("Missing token(password) in Telegram connection")
            return conn.password
        else:
            raise AirflowException(
                "Cannot get token: "
                "No valid Telegram token nor telegram_conn_id supplied."
            )

    def __get_chat_id(self, chat_id, telegram_conn_id):
        if chat_id is not None:
            return chat_id
        elif telegram_conn_id is not None:
            conn = self.get_connection(telegram_conn_id)

            if not getattr(conn, "host", None):
                raise AirflowException("Missing chat_id (host) in Telegram connection")
            return conn.host
        else:
            raise AirflowException(
                "Cannot get chat_id: " "No valid chat_id nor telegram_conn_id supplied."
            )

    def call(self, method, api_params):
        """
        Send a message to a telegram channel

        :param method: not used
        :type method: str
        :param method: params for telegram_instance.send_message. You can use
        it also to override chat_id
        :type method: dict
        """
        bot_instance = telegram.Bot(token=self.token)

        params = {
            "chat_id": self.chat_id,
            "parse_mode": telegram.ParseMode.HTML,
            "disable_web_page_preview": True,
        }
        params.update(api_params)

        self.log.info(telegram_retry(bot_instance.send_message, **params))
