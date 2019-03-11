import telegram
import time
from functools import wraps
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


def retry(exceptions, tries=4, delay=3, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.
    (с) Eliot aka saltycrane, https://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
    """

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = "{}, Retrying in {} seconds...".format(e, mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


class TelegramHook(BaseHook):
    """
       Interact with Telegram, using python-telegram-bot library.
    """

    def __init__(self, telegram_conn_id=None, chat_id=None):
        """
        Takes both telegram bot API token directly and connection that has telegram bot API token.
        If both supplied, telegram API token will be used.
        
        :param telegram_conn_id: connection that has telegram API token in the password field
        :type telegram_conn_id: str
        :param chat_id: Telegram public or private channel id (optional).
        Check https://stackoverflow.com/a/33862907 to see how you can obtain chat_id for private
        channel
        :type chat_id: str
        """
        self.token = self.__get_token(telegram_conn_id)
        self.chat_id = self.__get_chat_id(chat_id, telegram_conn_id)
        self.connection = self.get_conn()

    def get_conn(self):
        return telegram.Bot(token=self.token)

    def __get_token(self, telegram_conn_id):
        if telegram_conn_id is not None:
            conn = self.get_connection(telegram_conn_id)

            if not conn.password:
                raise AirflowException("Missing token(password) in Telegram connection")
            return conn.password
        else:
            raise AirflowException(
                "Cannot get token: " "No valid Telegram connection supplied."
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

    @retry(exceptions=telegram.error.TelegramError, tries=5, delay=0)
    def call(self, method, api_params):
        """
        Send a message to a telegram channel

        :param method: not used
        :type method: str
        :param api_params: params for telegram_instance.send_message. You can use it also to override chat_id
        :type api_params: dict
        """

        params = {
            "chat_id": self.chat_id,
            "parse_mode": telegram.ParseMode.HTML,
            "disable_web_page_preview": True,
        }
        params.update(api_params)

        self.log.info(self.connection.send_message(**params))


__all__ = [TelegramHook]
