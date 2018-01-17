import json
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException
from airflow.utils.db import provide_session


class PersistentContext(LoggingMixin):

    def __init__(self, initial_context, ti):
        self.log.debug(initial_context)
        if initial_context:
            self.internal_dict = json.loads(initial_context)
        else:
            self.internal_dict = dict()
        self.ti = ti

    @provide_session
    def save_kv(self, key, value, session=None):
        self.internal_dict[key] = value
        self.ti.saved_context = self.pickle()
        result = session.merge(self.ti)
        session.commit()

    def get(self, key):
        if key in self.internal_dict:
            return self.internal_dict[key]
        else:
            raise AirflowException("The persisted context does not contain the key {}".format(key))

    def pickle(self):
        return json.dumps(self.internal_dict)
