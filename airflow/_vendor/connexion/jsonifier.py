import datetime
import json
import uuid


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            if o.tzinfo:
                # eg: '2015-09-25T23:14:42.588601+00:00'
                return o.isoformat('T')
            else:
                # No timezone present - assume UTC.
                # eg: '2015-09-25T23:14:42.588601Z'
                return o.isoformat('T') + 'Z'

        if isinstance(o, datetime.date):
            return o.isoformat()

        if isinstance(o, uuid.UUID):
            return str(o)

        return json.JSONEncoder.default(self, o)


class Jsonifier(object):
    """
    Used to serialized and deserialize to/from JSon
    """
    def __init__(self, json_=json, **kwargs):
        """
        :param json_: json library to use. Must have loads() and dumps() method
        :param kwargs: default arguments to pass to json.dumps()
        """
        self.json = json_
        self.dumps_args = kwargs

    def dumps(self, data, **kwargs):
        """ Central point where JSON serialization happens inside
        Connexion.
        """
        for k, v in self.dumps_args.items():
            kwargs.setdefault(k, v)
        return self.json.dumps(data, **kwargs) + '\n'

    def loads(self, data):
        """ Central point where JSON deserialization happens inside
        Connexion.
        """
        if isinstance(data, bytes):
            data = data.decode()

        try:
            return self.json.loads(data)
        except Exception:
            if isinstance(data, str):
                return data
