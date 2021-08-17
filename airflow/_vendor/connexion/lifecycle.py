
class ConnexionRequest(object):
    def __init__(self,
                 url,
                 method,
                 path_params=None,
                 query=None,
                 headers=None,
                 form=None,
                 body=None,
                 json_getter=None,
                 files=None,
                 context=None):
        self.url = url
        self.method = method
        self.path_params = path_params or {}
        self.query = query or {}
        self.headers = headers or {}
        self.form = form or {}
        self.body = body
        self.json_getter = json_getter
        self.files = files
        self.context = context if context is not None else {}

    @property
    def json(self):
        return self.json_getter()


class ConnexionResponse(object):
    def __init__(self,
                 status_code=200,
                 mimetype=None,
                 content_type=None,
                 body=None,
                 headers=None):
        self.status_code = status_code
        self.mimetype = mimetype
        self.content_type = content_type
        self.body = body
        self.headers = headers or {}
