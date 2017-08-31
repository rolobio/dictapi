import types
import json
from functools import wraps
from dictapi.dictapi import APITable as AT
import cherrypy


def json_out(func):
    @wraps(func)
    def wrapper(*a, **kw):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        out = json.dumps(func(*a, **kw)).encode()
        return out
    return wrapper


RESTFUL_METHODS = ('GET', 'PUT', 'POST', 'DELETE', 'PATCH')

class CPJsonMixin:

    def wrap(self):
        http_methods = [i for i in dir(self) if i in RESTFUL_METHODS]
        for attr in http_methods:
            if callable(getattr(self, attr)):
                wrapped = json_out(getattr(self, attr))
                setattr(self, attr, wrapped)



class APITable(AT, CPJsonMixin):

    def __init__(self, api, table):
        super(AT, self).__init__()
        self.api = api
        self.table = table
        self.wrap()

