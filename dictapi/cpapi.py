import types
import json
from functools import wraps
from dictapi.dictapi import APITable as OrigAPITable, API as OrigAPI
import cherrypy


def json_out(func):
    @wraps(func)
    def wrapper(*a, **kw):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        code, entry = func(*a, **kw)
        cherrypy.response.status = code
        out = json.dumps(entry).encode()
        return out
    return wrapper


HTTP_METHODS = (
        'CONNECT',
        'DELETE',
        'GET',
        'HEAD',
        'OPTIONS',
        'PATCH',
        'POST',
        'PUT',
        'TRACE',
        )

class CPJsonMixin:

    def wrap(self):
        """
        Wrap all HTTP methods with json_out so that the methods return json
        parseable content
        """
        my_methods = [i for i in dir(self) if i in HTTP_METHODS]
        for attr in my_methods:
            if callable(getattr(self, attr)):
                wrapped = json_out(getattr(self, attr))
                setattr(self, attr, wrapped)



class APITable(OrigAPITable, CPJsonMixin):

    def __init__(self, api, table):
        super(OrigAPITable, self).__init__()
        self.api = api
        self.table = table
        self.wrap()



class API(OrigAPI):

    @classmethod
    def table_factory(cls): return APITable


    def generate_config(self):
        config = {}
        for table_name in self.dictdb:
            config['/'+str(table_name)] = {
                    'request.dispatch':cherrypy.dispatch.MethodDispatcher()
                    }
        return config



