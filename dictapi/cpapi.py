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


class APITable:

    exposed = True

    def __init__(self, api, table):
        self.api = api
        self.table = table
        self.apitable = OrigAPITable(api, table)

        for method_name in HTTP_METHODS:
            if method_name not in dir(self.apitable):
                continue
            original_method = getattr(self.apitable, method_name)
            setattr(self, method_name, json_out(original_method))



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



